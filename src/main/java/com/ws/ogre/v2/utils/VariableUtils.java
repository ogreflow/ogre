package com.ws.ogre.v2.utils;

import com.ws.common.logging.Logger;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VariableUtils {
    private static Logger ourLogger = Logger.getLogger();

    private static String REGEX_DOLLAR = Pattern.quote("$");
    private static String REGEX_OPEN_BRACE_2ND = Pattern.quote("{");
    private static String REGEX_CLOSE_BRACE_2ND = Pattern.quote("}");
    private static String REGEX_PLUS = Pattern.quote("+");
    private static String REGEX_MINUS = Pattern.quote("-");

    public static String replaceVariables(String theStr, Map<String, String> theVars) {

        Pattern aPattern = Pattern.compile(REGEX_DOLLAR + REGEX_OPEN_BRACE_2ND + "(.*?)" + REGEX_CLOSE_BRACE_2ND);
        Matcher aMatcher = aPattern.matcher(theStr);

        while (aMatcher.find()) {
            for (int i = 0; i < aMatcher.groupCount(); i++) {
                String aName = aMatcher.group(1);

                theStr = replaceForDateOperation(theStr, aName, theVars);
                theStr = replacePlain(theStr, aName, theVars);
            }
        }

        return theStr;
    }

    private static String replaceForDateOperation(String theStr, String theName, Map<String, String> theVars) {
        // Finding expression like this: ${fromDate - 1 DAY} or, ${from - 4 HOUR}
        Matcher aMatchForDateOperation = Pattern.compile("^([a-zA-Z0-9]+)\\s*([" + REGEX_PLUS + REGEX_MINUS + "])\\s*(\\d+)\\s*(DAY|HOUR)$").matcher(theName);
        if (!aMatchForDateOperation.matches()) {
            return theStr;
        }

        String aNameFrom = aMatchForDateOperation.group(1);
        String anOp = aMatchForDateOperation.group(2);
        String anInterval = aMatchForDateOperation.group(3);
        String aDateUnit = aMatchForDateOperation.group(4);

        // Validation
        if (
                StringUtils.isEmpty(aNameFrom) ||
                        !Arrays.asList("+", "-").contains(anOp) ||
                        !StringUtils.isNumeric(anInterval) ||
                        !Arrays.asList("DAY", "HOUR").contains(aDateUnit)
                ) {
            ourLogger.warn("Illegal replacement format given: %s", theName);
            return theStr;
        }

        String aValue = theVars.get(aNameFrom);
        if (aValue == null) {
            return theStr;
        }

        // This value must be datetime (yyyy-MM-dd / yyyy-MM-dd HH:mm:ss).
        try {
            for (String aFormat : new String[]{"yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss"}) {
                if (aValue.length() == aFormat.length()) {
                    SimpleDateFormat aDateFormat = new SimpleDateFormat(aFormat);

                    return replace(theStr, theName, aDateFormat.format(
                            getCalculatedDate(aDateFormat.parse(aValue).getTime(), anOp, anInterval, aDateUnit)
                    ));
                }
            }

        } catch (Exception e) {
            ourLogger.warn("Illegal date/datetime format found: %s", aValue);
            return theStr;
        }

        return theStr;
    }

    private static Date getCalculatedDate(long theRefTime, String theOp, String theInterval, String theDateUnit) {
        Calendar aCal = Calendar.getInstance();
        aCal.setTimeInMillis(theRefTime);

        if ("DAY".equals(theDateUnit)) {
            aCal.add(Calendar.DATE, ("-".equals(theOp) ? -1 : 1) * Integer.parseInt(theInterval));

        } else if ("HOUR".equals(theDateUnit)) {
            aCal.add(Calendar.HOUR_OF_DAY, ("-".equals(theOp) ? -1 : 1) * Integer.parseInt(theInterval));
        }

        return aCal.getTime();
    }

    private static String replacePlain(String theStr, String theName, Map<String, String> theVars) {
        String aValue = theVars.get(theName);
        if (aValue == null) {
            return theStr;
        }

        return replace(theStr, theName, aValue);
    }

    private static String replace(String theStr, String theName, String aValue) {
        return theStr.replace("${" + theName + "}", aValue);
    }
}
