package com.ws.ogre.v2.datetime;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Helper class for working with dates.
 */
public class DateUtil {

    public static Date getDateHourPart(Date theDate) {
        try {
            SimpleDateFormat aFormat = new SimpleDateFormat("yyyyMMddHH");
            aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            return aFormat.parse(aFormat.format(theDate));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Date getDatePart(Date theDate) {
        try {
            SimpleDateFormat aFormat = new SimpleDateFormat("yyyyMMdd");
            aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

            return aFormat.parse(aFormat.format(theDate));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String format(long theDate, String theSimpleDateFormat) {
        return format(new Date(theDate), theSimpleDateFormat);
    }

    public static String format(Date theDate, String theSimpleDateFormat) {
        SimpleDateFormat aDateFormat = new SimpleDateFormat(theSimpleDateFormat);
        aDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        return aDateFormat.format(theDate);
    }

    public static Date parse(String theDate, String theSimpleDateFormat) {
        return parse(theDate, theSimpleDateFormat, TimeZone.getTimeZone("UTC"));
    }

    public static Date parse(String theDate, String theSimpleDateFormat, TimeZone theTimeZone) {
        try {
            SimpleDateFormat aFormat = new SimpleDateFormat(theSimpleDateFormat);
            aFormat.setTimeZone(theTimeZone);

            return aFormat.parse(theDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Date getNDaysAgo(int theDaysAgo) {
        Calendar aCal = Calendar.getInstance();
        aCal.add(Calendar.DATE, -theDaysAgo);
        aCal.set(Calendar.HOUR_OF_DAY, 0);
        aCal.set(Calendar.MINUTE, 0);
        aCal.set(Calendar.SECOND, 0);
        aCal.set(Calendar.MILLISECOND, 0);
        return aCal.getTime();
    }

    public static Date getNHoursAgo(int theLookbackHours) {
        Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aCal.setTimeInMillis(System.currentTimeMillis());
        aCal.add(Calendar.HOUR_OF_DAY, -theLookbackHours);

        return aCal.getTime();
    }
}
