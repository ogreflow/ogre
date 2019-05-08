package com.ws.ogre.v2.commands.dataverifier;

import org.apache.commons.lang3.StringUtils;

import java.sql.Types;
import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MismatchTolerance {
    public enum MismatchToleranceType {
        NUMBER,
        PERCENTAGE,
        PERCENTAGE_IN_SAMPLE
    }

    public static MismatchTolerance ZERO_TOLERANCE = getToleranceFromSpec("0");

    private MismatchToleranceType myToleranceType;
    private ToleranceValue myToleranceValue;

    public MismatchTolerance(MismatchToleranceType theToleranceType, ToleranceValue theToleranceValue) {
        myToleranceType = theToleranceType;
        myToleranceValue = theToleranceValue;
    }

    public static MismatchTolerance getToleranceFromSpec(String theSpec) {
        PercentageInSampleValue aPctInSampleValue = PercentageInSampleValue.parseIfSpecMeansIt(theSpec);
        if (aPctInSampleValue != null) {
            return new MismatchTolerance(MismatchToleranceType.PERCENTAGE_IN_SAMPLE, aPctInSampleValue);
        }

        PercentageValue aPctValue = PercentageValue.parseIfSpecMeansIt(theSpec);
        if (aPctValue != null) {
            return new MismatchTolerance(MismatchToleranceType.PERCENTAGE, aPctValue);
        }

        NumberValue aNumberValue = NumberValue.parseIfSpecMeansIt(theSpec);
        if (aNumberValue != null) {
            return new MismatchTolerance(MismatchToleranceType.NUMBER, aNumberValue);
        }

        throw new IllegalArgumentException("Unknown tolerance specification: " + theSpec);
    }

    public String getAsString() {
        return myToleranceValue.toString();
    }

    public boolean isEqualData(int theColumnType, String theRefValue, String theTestValue) {
        if (theRefValue == null || theTestValue == null) {
            return theRefValue == theTestValue;
        }

        if (!isNumberType(theColumnType)) {
            return StringUtils.equals(theRefValue, theTestValue);
        }

        if (myToleranceType == MismatchToleranceType.PERCENTAGE_IN_SAMPLE) {
            return !hasMismatchForPercentageInSample(theRefValue, theTestValue);
        }

        if (myToleranceType == MismatchToleranceType.PERCENTAGE) {
            return !hasMismatchForPercentage(theRefValue, theTestValue);
        }

        if (myToleranceType == MismatchToleranceType.NUMBER) {
            return !hasMismatchForDouble(theColumnType, theRefValue, theTestValue);
        }

        return false;
    }

    public boolean isForWholeSample() {
        return myToleranceType == MismatchToleranceType.PERCENTAGE_IN_SAMPLE;
    }

    public boolean isEqualDataInWholeSample(int theTotalCount, int theMismatchCount) {
        // For those tolerance type where we want to determine the error will exists in X% of data.
        if (isForWholeSample()) {
            PercentageInSampleValue aTolerance = (PercentageInSampleValue) myToleranceValue;
            return ((double)(theTotalCount - theMismatchCount) / theTotalCount * 100.00) >= aTolerance.mySamplePct;
        }

        // For all other types, its okay.
        return true;
    }

    private boolean hasMismatchForDouble(int theColumnType, String theRefValue, String theTestValue) {
        NumberValue aTolerance = (NumberValue) myToleranceValue;

        if (isIntegralType(theColumnType)) {
            return Math.abs(Long.parseLong(theRefValue) - Long.parseLong(theTestValue)) > aTolerance.getAsLong();
        }

        if (isFloatingPointType(theColumnType)) {
            return Math.abs(Double.parseDouble(theRefValue) - Double.parseDouble(theTestValue)) > aTolerance.getAsDouble();
        }

        return false;
    }

    private boolean hasMismatchForPercentageInSample(String theRefValue, String theTestValue) {
        PercentageInSampleValue aTolerance = (PercentageInSampleValue) myToleranceValue;
        double aDiffPercentage = (Double.parseDouble(theRefValue) - Double.parseDouble(theTestValue)) / Double.parseDouble(theRefValue) * 100.00;

        return Math.abs(aDiffPercentage) > aTolerance.myPct;
    }

    private boolean hasMismatchForPercentage(String theRefValue, String theTestValue) {
        PercentageValue aTolerance = (PercentageValue) myToleranceValue;
        double aDiffPercentage = (Double.parseDouble(theRefValue) - Double.parseDouble(theTestValue)) / Double.parseDouble(theRefValue) * 100.00;

        return Math.abs(aDiffPercentage) > aTolerance.myPct;
    }

    private boolean isNumberType(int theColumnType) {
        return isIntegralType(theColumnType) || isFloatingPointType(theColumnType);
    }

    private boolean isIntegralType(int theColumnType) {
        return theColumnType == Types.BIGINT || theColumnType == Types.INTEGER || theColumnType == Types.SMALLINT || theColumnType == Types.TINYINT || theColumnType == Types.BIT;
    }

    private boolean isFloatingPointType(int theColumnType) {
        return theColumnType == Types.NUMERIC || theColumnType == Types.DECIMAL || theColumnType == Types.DOUBLE || theColumnType == Types.REAL || theColumnType == Types.FLOAT;
    }

    private static abstract class ToleranceValue {
    }

    private static class NumberValue extends ToleranceValue {
        private Double myValue;

        public static NumberValue parseIfSpecMeansIt(String theSpec) {
            Matcher aMatcher = Pattern.compile("^\\d+(?:\\.\\d+)?$").matcher(theSpec);
            if (!aMatcher.matches()) {
                return null;
            }

            NumberValue aValue = new NumberValue();
            aValue.myValue = Double.parseDouble(aMatcher.group());
            return aValue;
        }

        public long getAsLong() {
            return myValue.longValue();
        }

        public double getAsDouble() {
            return myValue.doubleValue();
        }

        @Override
        public String toString() {
            return new DecimalFormat("0.00").format(myValue);
        }
    }

    private static class PercentageValue extends ToleranceValue {
        private int myPct;

        public static PercentageValue parseIfSpecMeansIt(String theSpec) {
            Matcher aMatcher = Pattern.compile("^(\\d+)%$").matcher(theSpec);
            if (!aMatcher.matches()) {
                return null;
            }

            PercentageValue aValue = new PercentageValue();
            aValue.myPct = Integer.parseInt(aMatcher.group(1));
            return aValue;
        }

        @Override
        public String toString() {
            return new DecimalFormat("0.00").format(myPct) + "%";
        }
    }

    private static class PercentageInSampleValue extends ToleranceValue {
        private int myPct;
        private int mySamplePct;

        public static PercentageInSampleValue parseIfSpecMeansIt(String theSpec) {
            Matcher aMatcher = Pattern.compile("^(\\d+)%in(\\d+)%$").matcher(theSpec);
            if (!aMatcher.matches()) {
                return null;
            }

            PercentageInSampleValue aValue = new PercentageInSampleValue();
            aValue.myPct = Integer.parseInt(aMatcher.group(1));
            aValue.mySamplePct = Integer.parseInt(aMatcher.group(2));
            return aValue;
        }

        @Override
        public String toString() {
            return new DecimalFormat("0.00").format(myPct) + "% => " + new DecimalFormat("0.00").format(mySamplePct) + "%";
        }
    }

    @Override
    public String toString() {
        return "toleranceType=" + myToleranceType + ", toleranceValue=" + myToleranceValue;
    }
}
