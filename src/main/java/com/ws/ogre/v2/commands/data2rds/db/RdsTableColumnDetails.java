package com.ws.ogre.v2.commands.data2rds.db;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class RdsTableColumnDetails {
    private static final List<Integer> DATE_TYPES = Arrays.asList(Types.DATE, Types.TIME, Types.TIMESTAMP);
    private static final List<Integer> INTEGRAL_NUMBER_TYPES = Arrays.asList(Types.INTEGER, Types.BIGINT);
    private static final List<Integer> DECIMAL_NUMBER_TYPES = Arrays.asList(Types.FLOAT, Types.DOUBLE, Types.DECIMAL, Types.NUMERIC);

    private String myName;
    private int myType;
    private boolean myIsNullable = false;

    public RdsTableColumnDetails(String theName, int theType, boolean theIsNullable) {
        myName = theName;
        myType = theType;
        myIsNullable = theIsNullable;
    }

    public String getName() {
        return myName;
    }

    public int getType() {
        return myType;
    }

    public boolean isDateType() {
        return DATE_TYPES.contains(myType);
    }

    public boolean isIntegralType() {
        return INTEGRAL_NUMBER_TYPES.contains(myType);
    }

    public boolean isDecimalType() {
        return DECIMAL_NUMBER_TYPES.contains(myType);
    }

    public boolean isNullable() {
        return myIsNullable;
    }

    @Override
    public String toString() {
        return "name=" + myName + ", type=" + myType + ", isNull=" + myIsNullable;
    }
}
