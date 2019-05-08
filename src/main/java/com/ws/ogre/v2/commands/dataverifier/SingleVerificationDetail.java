package com.ws.ogre.v2.commands.dataverifier;

import java.util.List;

public class SingleVerificationDetail {
    private String myName;
    private String myRefSql;
    private String myTestSql;
    private List<MismatchTolerance> myTolerances;

    public SingleVerificationDetail(String theName, String theRefSql, String theTestSql, List<MismatchTolerance> theTolerances) {
        myName = theName;
        myRefSql = theRefSql;
        myTestSql = theTestSql;
        myTolerances = theTolerances;
    }

    public String getName() {
        return myName;
    }

    public String getRefSql() {
        return myRefSql;
    }

    public String getTestSql() {
        return myTestSql;
    }

    public List<MismatchTolerance> getTolerances() {
        return myTolerances;
    }

    @Override
    public String toString() {
        return "\nname=" + myName +
                "\nrefSql=" + myRefSql +
                "\ntestSql=" + myTestSql +
                "\ntolerances=" + myTolerances;
    }
}
