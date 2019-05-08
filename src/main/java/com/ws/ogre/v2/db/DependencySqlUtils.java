package com.ws.ogre.v2.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class DependencySqlUtils {
    public static void checkDependency(JdbcDbHandler theDbHandler, String theSql) throws DependencySqlFailedException {
        try {

            // Execute dependency sql. A dependency sql should return one row where the first column is true if
            // dependency is in place, else it should return false.
            theDbHandler.query(theSql, (ResultSet theResultSet) -> {
                if (!theResultSet.next()) {
                    throw new DependencySqlFailedException("No rows was returned for dependency sql");
                }

                ResultSetMetaData aMeta = theResultSet.getMetaData();
                int aDependencyCheckColumnIndex = 1; // First column should mean true / false for dependency.

                if (!isBooleanType(aMeta.getColumnType(aDependencyCheckColumnIndex))) {
                    throw new DependencySqlFailedException("A dependency sql must return a boolean in first column signaling if dependency in place (true) or not (false). Column type found: " + aMeta.getColumnType(aDependencyCheckColumnIndex));
                }

                // Is dependency in place?
                if (theResultSet.getBoolean(aDependencyCheckColumnIndex)) {
                    return;
                }

                // Dependency is missing...

                String aDescription = "";

                // Add all other columns to error message
                for (int i = 2; i <= aMeta.getColumnCount(); i++) {
                    aDescription += aMeta.getColumnLabel(i) + "=" + theResultSet.getString(i) + ", ";
                }

                throw new DependencySqlFailedException("Dependency is NOT in place: " + aDescription);
            });

        } catch (Exception e) {
            throw new DependencySqlFailedException("Dependency check failed: " + e.getMessage());
        }
    }

    private static boolean isBooleanType(int theColumnType) {
        return (theColumnType == Types.BOOLEAN ||
                theColumnType == Types.BIT ||
                theColumnType == Types.TINYINT ||
                theColumnType == Types.SMALLINT ||
                theColumnType == Types.INTEGER ||
                theColumnType == Types.BIGINT
        );
    }
}
