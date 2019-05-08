package com.ws.ogre.v2.db;

import com.ws.ogre.v2.utils.VariableUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * This class holds the SQL(s) to execute for a script. The last SQL is the one used to query and fetch the final result
 * to convert to AVRO. The other SQLs are just executed and used to prepare the final result if needed.
 * <p/>
 * The SQLs can contain comments enclosed by /<star> ... <star>/ or starting with //.
 * <p/>
 * Any variable references on format ${<name>} will be replaced by values in supplied variables if any matching.
 */
public class SqlScript {

    private List<String> myExecSqls = new ArrayList<>();
    private List<String> myDependencySqls = new ArrayList<>();
    private String myQuerySql;

    public SqlScript(String theSqlOrUrl, Map<String, String> theVars) throws IOException {

        if (!theSqlOrUrl.contains("://")) {
            parseFromSqlString(theVars, theSqlOrUrl); // For single query, its the only query.
        } else {
            parseFromSqlString(theVars, IOUtils.toString(new URL(theSqlOrUrl)));
        }
    }

    public SqlScript(Map<String, String> theVars, String theSqlString) throws IOException {
        parseFromSqlString(theVars, theSqlString);
    }

    private void parseFromSqlString(Map<String, String> theVars, String theSqlString) {
        theSqlString = SqlUtils.removeComments(theSqlString);

        theSqlString = VariableUtils.replaceVariables(theSqlString, theVars);

        int aPos = theSqlString.lastIndexOf(";");

        if (aPos < 0) {
            throw new RuntimeException("No rows in script");
        }

        theSqlString = theSqlString.substring(0, aPos) + ";";

        String[] aSqls = split(theSqlString);

        // All sqls except the last 1 is some plain execution sql.
        for (int i = 0; i < aSqls.length - 1; i++) {
            String aSql = aSqls[i];

            // Dependency sqls are mainly some pre-condition sqls (returns boolean) which must be true to execute other sqls.
            if (aSql.contains("@dependency")) {
                myDependencySqls.add(aSql.replace("@dependency", "").trim());
            }

            // All unannotated sqls are execution sqls.
            else {
                myExecSqls.add(aSql.trim());
            }
        }

        // Last 1 is the query sql from which will mainly be a select operation that returns data.
        myQuerySql = aSqls[aSqls.length - 1];
    }

    public List<String> getDependencySqls() {
        return myDependencySqls;
    }

    public List<String> getExecSqls() {
        return myExecSqls;
    }

    public String getQuerySql() {
        return myQuerySql;
    }

    private static String[] split(String theString) {

        ArrayList<String> aStrings = new ArrayList<>();

        String aString = "";

        boolean inQuote = false;

        for (char aChar : theString.toCharArray()) {

            if (aChar == '\'') {
                inQuote = !inQuote;
            }

            if (!inQuote && aChar == ';') {
                aStrings.add(aString);
                aString = "";
                continue;
            }

            aString += aChar;
        }

        return aStrings.toArray(new String[aStrings.size()]);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> aVars = new HashMap<>();
        aVars.put("apa", "banan");
        aVars.put("FROM", "2015-11-11");
        aVars.putAll(System.getenv());

        SqlScript aSqls = new SqlScript("file://script.sql", aVars);

        for (String aS : aSqls.getExecSqls()) {
            System.out.println(aS);
        }

        System.out.println("Query:");
        System.out.printf(aSqls.getQuerySql());
    }
}
