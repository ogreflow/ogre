package com.ws.ogre.v2.db;

import com.sun.rowset.JdbcRowSetImpl;

import javax.sql.rowset.JdbcRowSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;


/**
 * Handler for accessing and querying a DB using JDBC.
 */
public class JdbcDbHandler {

    public enum DbType {MYSQL, REDSHIFT, ATHENA}

    private String myUrl;
    private Properties myProps = new Properties();

    public JdbcDbHandler(DbType theType, String theHost, Integer thePort, String theDatabase, String theUser, String thePassword) {
        this(theType, theHost, thePort, theDatabase, theUser, thePassword, null);
    }

    public JdbcDbHandler(DbType theType, String theHost, Integer thePort, String theDatabase, String theUser, String thePassword, Map<String, Object> theCustomProps) {

        try {
            switch (theType) {
                case MYSQL:
                    initMySqlUrl(theHost, thePort, theDatabase);
                    break;

                case REDSHIFT:
                    initRedshiftUrl(theHost, thePort, theDatabase);
                    break;

                case ATHENA:
                    initAthenaUrl(theHost, thePort);
                    break;

                default:
                    throw new IllegalArgumentException("DB type not supported: " + theType);
            }

            initProperties(theUser, thePassword, theCustomProps);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load DB driver", e);
        }
    }

    private void initMySqlUrl(String theHost, Integer thePort, String theDatabase) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");

        if (thePort == null) {
            thePort = 3306;
        }

        myUrl = "jdbc:mysql://" + theHost + ":" + thePort + "/" + theDatabase;
    }

    private void initRedshiftUrl(String theHost, Integer thePort, String theDatabase) throws ClassNotFoundException {
        Class.forName("com.amazon.redshift.jdbc41.Driver");

        if (thePort == null) {
            thePort = 5439;
        }

        myUrl = "jdbc:postgresql://" + theHost + ":" + thePort + "/" + theDatabase + "?tcpKeepAlive=true";
    }

    private void initAthenaUrl(String theHost, Integer thePort) throws ClassNotFoundException {
        Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");

        if (thePort == null) {
            thePort = 443;
        }

        myUrl = "jdbc:awsathena://" + theHost + ":" + thePort + "/";
    }

    private void initProperties(String theUser, String thePassword, Map<String, Object> theCustomProps) {
        myProps.setProperty("user", theUser);
        myProps.setProperty("password", thePassword);

        if (theCustomProps != null) {
            myProps.putAll(theCustomProps);
        }
    }

    public boolean execute(String theSql) throws Exception {

        try (
                Connection aConn = DriverManager.getConnection(myUrl, getConfigProperties());
                Statement aStmt = aConn.createStatement();
        ) {
            return aStmt.execute(theSql);

        } catch (Exception e) {
            throw new Exception("Failed to execute sql: " + theSql, e);
        }
    }

    public int executeUpdate(String theSql) throws Exception {

        try (
                Connection aConn = DriverManager.getConnection(myUrl, getConfigProperties());
                Statement aStmt = aConn.createStatement();
        ) {
            return aStmt.executeUpdate(theSql);

        } catch (Exception e) {
            throw new Exception("Failed to execute sql: " + theSql, e);
        }
    }

    public void query(String theSql, RsAction theAction) throws Exception {

        try (
                Connection aConn = DriverManager.getConnection(myUrl, getConfigProperties());
                Statement aStmt = aConn.createStatement();
        ) {

            theAction.onData(aStmt.executeQuery(theSql));

        } catch (Exception e) {
            throw new Exception("Failed to execute sql: " + theSql, e);
        }
    }

    private Properties getConfigProperties() {
        return myProps;
    }

    public interface RsAction {
        void onData(ResultSet theResultSet) throws Exception;
    }

    /**
     * @deprecated Not working with redshift query :(
     */
    public void query(String theSql, Action theAction) throws Exception {

        try (
                JdbcRowSetImpl aRs = new JdbcRowSetImpl();
        ) {
            aRs.setUrl(myUrl);
            aRs.setUsername(myProps.getProperty("user"));
            aRs.setPassword(myProps.getProperty("password"));

            aRs.setCommand(theSql);

            aRs.execute();

            theAction.onData(aRs);

        } catch (Exception e) {
            throw new Exception("Failed to execute sql: " + theSql, e);
        }
    }

    public interface Action {
        void onData(JdbcRowSet theRowSet) throws Exception;
    }

}
