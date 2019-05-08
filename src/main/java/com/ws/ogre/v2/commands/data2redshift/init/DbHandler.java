package com.ws.ogre.v2.commands.data2redshift.init;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Class for bootstraping an Ogre Redshift database.
 */
public class DbHandler {

    private String myUrl;
    private String mySchema;
    private String myUser;
    private String myPassword;

    static {
        try {
            Class.forName("com.amazon.redshift.jdbc41.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load RedShift driver", e);
        }
    }

    public DbHandler(String theHost, int thePort, String theDatabase, String theSchema, String theUser, String thePassword) {

        myUrl = "jdbc:postgresql://" + theHost + ":" + thePort + "/" + theDatabase + "?tcpKeepAlive=true";
        myUrl += "&" + "currentSchema=" + theSchema;

        mySchema = theSchema;
        myUser = theUser;
        myPassword = thePassword;
    }

    public void ping() throws Exception {
        System.out.println("Testing RedShift connection...");

        Properties aProps = getConfigProperties();

        try (
                Connection aConn = DriverManager.getConnection(myUrl, aProps);
                Statement aStmt = aConn.createStatement();
        ) {
            String sql = "select now()";

            ResultSet aRs = aStmt.executeQuery(sql);

            aRs.close();
        } catch (Exception e) {
            throw new Exception("Failed to connect to RedShift", e);
        }
    }

    private void executeSql(String theSql) throws Exception {
        executeSql(theSql, mySchema);
    }

    private void executeSql(String theSql, String theSchema) throws Exception {

        Properties aProps = getConfigProperties();

        try (
            Connection aConn = DriverManager.getConnection(myUrl, aProps);
            Statement aStmt = aConn.createStatement();
        ) {
            // Set schema to use if requested
            if (theSchema != null) {
                aStmt.executeUpdate("set search_path to " + theSchema);
            }

            aStmt.executeUpdate(theSql);

        } catch (Exception e) {
            throw new Exception("Failed to execute sql: " + theSql, e);
        }
    }

    public void setupSchema() throws Exception {

        System.out.println("Setting up Ogre schema...");

        if (!isSchemaExists()) {
            // First create schema if not exists
            System.out.println("Creating schema '" + mySchema + "' if not exists...");
            executeSql("create schema if not exists " + mySchema + " authorization " + myUser, null);
        } else {
            System.out.println("Schema '" + mySchema + "' already exists. Assuming the user has all permissions in this schema...");
        }

        InputStream anIn = ClassLoader.getSystemResourceAsStream("schema/ogre-schema-redshift.sql");

        if (anIn == null) {
            throw new RuntimeException("Failed to locate ogre-schema-redshift.sql");
        }

        String aSchema = IOUtils.toString(anIn);

        int aPos = aSchema.lastIndexOf(";");

        if (aPos < 0) {
            throw new RuntimeException("No rows to execute ?");
        }

        aSchema = aSchema.substring(0, aPos);

        String[] aSqls = aSchema.split(";");

        for (String aSql : aSqls) {
            executeSql(aSql);
        }
    }

    private boolean isSchemaExists() throws Exception {
        System.out.println("Checking whether schema '" + mySchema + "' exists or not...");

        Properties aProps = getConfigProperties();
        try (
                Connection aConn = DriverManager.getConnection(myUrl, aProps);
                Statement aStmt = aConn.createStatement();
        ) {
            ResultSet aRs = aStmt.executeQuery("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + mySchema + "';");
            boolean exists = aRs.next();
            aRs.close();

            return exists;

        } catch (Exception e) {
            throw new Exception("Failed to check existence of schema", e);
        }
    }

    private Properties getConfigProperties() {
        Properties aProps = new Properties();

        aProps.setProperty("user", myUser);
        aProps.setProperty("password", myPassword);

        return aProps;
    }
}
