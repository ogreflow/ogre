package com.ws.ogre.v2.commands.data2redshift.db;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.datetime.DateHour;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import javax.persistence.EntityManager;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class RedShiftDao {

    public static final String REDSHIFT_DATE_FORMAT = "yyyyMMdd HH:mm:ss";

    public enum Format {AVRO, JSON}

    private static final Logger ourLogger = Logger.getLogger();

    private static RedShiftDao ourInstance = new RedShiftDao();

    private String myAccessKeyId;
    private String mySecretKey;

    private String mySchema;

    private RedShiftDao() {
    }


    public static RedShiftDao getInstance() {
        return ourInstance;
    }

    public void init(String theAccessKeyId, String theSecretKey, String theSchema) {
        myAccessKeyId = theAccessKeyId;
        mySecretKey = theSecretKey;
        mySchema = theSchema;
    }

    public void copy(String theTable, S3Url theManifestUrl, S3Url theMappingUrl, Format theFormat) {
        try {
            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            String aQuery =
                    "COPY " + theTable + "\n" +
                    "FROM '" + theManifestUrl + "'\n" +
                    "WITH CREDENTIALS 'aws_access_key_id=" + myAccessKeyId + ";aws_secret_access_key=" + mySecretKey + "'\n" +
                    "FORMAT " + theFormat + " '" + theMappingUrl + "'\n" +
                    (theFormat == Format.JSON ? "GZIP\n" : "") +
                    "timeformat as 'epochmillisecs'\n" +
                    "MANIFEST\n" +
                    "ACCEPTINVCHARS\n" +
                    "ROUNDEC\n" +
                    "MAXERROR 0\n" +
                    "TRUNCATECOLUMNS\n";
//                    "COMPUPDATE ON\n";

            ourLogger.debug(aQuery);

            aManager
                .createNativeQuery(aQuery)
                .executeUpdate();

        } catch (Exception e) {
            throw new RuntimeException("Failed to issue Redshift COPY for table: " + theTable + ", manifest: " + theManifestUrl + ", mappings: " + theMappingUrl, e);
        }

    }

    public void analyze(String theTable) {
        try {
            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            aManager.createNativeQuery("analyze " + theTable).executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to analyse table: " + theTable, e);
        }
    }

    public void executeUpdate(final String theSql) {
        try {
            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            aManager.createNativeQuery(theSql)
                    .executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + theSql, e);
        }
    }

    public void deleteByTimeRange(String theTable, DateHour theFrom, DateHour theTo) {

        // Add 1h to end to include it in search
        Calendar aTo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aTo.setTime(theTo.getDateHour());
        aTo.add(Calendar.HOUR_OF_DAY, 1);

        SimpleDateFormat aFormat = new SimpleDateFormat(REDSHIFT_DATE_FORMAT);

        String aSql = String.format("delete from %s where timestamp >= '%s' and timestamp < '%s'", theTable, aFormat.format(theFrom.getDateHour()), aFormat.format(aTo.getTime()));

        executeUpdate(aSql);
    }

    public void deleteAll(String theTable) {

        String aSql = String.format("delete from %s", theTable);

        executeUpdate(aSql);
    }

    @SuppressWarnings("all")
    public Set<String> getTablesStartingWith(String thePrefix) {
        try {
            String aSql =
                    "SELECT table_name " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = current_schema() AND table_name LIKE '" + thePrefix + "%'";

            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            List<String> aTableNames = aManager.createNativeQuery(aSql).getResultList();

            return new HashSet<>(aTableNames);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get tables starting with: " + thePrefix, e);
        }
    }

    /**
     * CREATE TABLE LIKE:
     * You can use CREATE TABLE LIKE to recreate the original table; however, the new table will not inherit
     * the primary key and foreign key attributes of the parent table. The new table does inherit the encoding,
     * distkey, sortkey, and notnull attributes of the parent table.
     */
    public void createTableLike(String theSourceTable, String theNewTable) {
        try {
            String aSql = "CREATE TABLE " + theNewTable + " (LIKE " + theSourceTable + ")";

            executeUpdate(aSql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a table: " + theNewTable + " like: " + theSourceTable, e);
        }
    }

    public void recreateView(String theView, List<String> theTables) {
        try {
            if (theTables.isEmpty()) {
                return;
            }

            executeUpdate("drop view if exists " + theView + " CASCADE");

            String aSql = null;

            for (String aTable : theTables) {

                if (aSql == null) {

                    aSql = "SELECT * FROM " + aTable;

                } else {

                    aSql += " UNION ALL SELECT * FROM " + aTable;
                }
            }

            aSql = "CREATE OR REPLACE VIEW " + theView + " AS " + aSql;

            executeUpdate(aSql);

        } catch (Exception e) {
            throw new RuntimeException("Failed to recreate view: " + theView + " for tables: " + theTables, e);
        }
    }

    public void dropTable(String theTable) {
        try {
            String aSql = "DROP TABLE " + theTable + " CASCADE";

            executeUpdate(aSql);

        } catch (Exception e) {
            throw new RuntimeException("Failed to drop table: " + theTable, e);
        }
    }

    public boolean hasTimestampColumn(final String theTable) {

        try {
            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            // Get the hibernate session in order to be able to get connection and database meta data
            Session aSession = aManager.unwrap(Session.class);

            final boolean[] aResult = new boolean[1];

            aSession.doWork(new Work() {

                @Override
                public void execute(Connection theConnection) throws SQLException {

                    DatabaseMetaData aMetaData = theConnection.getMetaData();

                    // Query table information regarding the table and column to find
                    ResultSet aCat = aMetaData.getColumns(null, mySchema, theTable, "timestamp");

                    // Step to first hit
                    boolean isHit = aCat.next();

                    // Did we got a hit?
                    if (!isHit) {
                        ourLogger.debug("The table has no 'timestamp' column");
                        aResult[0] = false;
                        return;
                    }

                    // Get type of column of name 'timestamp'
                    Object aType = aCat.getObject(5);

                    if (!(aType instanceof Integer) || ((Integer)aType) != Types.TIMESTAMP) {
                        ourLogger.debug("The table has a 'timestamp' column but it is not of TIMESTAMP type: %s (%s)", aType, aCat.getObject(6));
                        aResult[0] = false;
                    }

                    aResult[0] = true;
                }
            });

            return aResult[0];

        } catch (Exception e) {
            throw new RuntimeException("Failed to check for 'timestamp' column in " + mySchema + "." + theTable + " table.", e);
        }
    }


}
