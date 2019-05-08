package com.ws.ogre.v2.aws;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateUtil;

import java.sql.ResultSet;
import java.util.*;

/**
 * Handler for accessing and querying Athena using JDBC.
 */
public class AthenaClient {

    private static final Logger ourLogger = Logger.getLogger();

    private JdbcDbHandler myDbHandler;

    public AthenaClient(String theHost, Integer thePort, String theAwsAccessKey, String theAwsSecretKey, String theResultS3Url) {
        ourLogger.info("Initiate the Athena driver: %s, %s, %s, %s", theHost, thePort, theAwsAccessKey, theResultS3Url);

        Map<String, Object> someCustomProps = new HashMap<>();
        someCustomProps.put("s3_staging_dir", theResultS3Url);
        someCustomProps.put("connection_timeout", "60000");
        someCustomProps.put("socket_timeout", "" + 2 * 60 * 60 * 1000);

        myDbHandler = new JdbcDbHandler(JdbcDbHandler.DbType.ATHENA, theHost, thePort, null, theAwsAccessKey, theAwsSecretKey, someCustomProps);
    }

    public JdbcDbHandler getDbHandler() {
        return myDbHandler;
    }

    public void addDatePartitions(String theTable, S3Url theLocation, DateHour.Range theRange) throws Exception {

        ourLogger.info("Add missing Athena %s table partitions", theTable);

        Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aCal.setTime(theRange.getFrom().getDateHour());

        String anUrl = theLocation.toString();

        if (!anUrl.endsWith("/")) {
            anUrl += "/";
        }

        // Get all existing partitions
        Set<String> anExisting = getPartitions(theTable);

        Date aNow = new Date();

        String aSql = "";

        // Generate partition sql for all unregistered partitions
        while (aCal.getTime().before(theRange.getTo().getNextDateHour().getDateHour()) && aCal.getTime().before(aNow)) {

            String aDate = DateUtil.format(aCal.getTime(), "yyyy-MM-dd");
            String aPartition = String.format("d=%s", aDate);

            if (!anExisting.contains(aPartition)) {
                aSql += String.format("  PARTITION (d='%s') LOCATION '%s%s/'%n", aDate, anUrl, aPartition);
            }

            aCal.add(Calendar.DATE, 1);
        }

        if (aSql.isEmpty()) {
            ourLogger.info("No partitions to add");
            return;
        }

        aSql = "ALTER TABLE " + theTable + " ADD IF NOT EXISTS \n" + aSql;

        myDbHandler.execute(aSql);
    }

    public boolean addHourPartitions(String theTable, S3Url theLocation, DateHour.Range theRange) throws Exception {

        String anUrl = theLocation.toString();

        if (!anUrl.endsWith("/")) {
            anUrl += "/";
        }

        String aSql = "ALTER TABLE " + theTable + " ADD IF NOT EXISTS \n";

        for (DateHour aHour : theRange.getHours()) {

            if (aHour.getTime() >= System.currentTimeMillis()) {
                break;
            }

            String aHourStr = aHour.format("HH");
            String aDateStr = aHour.format("yyyy-MM-dd");

            aSql += String.format("  PARTITION (d='%s', h=%s) LOCATION '%sd=%s/h=%s/'%n", aDateStr, aHourStr, anUrl, aDateStr, aHourStr);
        }

        return myDbHandler.execute(aSql);
    }

    public Set<String> getPartitions(String theTable) throws Exception {
        final Set<String> aPartitions = new HashSet<>();

        myDbHandler.query("show partitions " + theTable, (ResultSet theResultSet) -> {
            while (theResultSet.next()) {
                aPartitions.add(theResultSet.getString(1));
            }
        });

        return aPartitions;
    }
}
