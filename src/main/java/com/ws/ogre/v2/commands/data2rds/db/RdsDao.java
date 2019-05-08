package com.ws.ogre.v2.commands.data2rds.db;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateUtil;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.jdbc.Work;

import javax.persistence.EntityManager;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class RdsDao {

    private static final Logger ourLogger = Logger.getLogger();

    public static final String RDS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String TSV_FIELDS_TERMINATED_BY = "\t";
    private static final String TSV_FIELDS_ENCLOSED_BY_QUOTE = "\"";
    private static final String TSV_FIELDS_ENCLOSED_BY_EMPTY = "";
    private static final String TSV_LINE_TERMINATED_BY = "\n";

    private static RdsDao ourInstance = new RdsDao();

    private String myTimestampColumnName;
    private boolean myIsConvertNull = false;

    private RdsDao() {
    }

    public void init(String theTimestampColumnName, boolean theIsConvertNull) {
        myTimestampColumnName = theTimestampColumnName;
        myIsConvertNull = theIsConvertNull;
    }

    public static RdsDao getInstance() {
        return ourInstance;
    }

    public void insertFromTsvFile(String theTableName, File theTsvFile, String theFieldSeparator, String theValueEnclosure, boolean isToDeleteTsvAfterInsertion) {
        insertFromTsvFile(
                theTableName,
                theTsvFile,
                StringEscapeUtils.escapeJava(StringUtils.defaultString(theFieldSeparator, TSV_FIELDS_TERMINATED_BY)),
                StringEscapeUtils.escapeJava(StringUtils.defaultString(theValueEnclosure, TSV_FIELDS_ENCLOSED_BY_EMPTY)),
                StringEscapeUtils.escapeJava(TSV_LINE_TERMINATED_BY),
                isToDeleteTsvAfterInsertion
        );
    }

    public void insertFromTsvFile(String theTableName, File theTsvFile) {
        insertFromTsvFile(
                theTableName,
                theTsvFile,
                StringEscapeUtils.escapeJava(TSV_FIELDS_TERMINATED_BY),
                StringEscapeUtils.escapeJava(TSV_FIELDS_ENCLOSED_BY_QUOTE),
                StringEscapeUtils.escapeJava(TSV_LINE_TERMINATED_BY),
                true
        );
    }

    public void writeValuesAsTsv(File theTsvFile, String theTableName, List<InsertValues> theRowValues) throws IOException {
        List<RdsTableColumnDetails> someColumnDetails = getColumnDetails(theTableName);

        List<String> someRows = new ArrayList<>();

        for (InsertValues aRowColValues : theRowValues) {
            List<String> someValuesOf1Row = new ArrayList<>();

            for (int j = 0; j < someColumnDetails.size(); j++) {
                Object aValue = getValueToInsert(someColumnDetails.get(j), aRowColValues.get(j));
                someValuesOf1Row.add(aValue == null ? "NULL" : (TSV_FIELDS_ENCLOSED_BY_QUOTE + getEscapedValue(aValue) + TSV_FIELDS_ENCLOSED_BY_QUOTE));
            }

            someRows.add(StringUtils.join(someValuesOf1Row, TSV_FIELDS_TERMINATED_BY));
        }

        FileUtils.writeLines(theTsvFile, "UTF-8", someRows, TSV_LINE_TERMINATED_BY, true);
    }

    private Object getValueToInsert(RdsTableColumnDetails theColumnDetail, Object theValue) {
        if (theValue == null) {
            if (myIsConvertNull) {
                return convertNullValue(theColumnDetail);
            }

            return theValue;
        }

        // For date type things, need to convert "long" input.
        if (theColumnDetail.isDateType() && theValue instanceof Long) {
            return DateUtil.format((Long) theValue, RDS_DATE_FORMAT);
        }

        if (theValue instanceof Utf8) {
            return theValue.toString();
        }

        return theValue;
    }

    private Object convertNullValue(RdsTableColumnDetails theColumnDetail) {
        if (theColumnDetail.isNullable()) {
            return null;
        }

        // For integral number type things, need to convert "null" into 0.
        if (theColumnDetail.isIntegralType()) {
            return new Integer(0);
        }

        // For decimal number type things, need to convert "null" into 0.0.
        if (theColumnDetail.isDecimalType()) {
            return new Double(0.0);
        }

        return null; // What can we do in this case :(
    }

    private String getEscapedValue(Object theValue) {
        // Will write all value as string and let mysql deal with it according to its data type.
        String aStr = theValue.toString();

        // Escape double quotation.
        aStr = aStr.replace(TSV_FIELDS_ENCLOSED_BY_QUOTE, "\\" + TSV_FIELDS_ENCLOSED_BY_QUOTE);

        // Escape line termination.
        aStr = aStr.replace(TSV_LINE_TERMINATED_BY, " ");

        return aStr;
    }

    private void insertFromTsvFile(String theTableName, File theTsvFile, String theFieldSeparator, String theValueEncloser, String theLineSeparator, boolean isToDeleteTsvAfterInsertion) {
        String aSql = ("" +
                " LOAD DATA LOCAL INFILE '" + theTsvFile.getAbsolutePath() + "' INTO TABLE " + theTableName +
                " CHARACTER SET UTF8" +
                " FIELDS TERMINATED BY '" + theFieldSeparator + "' ENCLOSED BY '" + theValueEncloser + "'" +
                " LINES TERMINATED BY '" + theLineSeparator + "'"
        );

        try {
            ourLogger.info("INSERT in %s using local file. Size: %s, Sql: %s", theTableName, FileUtils.byteCountToDisplaySize(FileUtils.sizeOf(theTsvFile)), aSql);
            JpaDbHandler.getInstance().getEntityManager()
                    .createNativeQuery(aSql)
                    .executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + aSql, e);

        } finally {
            if (isToDeleteTsvAfterInsertion) {
                ourLogger.info("Deleting temporary file: %s", theTsvFile.getAbsolutePath());

                if (!FileUtils.deleteQuietly(theTsvFile)) {
                    Alert.getAlert().alert("Unable to delete temporary file: %s", theTsvFile.getAbsolutePath());
                }
            }
        }
    }

    public void executeUpdate(final String theSql) {
        try {
            JpaDbHandler.getInstance().getEntityManager()
                    .createNativeQuery(theSql)
                    .executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + theSql, e);
        }
    }

    private BigInteger getCount(String theSql) {
        try {
            return (BigInteger) JpaDbHandler.getInstance().getEntityManager()
                    .createNativeQuery(theSql)
                    .getSingleResult();

        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + theSql, e);
        }
    }

    public void deleteByTimeRange(String theTable, DateHour theFrom, DateHour theTo) {

        SimpleDateFormat aFormat = new SimpleDateFormat(RDS_DATE_FORMAT);

        String aSql = String.format(
                "DELETE FROM %s WHERE " + myTimestampColumnName + " >= '%s' AND " + myTimestampColumnName + " < '%s'",
                theTable, aFormat.format(theFrom.getDateHour()), aFormat.format(theTo.getNextDateHour().getDateHour())
        );

        ourLogger.info("Sql: %s", aSql);
        executeUpdate(aSql);
    }

    public boolean isAnyDataExists(String theTable, DateHour theFrom, DateHour theTo) {
        SimpleDateFormat aFormat = new SimpleDateFormat(RDS_DATE_FORMAT);

        String aSql = String.format(
                "SELECT COUNT(*) FROM %s WHERE " + myTimestampColumnName + " >= '%s' AND " + myTimestampColumnName + " < '%s'",
                theTable, aFormat.format(theFrom.getDateHour()), aFormat.format(theTo.getNextDateHour().getDateHour())
        );

        try {
            BigInteger aCount = getCount(aSql);
            return aCount != null && aCount.intValue() > 0;

        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + aSql, e);
        }
    }

    public boolean isAnyDataExists(String theTable) {
        String aSql = String.format("SELECT COUNT(*) FROM %s", theTable);

        try {
            BigInteger aCount = getCount(aSql);
            return aCount != null && aCount.intValue() > 0;

        } catch (Exception e) {
            throw new RuntimeException("Failed to run query: " + aSql, e);
        }
    }

    public void deleteAll(String theTable) {
        String aSql = String.format("DELETE FROM %s", theTable);

        ourLogger.info("Sql: %s", aSql);
        executeUpdate(aSql);
    }

    public boolean hasTimestampColumn(final String theTable) {
        for (RdsTableColumnDetails aColumnDetail : getColumnDetails(theTable)) {
            if (isTheTimestampColumn(aColumnDetail)) {
                if (aColumnDetail.getType() == Types.TIMESTAMP || aColumnDetail.getType() == Types.DATE) {
                    return true;
                }

                ourLogger.debug("The table has a 'timestamp' column but it is not of TIMESTAMP: %s", aColumnDetail);
                return false;
            }
        }

        ourLogger.debug("The table has no 'timestamp' column");
        return false;
    }

    public boolean isTheTimestampColumn(RdsTableColumnDetails theColumnDetail) {
        return StringUtils.equals(theColumnDetail.getName(), myTimestampColumnName);
    }

    public List<RdsTableColumnDetails> getColumnDetails(final String theTable) {
        try {
            final List<RdsTableColumnDetails> someColumnDetails = new ArrayList<>();
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            // Get the hibernate session in order to be able to get connection and database meta data
            Session aSession = aManager.unwrap(Session.class);

            aSession.doWork(new Work() {

                @Override
                public void execute(Connection theConnection) throws SQLException {

                    DatabaseMetaData aMetaData = theConnection.getMetaData();

                    // Query table information regarding the table and column to find
                    ResultSet aCat = aMetaData.getColumns(null, null, theTable, null);
                    while (aCat.next()) {
                        RdsTableColumnDetails aDetails = new RdsTableColumnDetails(
                                aCat.getString(4),
                                aCat.getInt(5),
                                !StringUtils.equalsIgnoreCase("NO", aCat.getString(18)) /* Checking !NO instead of YES to include the indeterministic state too. */
                        );

                        someColumnDetails.add(aDetails);
                    }

                    aCat.close();
                }
            });

            return someColumnDetails;

        } catch (Exception e) {
            throw new RuntimeException("Failed to get column details for " + theTable + " table.", e);
        }
    }

    public Set<String> getAllTablesInSchema() {
        try {
            final Set<String> someTables = new HashSet<>();
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            // Get the hibernate session in order to be able to get connection and database meta data
            Session aSession = aManager.unwrap(Session.class);

            aSession.doWork(new Work() {

                @Override
                public void execute(Connection theConnection) throws SQLException {

                    DatabaseMetaData aMetaData = theConnection.getMetaData();

                    // Query table information regarding the table and column to find
                    ResultSet aResultSet = aMetaData.getTables(null, null, null, new String[]{"TABLE"});
                    while (aResultSet.next()) {
                        String aTableCatalog = aResultSet.getString(1);
                        String aTableSchema = aResultSet.getString(2);
                        String aTableName = aResultSet.getString(3);
                        ourLogger.debug("Got table details: catalog=%s, schema=%s, tableName=%s", aTableCatalog, aTableSchema, aTableName);

                        someTables.add(aTableName);
                    }

                    aResultSet.close();
                }
            });

            return someTables;

        } catch (Exception e) {
            throw new RuntimeException("Failed to get table list.", e);
        }
    }

    /**
     * @deprecated TODO:
     */
    public Set<String> getAllPartitionNames(String theTableName) {
        return Collections.emptySet();
    }

    /**
     * @deprecated TODO:
     */
    public void createPartition(String theTableName, String thePartitionName) {

    }

    /**
     * @deprecated TODO:
     */
    public void dropPartition(String theTableName, String thePartitionName) {

    }
}
