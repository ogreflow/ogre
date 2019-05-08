package com.ws.ogre.v2.commands.db2avro;

import com.ws.common.avrologging.datalake.DataLakePublisher;
import com.ws.common.avrologging.writer.v2.AvroWriter;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.db2avro.CliCommand.DdlCommand.Dialect;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesByType;
import com.ws.ogre.v2.db.DependencySqlUtils;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import com.ws.ogre.v2.db.SqlScript;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.utils.SleepUtil;
import org.apache.avro.generic.GenericRecord;

import javax.sql.rowset.JdbcRowSet;
import java.io.File;
import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Class quering DB and exporting result to S3 in AVRO format.
 */
public class DbToAvroHandler {

    private static final Logger ourLogger = Logger.getLogger();
    private static final String SYNCING_FILE_PREFIX = "SyncingPart-";

    private Config myConfig;

    private DataFileHandler myDataFileHandler;
    private S3Client myS3Client;
    private JdbcDbHandler myDbHandler;

    protected Set<String> myTypes;

    private Map<String, String> myVars = new HashMap<>();

    public DbToAvroHandler(Config theConfig, Set<String> theCliTypes, Map<String, String> theVariables) {

        myConfig = theConfig;

        myS3Client = new S3Client(theConfig.dstAccessKey, theConfig.dstSecret);
        myDataFileHandler = new DataFileHandler(myS3Client, new S3Url(theConfig.getDstS3FullPath()));

        myDbHandler = JdbcDbHandlerBuilder.getInstance().buildJdbcDbHandler(theConfig.getSrcDbConfig());

        myTypes = getTypes(theCliTypes, theConfig.types);

        initForDoingQueryForGlobalData(theVariables);
    }

    private void initForDoingQueryForGlobalData(Map<String, String> theVariables) {
        myVars.putAll(System.getenv());
        myVars.putAll(theVariables);
    }

    protected void initForDoingQueryForChunk(DateHour.Range theChunk) {
        DateHour aFromHour = theChunk.getFrom();
        myVars.put("from", aFromHour.format("yyyy-MM-dd HH:mm:ss"));
        myVars.put("fromDate", aFromHour.format("yyyy-MM-dd"));
        myVars.put("fromYear", aFromHour.format("yyyy"));
        myVars.put("fromMonth", aFromHour.format("MM"));
        myVars.put("fromDay", aFromHour.format("dd"));
        myVars.put("fromHour", aFromHour.format("HH"));

        DateHour aToHour = theChunk.getTo().getNextDateHour();
        myVars.put("before", aToHour.format("yyyy-MM-dd HH:mm:ss")); // Exclusive.
        myVars.put("to", aToHour.format("yyyy-MM-dd HH:mm:ss")); // Exclusive. Legacy. Do not use it. Use 'before'. 'to' doesn't sound exclusive.
        myVars.put("beforeDate", aToHour.format("yyyy-MM-dd")); // Exclusive.
        myVars.put("beforeYear", aToHour.format("yyyy"));
        myVars.put("beforeMonth", aToHour.format("MM"));
        myVars.put("beforeDay", aToHour.format("dd"));
        myVars.put("beforeHour", aToHour.format("HH"));

        myVars.put("now", new DateHour(new Date()).format("yyyy-MM-dd HH:mm:ss"));
        myVars.put("nowDate", new DateHour(new Date()).format("yyyy-MM-dd"));
        myVars.put("nowYear", new DateHour(new Date()).format("yyyy"));
        myVars.put("nowMonth", new DateHour(new Date()).format("MM"));
        myVars.put("nowDay", new DateHour(new Date()).format("dd"));
        myVars.put("nowHour", new DateHour(new Date()).format("HH"));

        myVars.put("s3DstPath", myConfig.getDstS3FullPath());
        myVars.put("s3AccessKeyId", myS3Client.getCredentials().getAWSAccessKeyId());
        myVars.put("s3SecretKey", myS3Client.getCredentials().getAWSSecretKey());
        myVars.put("s3StorageClass", myConfig.dstStorageClass.toString());
    }

    private void initForDoingQueryForType(String theType) {
        myVars.put("type", theType);
    }

    private void initForDoingQueryForSeparator(StorageType theStorageType) {
        if (theStorageType == StorageType.TSV) {
            myVars.put("valueSeparator", "\\t");

        } else if (theStorageType == StorageType.CSV) {
            myVars.put("valueSeparator", ",");
        }
    }

    public void ddls(final Dialect theDialect) throws Exception {
        if (myConfig.dstStorageType != StorageType.AVRO) {
            throw new IllegalArgumentException("'ddl' command is only for avro storage type");
        }

        ourLogger.info("Run SQL(s) and dump DDL(s) for result");

        for (final String aType : myTypes) {

            ourLogger.info("Execute SQL(s) for type: %s", aType);

            Config.Sql aSqlOrScript = myConfig.getSql(aType);

            if (aSqlOrScript == null) {
                throw new IllegalArgumentException("No SQL found for type '" + aType + "' in config");
            }

            SqlScript aScript = new SqlScript(aSqlOrScript.sql, myVars);

            for (String aSql : aScript.getExecSqls()) {
                ourLogger.debug("Execute: %s", aSql);
                myDbHandler.executeUpdate(aSql);
            }

            ourLogger.debug("Query: %s", aScript.getQuerySql());

            myDbHandler.query(aScript.getQuerySql(), (JdbcRowSet theRowSet) -> {

                String aDdl = RowSetToDdl.getDdl(aType, theRowSet, theDialect);
                System.out.println(aDdl);

                String anImport = RowSetToDdl.getColumnMappings(aType, theRowSet, theDialect);
                System.out.println(anImport);
            });
        }
    }

    public void dump(DateHour.Range theTimeRange, DateHour.Range.Chunking theChunking, boolean theReplace, boolean theSkipDependencyCheck) {
        // If no time range given set it to current slot of the chunking.
        if (theTimeRange == null) {
            theTimeRange = DateHour.Range.getChunkRangeOfCurrentTime(0, theChunking);
        }

        ourLogger.info("Run SQL(s) for %s and dump result to file(s) on S3", theTimeRange);

        // Calc chunks
        DateHour.Ranges aChunks = theTimeRange.getChunkedRanges(theChunking);

        // Load data chunk by chunks
        for (DateHour.Range aChunk : aChunks) {
            initForDoingQueryForChunk(aChunk);
            dumpWithRetry(aChunk, theReplace, theSkipDependencyCheck, false, 30);
        }
    }

    private void dumpWithRetry(DateHour.Range theTimeChunk, boolean theReplace, boolean theSkipDependencyCheck, boolean theIsPartial, int theTryCount) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theTryCount; i++) {
            try {
                dumpTimeRange(theTimeChunk, theReplace, theSkipDependencyCheck, theIsPartial);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully dumped SQL(s) after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alert.getAlert().alert("Failed to dump SQL(s), will delete already dumped data and retry dump in 60 s. (" + (theTryCount - i) + " retries left)", e);
                } else {
                    ourLogger.warn("Failed to dump SQL(s), will delete already dumped data and retry dump in 60 s. (" + (theTryCount - i) + " retries left)", e);
                }

                SleepUtil.sleepForRetry();

                theReplace = true;
            }
        }

        throw new RuntimeException("Failed to dump SQL(s)");
    }

    protected void dumpTimeRange(DateHour.Range theUnitChunk, boolean theReplace, boolean theSkipDependencyCheck, boolean theIsPartial) throws Exception {
        String aDstFileNamePrefix = theIsPartial ? SYNCING_FILE_PREFIX : null;

        // First remove any partial files.
        deleteExisting(myTypes, theUnitChunk, SYNCING_FILE_PREFIX);

        ourLogger.info("Run SQL(s) and dump result as avro files on S3 for: %s", theUnitChunk);

        // Should we replace old existing dumps, then delete old first
        if (theReplace) {
            deleteExisting(myTypes, theUnitChunk);

            dumpTypes(myTypes, theUnitChunk, theSkipDependencyCheck, aDstFileNamePrefix);

        } else {
            // Check existing dumps and resolve types to dump for time range
            Set<String> aTypes = resolveTypesNotDumpedYet(myTypes, theUnitChunk);

            // Dump them
            dumpTypes(aTypes, theUnitChunk, theSkipDependencyCheck, aDstFileNamePrefix);
        }

        ourLogger.info("Dump done");
    }

    private Set<String> resolveTypesNotDumpedYet(Set<String> theTypes, DateHour.Range theTimeRange) {

        ourLogger.info("Check %s for types to dump", theTimeRange);

        // Fetch already dumped files/types for period
        DataFiles aCreatedFiles = myDataFileHandler.findFilesByTimeRange(theTimeRange.getFrom(), theTimeRange.getTo(), theTypes);

        // Index them by type
        DataFilesByType aCreatedFilesByType = new DataFilesByType(aCreatedFiles);

        // Resolve the types not already dumped
        Set<String> aTypes = new HashSet<>(theTypes);
        aTypes.removeAll(aCreatedFilesByType.getTypes());

        return aTypes;
    }

    private void dumpTypes(Set<String> theTypes, final DateHour.Range theTimeChunk, boolean theSkipDependencyCheck, String theDstFileNamePrefix) throws Exception {

        if (theTypes.isEmpty()) {
            ourLogger.info("Nothing to dump");
            return;
        }

        // Iterate over all types and dump SQL as avro
        for (final String aType : theTypes) {

            ourLogger.info("Execute SQL(s) for type: %s", aType);

            // Get SQL(s) for type
            Config.Sql aSqlOrScript = myConfig.getSql(aType);

            if (aSqlOrScript == null) {
                throw new IllegalArgumentException("No SQL found for type '" + aType + "' in config");
            }

            // Parse and extracts SQLs from script
            initForDoingQueryForType(aType);
            initForDoingQueryForSeparator(myConfig.dstStorageType);
            SqlScript aScript = new SqlScript(aSqlOrScript.sql, myVars);

            // Make sure we pass the dependencies (if asked to).
            if (!theSkipDependencyCheck) {
                for (String aSql : aScript.getDependencySqls()) {
                    ourLogger.debug("Dependency Query: %s", aSql.replace('\n', ' '));

                    DependencySqlUtils.checkDependency(myDbHandler, aSql);
                }
            }

            if (myConfig.dstStorageType == StorageType.AVRO) {
                dumpTypeToAvro(theTimeChunk, aType, aScript, theDstFileNamePrefix);

            } else {
                dumpTypeToCsv(aScript);
            }
        }
    }

    private void dumpTypeToCsv(SqlScript theScript) throws Exception {
        if (myConfig.srcDbType != JdbcDbHandler.DbType.REDSHIFT) {
            throw new IllegalArgumentException(myConfig.srcDbType + " is not supported yet");
        }

        // All sqls are executable.
        for (String aSql : theScript.getExecSqls()) {
            ourLogger.debug("Execute: %s", aSql.replace('\n', ' '));
            myDbHandler.executeUpdate(aSql);
        }

        ourLogger.debug("Query: %s", theScript.getQuerySql().replace('\n', ' '));
        myDbHandler.executeUpdate(theScript.getQuerySql());
    }

    private void dumpTypeToAvro(DateHour.Range theTimeChunk, String theType, SqlScript theScript, String theAvroFileNamePrefix) throws Exception {
        // All SQL(s) except the last one in script are for preparing end result, execute them here.
        for (String aSql : theScript.getExecSqls()) {
            ourLogger.debug("Execute: %s", aSql.replace('\n', ' '));
            myDbHandler.execute(aSql);
        }

        // The last SQL is the query returning the records to dump to file.
        ourLogger.debug("Query: %s", theScript.getQuerySql().replace('\n', ' '));
        executeAndWriteAvro(theTimeChunk, theType, theScript.getQuerySql(), theAvroFileNamePrefix);
    }

    private void executeAndWriteAvro(final DateHour.Range theTimeChunk, final String theType, String theQuerySql, String theAvroFileNamePrefix) throws Exception {
        myDbHandler.query(theQuerySql, new JdbcDbHandler.RsAction() {

            @Override
            public void onData(ResultSet theRowSet) throws Exception {

                // Create a converter that converts from DB records to avro records.
                RowSetToAvro aConverter = new RowSetToAvro("com.ws.db2avro." + theType, theRowSet);

                // Create a publisher that writes the avro records to local filesystem and finally uploads them to S3.
                DataLakePublisher aPublisher = new DataLakePublisher(
                        myConfig.dstComponent, myConfig.dstSource, myConfig.localAvroRoot,
                        myConfig.dstRoot.bucket, myS3Client.getCredentials().getAWSAccessKeyId(), myS3Client.getCredentials().getAWSSecretKey()
                );

                // Set storage class for uploaded files
                aPublisher.setStorageClass(myConfig.dstStorageClass);
                aPublisher.setFileNamePrefix(theAvroFileNamePrefix /* Can be null */);

                // Create a writer handling avro records packaging
                AvroWriter<GenericRecord> aWriter = aPublisher.createGenericRecordWriter(aConverter.getSchema(), Integer.MAX_VALUE - 100000, 10 * 3600);

                int aRecords = 0;

                while (true) {
                    // Get DB record and convert it to avro
                    GenericRecord aRecord = aConverter.next();

                    // No more records?
                    if (aRecord == null) {
                        break;
                    }

                    // Get timestamp value if any such exists
                    Long aTimestamp = (Long) aRecord.get("timestamp");

                    // Check if record timestamp within given range
                    if (aTimestamp != null && !theTimeChunk.inRange(aTimestamp)) {
                        throw new RuntimeException("Returned record contains 'timestamp' column (" + new Date(aTimestamp) + ") with time that is not within requested time range " + theTimeChunk);
                    }

                    // If no time range given, then use current time.
                    if (aTimestamp == null) {
                        aTimestamp = theTimeChunk.getFrom().getTime();
                    }

                    aWriter.write(aTimestamp, aRecord);

                    aRecords++;
                }

                // Close all avro files and upload them to S3
                Collection<File> aFiles = aPublisher.publish();
                aPublisher.close();

                // TODO: We need to fix sync with replace load option to work with multiple files. Until then just try to catch them using alarms...
                if (aFiles.size() > 1) {
                    Alert.getAlert().alert("Data was written to multiple files, that's not supported by replace. This needs fixing... %s", Arrays.toString(aFiles.toArray()));
                }

                ourLogger.info("Query executed and %s records stored as avro.", aRecords);
            }
        });
    }

    private void deleteExisting(Set<String> theTypes, DateHour.Range theTimeRange) {

        ourLogger.info("Deleting files for %s", theTimeRange);

        myDataFileHandler.deleteFilesByTimeRange(theTimeRange.getFrom(), theTimeRange.getTo(), theTypes);
    }

    protected void deleteExisting(Set<String> theTypes, DateHour.Range theTimeRange, String theFileNamePrefix) {
        ourLogger.info("Deleting files for %s with prefix %s", theTimeRange, theFileNamePrefix);

        myDataFileHandler.deleteFilesByTimeRange(theTimeRange.getFrom(), theTimeRange.getTo(), theTypes, "^.*/h=\\d{2}/" + Pattern.quote(theFileNamePrefix) + ".*$");
    }

    private Set<String> getTypes(Set<String> theCliTypes, String[] theConfigTypes) {
        if (theCliTypes != null && !theCliTypes.isEmpty()) {
            return theCliTypes;
        }

        if (theConfigTypes == null || theConfigTypes.length == 0) {
            throw new IllegalArgumentException("No 'types' configured");
        }

        return new HashSet<>(Arrays.asList(theConfigTypes));
    }
}
