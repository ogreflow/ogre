package com.ws.ogre.v2.commands.data2rds;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.avroutils.AvroRecordReader;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.commands.data2rds.db.*;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.DataImportHandler;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.SchemaHandler;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.managed.ManagedDataImportHandler;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.managed.ManagedSchemaHandler;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.plain.PlainDataImportHandler;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.plain.PlainSchemaHandler;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFile;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesById;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesByType;
import com.ws.ogre.v2.utils.CommandSyncer;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateHour.Range;
import com.ws.ogre.v2.datetime.DateHour.Ranges;
import com.ws.ogre.v2.utils.SleepUtil;
import com.ws.ogre.v2.utils.StopWatch;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Handler for importing data into Rds.
 */
public class DataToRdsHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private static final int TSV_FILE_WRITE_CHUNK_LINES = 10000;

    private Config myConfig;

    private DataFileHandler myDataFileHandler;
    private PartitionHandler myPartitionHandler;

    private SchemaHandler mySchemaHandler;
    private DataImportHandler myImportHandler;

    private Set<String> myTypes;
    private Set<String> myRequestedTypes;

    // Import states (to resume upon failures)
    private Set<String> myImportedChunks = new HashSet<>();
    private Set<String> myImportedTypes = new HashSet<>();

    private long myLastImportLogCleanupTime = System.currentTimeMillis();

    public DataToRdsHandler(Config theConfig, Set<String> theTypes) {
        myConfig = theConfig;
        myRequestedTypes = theTypes;
    }

    public void init() {
        myDataFileHandler = new DataFileHandler(new S3Client(myConfig.srcAccessKey, myConfig.srcSecret), myConfig.srcRootDir);
        myPartitionHandler = new PartitionHandler(myConfig.tableSpec, myConfig.partitionings);

        if (myConfig.loadType == Config.LoadType.PLAIN) {
            myImportHandler = new PlainDataImportHandler();
            mySchemaHandler = new PlainSchemaHandler();
        } else {
            myImportHandler = new ManagedDataImportHandler();
            mySchemaHandler = new ManagedSchemaHandler(myConfig.srcDdlDir, myConfig.srcAccessKey, myConfig.srcSecret);
        }

        JpaDbHandler.getInstance().init(myConfig.dstHost, myConfig.dstPort, myConfig.dstDb, myConfig.dstUser, myConfig.dstPwd);
        RdsDao.getInstance().init(myConfig.dstTimestampColumnName, myConfig.dstConvertNullValue);

        // Sync new DDLs if any changes, returns the available types.
        myTypes = syncDdls(true, null);
    }

    public void close() {
        // Must close the DB connection. Otherwise the main thread doesn't exit.
        JpaDbHandler.getInstance().close();
    }

    public void scanAndLoad(int theScanIntervalS, int theLookbackUnits, DateHour.Range.Chunking theChunking, boolean theReplaceAllWithLatest, boolean theReplace, boolean theIsSnapshotFile) {
        CommandSyncer.sync(
                theScanIntervalS,
                theLookbackUnits,
                theChunking,
                theRangeToRun -> {
                    if (theReplaceAllWithLatest) {
                        replaceAllWithLatest(theRangeToRun.getFrom(), theRangeToRun.getTo(), theIsSnapshotFile);
                    } else {
                        load(theRangeToRun.getFrom(), theRangeToRun.getTo(), theChunking, theReplace, theIsSnapshotFile);
                    }
                }
        );
    }

    public void loadWithRetry(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReplace, boolean theIsSnapshotFile, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {

                load(theFrom, theTo, theChunking, theReplace, theIsSnapshotFile);

                ourLogger.info("Processed %s - %s", theFrom, theTo);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully processed files after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alert.getAlert().alert("Failed to process files, will delete processed files for period and retry in 60 s. (" + (theRetries - i) + " retries left)", e);
                } else {
                    ourLogger.warn("Failed to process files, will delete processed files for period and retry in 60 s. (" + (theRetries - i) + " retries left)", e);
                }

                SleepUtil.sleepForRetry();

                // Remove old failed files
                theReplace = true;
            }
        }

        throw new RuntimeException("Failed to process period " + theFrom + "-" + theTo + ".");
    }

    public void replaceAllWithLatestWithRetries(DateHour theFrom, DateHour theTo, boolean theIsSnapshotFile, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {
                replaceAllWithLatest(theFrom, theTo, theIsSnapshotFile);

                ourLogger.info("Loaded %s - %s", theFrom, theTo);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully replaced data after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alert.getAlert().alert("Failed to replace data, will retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                } else {
                    ourLogger.warn("Failed to replace data, will retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                }

                SleepUtil.sleepForRetry();
            }
        }

        throw new RuntimeException("Failed to reload for period " + theFrom + "-" + theTo + ".");
    }

    private void replaceAllWithLatest(final DateHour theFrom, final DateHour theTo, final boolean theIsSnapshotFile) {

        ourLogger.info("Check for new files for period %s to %s and replace old with newest if any found", theFrom, theTo);

        final DataFilesByType aNewFiles = JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<DataFilesByType>() {
            @Override
            public DataFilesByType doTask() {
                return getNewFilesToImport(theFrom, theTo, theIsSnapshotFile);
            }
        });

        if (aNewFiles.isEmpty()) {
            return;
        }

        // Sync new DDLs if any changes (in own transaction)
        myTypes = syncDdls(false, myTypes);

        // Manage table partitions for period (in own transaction)
        partitionTables(theFrom, theTo);

        // Remove old obsolete import logs
        cleanupImportLog();

        for (final String aType : aNewFiles.getTypes()) {

            if (myImportedTypes.contains(aType)) {
                continue;
            }

            JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Void>() {
                @Override
                public Void doTask() {
                    deleteAll(aType);

                    DataFiles aFiles = aNewFiles.getForType(aType);

                    // Get latest/newest
                    aFiles.sortDesc();

                    DataFiles aToImport = new DataFiles();
                    aToImport.add(aFiles.get(0));

                    copyIntoRds(aType, aToImport);
                    myImportHandler.markAsImported(aFiles);

                    return null;
                }
            });

            myImportedTypes.add(aType);
        }

        myImportedTypes.clear();
    }

    public void delete(final DateHour theFrom, final DateHour theTo) {

        JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Void>() {
            @Override
            public Void doTask() {
                ourLogger.info("Delete data from  %s - %s", theFrom, theTo);

                for (String aType : myTypes) {
                    deleteExisting(aType, theFrom, theTo);
                }

                return null;
            }
        });
    }

    private void load(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReplace, boolean theIsSnapshotFile) {

        ourLogger.info("Load data for %s - %s: chunked=%s, reload=%s", theFrom, theTo, theChunking, theReplace);

        // Sync new DDLs if any changes (in own transaction)
        myTypes = syncDdls(false, myTypes);

        // Manage table partitions for period (in own transaction)
        partitionTables(theFrom, theTo);

        // Remove old obsolete import logs
        cleanupImportLog();

        // Load data in chunks by type
        loadChunked(theFrom, theTo, theChunking, theReplace, theIsSnapshotFile);
    }

    private void loadChunked(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReplace, boolean theIsSnapshotFile) {

        // Calc chunks
        Range aPeriod = new Range(theFrom, theTo);
        Ranges aChunks = aPeriod.getChunkedRanges(theChunking);

        // Load data chunk by chunks
        for (Range aChunk : aChunks) {

            for (String aType : myTypes) {

                String aChunkKey = aType + ":" + aChunk;

                // Skip if already imported this chunk (in case we had a failure and this is a retry)?
                if (myImportedChunks.contains(aChunkKey)) {
                    ourLogger.info("Already imported %s, wind forward.", aChunk);
                    continue;
                }

                loadInTransaction(aType, aChunk.getFrom(), aChunk.getTo(), theReplace, theIsSnapshotFile);

                myImportedChunks.add(aChunkKey);
            }
        }

        myImportedChunks.clear();
    }

    private void loadInTransaction(final String theType, final DateHour theFrom, final DateHour theTo, final boolean theReplace, final boolean theIsSnapshotFile) {
        JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Void>() {
            @Override
            public Void doTask() {
                loadPeriod(theType, theFrom, theTo, theReplace, theIsSnapshotFile);
                return null;
            }
        });
    }

    private void loadPeriod(String theType, DateHour theFrom, DateHour theTo, boolean theReplace, boolean theIsSnapshotFile) {

        ourLogger.info("Load '%s' for %s - %s", theType, theFrom, theTo);

        // Resolves data files to be imported
        DataFiles aToImport = getNewFilesToImport(theType, theFrom, theTo, theIsSnapshotFile);
        if (aToImport.size() <= 0) {
            ourLogger.info("No '%s' data files found for %s - %s to import.", theType, theFrom, theTo);
            return; // We have no avro files. Thus abort further action (specially deletion of existing data).
        }

        // Delete old data if it should be replaced
        if (theReplace) {
            deleteExisting(theType, theFrom, theTo);

        } else {
            if (isDataExists(theType, theFrom, theTo)) {
                ourLogger.info("Data exists already for '%s' within the range %s - %s, Skipping load.", theType, theFrom, theTo);
                return;
            }
        }

        copyIntoRds(theType, aToImport);
        myImportHandler.markAsImported(aToImport);
    }

    private DataFilesByType getNewFilesToImport(DateHour theFrom, DateHour theTo, boolean theIsSnapshotFile) {
        DataFiles aFiles = new DataFiles();

        for (String aType : myTypes) {
            aFiles.addAll(getNewFilesToImport(aType, theFrom, theTo, theIsSnapshotFile));
        }

        return new DataFilesByType(aFiles);
    }

    private DataFiles getNewFilesToImport(String theType, DateHour theFrom, DateHour theTo, boolean theIsSnapshotFile) {

        // Get S3 data files within the range.
        // Note: type should be lowercase as DataLakePublisher uses %t and that makes all types lowercase in s3.
        DataFiles aFiles = myDataFileHandler.findFilesByTimeRange(theFrom, theTo, Collections.singleton(theType.toLowerCase()));

        // Get imported files within the range
        DataFiles someImported = myImportHandler.getImportedFiles(theFrom, theTo, theType);

        // Index them on Id
        DataFilesById someImportedById = new DataFilesById(someImported);

        DataFiles aToImport = new DataFiles();

        int i = 0;
        // Resolves data files to be imported
        for (DataFile aFile : aFiles) {
            if (aFile == null) {
                ourLogger.warn("Null file for idx %s: id=%s", i);
                continue;
            }
            i++;

            if (!someImportedById.contains(aFile)) {
                aToImport.add(aFile);
            }
        }

        ourLogger.info("Found %s files to import for type: %s, period: %s - %s", aToImport.size(), theType, theFrom, theTo);

        // For snapshot type file, only consider the latest files.
        if (theIsSnapshotFile && CollectionUtils.isNotEmpty(aToImport)) {
            DataFile aLatestDataFile = aToImport.get(aToImport.size() - 1);

            ourLogger.info("Among %s files, using latest one '%s' for '%s' in range %s - %s.", aToImport.size(), aLatestDataFile, theType, theFrom, theTo);
            aToImport = new DataFiles(Arrays.asList(aLatestDataFile));
        }

        return aToImport;
    }

    private void copyIntoRds(String theType, DataFiles theToImport) {
        // Import files in chronological order
        theToImport.sortAsc();

        String aTable = myPartitionHandler.getPartitionTableName(theType);

        Config.LoadFormat aFormat = detectFormat(theToImport.get(0));
        if (aFormat == Config.LoadFormat.TSV) {
            copyIntoRdsFromTsv(aTable, theToImport);
        } else {
            copyIntoRdsFromAvroOrJson(aTable, theToImport);
        }
    }

    private void copyIntoRdsFromTsv(String theTable, DataFiles theToImport) {
        for (DataFile aFile : theToImport) {
            File aTsvFile = myDataFileHandler.downloadDataFileAndUnzip(aFile);

            StopWatch aWatch = new StopWatch();
            ourLogger.info("INSERT into DB. (Table: %s, Files: %s)", theTable, aFile);
            RdsDao.getInstance().insertFromTsvFile(
                    theTable,
                    aTsvFile,
                    myConfig.loadFormatTsvFieldSeparator,
                    myConfig.loadFormatTsvValueEnclosure,
                    true
            );
            ourLogger.info("INSERT done (%s)", aWatch);
        }
    }

    private void copyIntoRdsFromAvroOrJson(String theTable, DataFiles theToImport) {
        StopWatch aWatch = new StopWatch();

        List<String> someKeys = mySchemaHandler.getJsonMappingsSequence(theTable);
        if (CollectionUtils.isEmpty(someKeys)) {
            // No column mapping is defined. Derive it from destination table's column name's lowercase version.
            someKeys = getJsonMappingsFromColumnNames(theTable);
        }

        File aTsvFile = readJsonPathValuesAndWriteInTsv(theTable, theToImport, someKeys);

        ourLogger.info("INSERT into DB. (Table: %s, Files: %s, Keys: %s)", theTable, theToImport, someKeys);
        RdsDao.getInstance().insertFromTsvFile(theTable, aTsvFile);
        ourLogger.info("INSERT done (%s)", aWatch);
    }

    private Set<String> syncDdls(final boolean theInitiate, final Set<String> theOldTypes) {
        return JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Set<String>>() {
            @Override
            public Set<String> doTask() {
                // Import new DDLs if any
                boolean isChanged = mySchemaHandler.syncDdls();

                // Do we need to resolve the types to work with
                if (isChanged || theInitiate) {
                    return getTypesToWorkWith();
                }

                return theOldTypes;
            }
        });
    }

    private void partitionTables(final DateHour theFrom, final DateHour theTo) {
        JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Void>() {
            @Override
            public Void doTask() {
                myPartitionHandler.partitionTable(myTypes, theFrom, theTo);
                return null;
            }
        });
    }

    private void cleanupImportLog() {

        if (System.currentTimeMillis() > myLastImportLogCleanupTime + 24 * 60 * 60 * 1000l) {

            myLastImportLogCleanupTime = System.currentTimeMillis();

            for (final String aType : myTypes) {

                final DateHour aStart = myPartitionHandler.getStart(aType);

                if (aStart == null) {
                    continue;
                }

                ourLogger.info("Remove old import logs before: " + aStart);

                try {
                    JpaDbHandler.getInstance().executeInTransaction(new JpaDbHandler.ExecutionTask<Void>() {
                        @Override
                        public Void doTask() {
                            int aDeleted = myImportHandler.deleteByTimeRange(aType, new DateHour("2000-01-01:00"), aStart);
                            ourLogger.info("Removed %s old obsolete import logs", aDeleted);

                            return null;
                        }
                    });

                } catch (Exception e) {
                    Alert.getAlert().alert("Failed to remove old import logs for " + aType + ", older than " + aStart, e);
                }
            }
        }

    }

    private void deleteExisting(String theType, DateHour theFrom, DateHour theTo) {

        String aTable = myPartitionHandler.getPartitionTableName(theType);

        // Delete ALL data in table for type if no "timestamp" column exists, else just delete the data for period.
        if (!RdsDao.getInstance().hasTimestampColumn(aTable)) {

            ourLogger.info("No timestamp column in table '%s', delete all current rows", aTable);

            RdsDao.getInstance().deleteAll(aTable);
            myImportHandler.deleteAllByType(theType);

        } else {

            ourLogger.info("Deleting rows in db table %s for period %s to %s", aTable, theFrom, theTo);

            RdsDao.getInstance().deleteByTimeRange(aTable, theFrom, theTo);
            myImportHandler.deleteByTimeRange(theType, theFrom, theTo);
        }
    }

    private boolean isDataExists(String theType, DateHour theFrom, DateHour theTo) {
        String aTable = myPartitionHandler.getPartitionTableName(theType);

        if (!RdsDao.getInstance().hasTimestampColumn(aTable)) {
            return RdsDao.getInstance().isAnyDataExists(aTable);
        }

        return RdsDao.getInstance().isAnyDataExists(aTable, theFrom, theTo);
    }

    private void deleteAll(String theType) {

        String aTable = myPartitionHandler.getPartitionTableName(theType);
        ourLogger.info("Delete all rows for type %s from table: %s", theType, aTable);
        RdsDao.getInstance().deleteAll(aTable);
    }

    private Set<String> getTypesToWorkWith() {

        // Get all tables/types imported
        Set<String> someTypesInSchema = mySchemaHandler.getExistingAllTypes();  /* All types in schema */
        Set<String> someTypesInS3 = myDataFileHandler.getAllTypes(); /* All types in s3 */
        Set<String> allTypes = new HashSet<>(CollectionUtils.intersection(someTypesInSchema, someTypesInS3));

        ourLogger.info("Types in schema: %s, in S3: %s, common: %s, requested: %s", someTypesInSchema, someTypesInS3, allTypes, myRequestedTypes);

        // Should we process all known types ?
        if (myRequestedTypes == null) {
            return allTypes;
        }

        // No types in s3 means the case where no avro is generated yet.
        if (CollectionUtils.isEmpty(someTypesInS3)) {
            ourLogger.warn("No types found to work with. Please review things.");
            return Collections.emptySet();
        }

        // Validate that we do not have any misspelled or missing requested types
        for (String aType : myRequestedTypes) {
            if (!someTypesInS3.contains(aType)) {
                throw new IllegalArgumentException("Type: " + aType + " does not exist in S3, must be one of: " + someTypesInS3);
            }
        }

        return myRequestedTypes;
    }

    private List<String> getJsonMappingsFromColumnNames(String theTableName) {
        List<String> someJsonKeys = new ArrayList<>();

        for (RdsTableColumnDetails aColumnDetail : RdsDao.getInstance().getColumnDetails(theTableName)) {
            if (RdsDao.getInstance().isTheTimestampColumn(aColumnDetail)) {
                someJsonKeys.add("$." + myConfig.srcTimestampColumnName); // Default "$.timestamp". Otherwise, specify in configuration.
            } else {
                someJsonKeys.add("$." + aColumnDetail.getName().toLowerCase());
            }
        }

        return someJsonKeys;
    }

    private File readJsonPathValuesAndWriteInTsv(String theTable, DataFiles theFiles, List<String> theJsonKeysSequence) {
        try {
            File aTsvFile = File.createTempFile("data-" + theTable + "-", ".tsv");
            aTsvFile.deleteOnExit(); // Safe guard to delete even though we will manually delete it later.

            for (DataFile aFile : theFiles) {
                Config.LoadFormat aFormat = detectFormat(aFile);

                if (aFormat == Config.LoadFormat.AVRO) {
                    readJsonPathValuesFromAvroAndWriteInTsv(aFile, theJsonKeysSequence, theTable, aTsvFile);
                }

                // TODO: Support for json.gz file?
            }

            return aTsvFile;

        } catch (IOException e) {
            throw new RuntimeException("Unable to extract json paths from avro: " + theFiles, e);
        }
    }

    private Config.LoadFormat detectFormat(DataFile theFile) {
        if (theFile.isAvroFile()) {
            return Config.LoadFormat.AVRO;
        }

        if (theFile.isJson() || theFile.isGzipedJson()) {
            return Config.LoadFormat.JSON;
        }

        return Config.LoadFormat.TSV;
    }

    private void readJsonPathValuesFromAvroAndWriteInTsv(DataFile theFile, List<String> theJsonPaths, String theTable, File theTsvFile) {
        InputStream anIn = null;
        AvroRecordReader aReader = null;
        List<InsertValues> someValues = new ArrayList<>();

        try {
            anIn = myDataFileHandler.getInputStream(theFile);
            aReader = new AvroRecordReader(anIn);

            for (List<Object> aRow : aReader.readValues(theJsonPaths)) {
                someValues.add(new InsertValues(aRow));

                if (someValues.size() >= TSV_FILE_WRITE_CHUNK_LINES) {
                    RdsDao.getInstance().writeValuesAsTsv(theTsvFile, theTable, someValues);
                    someValues.clear();
                }
            }

            if (someValues.size() > 0) {
                RdsDao.getInstance().writeValuesAsTsv(theTsvFile, theTable, someValues);
                someValues.clear();
            }

        } catch (IOException e) {
            throw new RuntimeException("Unable to extract json paths from avro: " + theFile, e);

        } finally {
            IOUtils.closeQuietly(aReader);
            IOUtils.closeQuietly(anIn);
        }
    }
}
