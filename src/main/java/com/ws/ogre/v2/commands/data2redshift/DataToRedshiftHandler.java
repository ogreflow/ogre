package com.ws.ogre.v2.commands.data2redshift;

import com.google.gson.Gson;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2redshift.db.ColumnMappingDao;
import com.ws.ogre.v2.commands.data2redshift.db.DbHandler;
import com.ws.ogre.v2.commands.data2redshift.db.ImportLogDao;
import com.ws.ogre.v2.commands.data2redshift.db.RedShiftDao;
import com.ws.ogre.v2.commands.data2redshift.db.RedShiftDao.Format;
import com.ws.ogre.v2.data2dbcommon.db.ImportLog;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFile;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesById;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesByType;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateHour.Range;
import com.ws.ogre.v2.datetime.DateHour.Ranges;
import com.ws.ogre.v2.utils.SleepUtil;
import com.ws.ogre.v2.utils.StopWatch;

import java.util.*;

/**
 * Handler for importing data into Redshift.
 */
public class DataToRedshiftHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private static DbHandler myDbHandler = DbHandler.getInstance();
    private static ImportLogDao ourImportLogDao = ImportLogDao.getInstance();
    private static RedShiftDao ourRedShiftDao = RedShiftDao.getInstance();

    private S3Client myS3Client;
    private SchemaHandler mySchemaHandler;
    private PartitionHandler myPartitionHandler;
//    private AnalyseHandler myAnalyseHandler;
    private DataFileHandler myDataFileHandler;
    private ImportedHandler myImportedHandler;

    private S3Url myTmpDir;
    private S3Url myManifestDir;
    private S3Url myMappingsDir;

    private Set<String> myTypes;
    private Set<String> myRequestedTypes;

    // Import states (to resume upon failures)
    private Set<String> myImportedChunks = new HashSet<>();
    private Set<String> myImportedTypes = new HashSet<>();

    private long myLastImportLogCleanupTime = System.currentTimeMillis();


    public DataToRedshiftHandler(Config theConfig, Set<String> theTypes) {

        myRequestedTypes = theTypes;

        myS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
        myDataFileHandler = new DataFileHandler(myS3Client, theConfig.srcRootDir);
        myImportedHandler = new ImportedHandler(theConfig.srcRootDir.bucket);

        myDbHandler.init(theConfig.dstHost, theConfig.dstPort, theConfig.dstDb, theConfig.dstSchema, theConfig.dstUser, theConfig.dstPwd);
        ourRedShiftDao.init(theConfig.srcAccessKey, theConfig.srcSecret, theConfig.dstSchema);

        // Create a unique tmpdir
        myTmpDir = new S3Url(theConfig.srcTmpDir, "" + new Random().nextLong());

        // Create s3 dirs for manifests and mappings under temp dir
        myManifestDir = new S3Url(myTmpDir, "manifest");
        myMappingsDir = new S3Url(myTmpDir, "mappings");


        // Handler managing table partitioning
        myPartitionHandler = new PartitionHandler(theConfig.partitionings);

        // Handler fo analysing tables regulary
//        myAnalyseHandler = new AnalyseHandler(myPartitionHandler);

        // Create handler for live schema changes
        mySchemaHandler = new SchemaHandler(theConfig.srcDdlDir, myMappingsDir, theConfig.srcAccessKey, theConfig.srcSecret, myPartitionHandler);

        // Sync new DDLs if any changes, returns the available types.
        myTypes = syncDdls(true, null, theTypes);
    }

    public void close() {

        // Remove temp manifest dir
        myS3Client.deleteObjects(myTmpDir);

        // Stops the timer daemon.
//        myAnalyseHandler.stopAnalyse();

        // Must close the DB connection. Otherwise the main thread doesn't exit.
        myDbHandler.close();
    }

    public void scanAndLoad(int theScanIntervalS, int theLookbackUnits, Range.Chunking theChunking, boolean theReplaceAllWithLatest) {

        ourLogger.info("Scan and import new data files into redshift every %s second and with a lookback of %s, %s", theScanIntervalS, theLookbackUnits, theChunking);

        while (true) {
            StopWatch aWatch = new StopWatch();

            long aNextScan = System.currentTimeMillis() + theScanIntervalS * 1000;

            DateHour aFrom = DateHour.getChunkStart(new Date().getTime(), theLookbackUnits, theChunking);
            DateHour aTo = DateHour.getChunkEnd(new Date().getTime(), theChunking);

            if (theReplaceAllWithLatest) {
                replaceAllWithLatest(aFrom, aTo, 30);
            } else {
                loadWithRetry(aFrom, aTo, theChunking, false, 30);
            }

            ourLogger.info("Scan done (%s)", aWatch);

            if (theScanIntervalS < 0) {
                break;
            }

            ourLogger.info("Wait for next scan...");

            SleepUtil.sleep(aNextScan - System.currentTimeMillis());
        }
    }

    public void loadWithRetry(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReload, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {

                load(theFrom, theTo, theChunking, theReload);

                ourLogger.info("Loaded %s - %s", theFrom, theTo);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully loaded files after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alert.getAlert().alert("Failed to load files, will retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                } else {
                    ourLogger.warn("Failed to load files, will retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                }

                SleepUtil.sleepForRetry();
            }
        }

        throw new RuntimeException("Failed to load period " + theFrom + "-" + theTo + ".");
    }

    public void replaceAllWithLatest(DateHour theFrom, DateHour theTo, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {
                replaceAllWithLatest(theFrom, theTo);

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

    private void replaceAllWithLatest(DateHour theFrom, DateHour theTo) {

        ourLogger.info("Check for new files for period %s to %s and replace old with newest if any found", theFrom, theTo);
        DataFilesByType aNewFiles;

        try {
            myDbHandler.beginTransaction();

            // Find all new files not imported
            aNewFiles = getNewFiles(theFrom, theTo);

            myDbHandler.commitTransaction();

        } catch (Exception e) {
            myDbHandler.rollbackTransaction();
            throw e;
        }

        if (aNewFiles.isEmpty()) {
            return;
        }

        // Sync new DDLs if any changes (in own transaction)
        myTypes = syncDdls(false, myTypes, myRequestedTypes);

        // Manage table partitions for period (in own transaction)
        partitionTables(theFrom, theTo);

        // Remove old obsolete import logs
        cleanupImportLog();

        for (String aType : aNewFiles.getTypes()) {

            if (myImportedTypes.contains(aType)) {
                continue;
            }

            try {
                myDbHandler.beginTransaction();

                deleteAll(aType);

                DataFiles aFiles = aNewFiles.getForType(aType);

                // Get latest/newest
                aFiles.sortDesc();

                DataFiles aToImport = new DataFiles();
                aToImport.add(aFiles.get(0));

                copyIntoRedShift(aToImport, aFiles);

                myDbHandler.commitTransaction();

            } catch (Exception e) {
                myDbHandler.rollbackTransaction();
                throw e;
            }

            myImportedTypes.add(aType);
        }

        myImportedTypes.clear();
    }

    public void delete(DateHour theFrom, DateHour theTo) {

        try {

            myDbHandler.beginTransaction();

            ourLogger.info("Delete data from  %s - %s", theFrom, theTo);

            for (String aType : myTypes) {
                deleteExisting(aType, theFrom, theTo);
            }

            myDbHandler.commitTransaction();

        } catch (Exception e) {
            myDbHandler.rollbackTransaction();
            throw e;
        }
    }

    public void recreateViews() {

        try {

            myDbHandler.beginTransaction();

            ourLogger.info("Recreate partition views for types %s", myTypes);

            for (String aType : myTypes) {
                if (myPartitionHandler.isPartitioned(aType)) {
                    myPartitionHandler.recreatePartitionView(aType);
                }
            }

            myDbHandler.commitTransaction();

        } catch (Exception e) {
            myDbHandler.rollbackTransaction();
            throw e;
        }
    }

    private void load(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReload) {

        ourLogger.info("Load data for %s - %s: chunked=%s, reload=%s", theFrom, theTo, theChunking, theReload);

        // Sync new DDLs if any changes (in own transaction)
        myTypes = syncDdls(false, myTypes, myRequestedTypes);

        // Manage table partitions for period (in own transaction)
        partitionTables(theFrom, theTo);

        // Remove old obsolete import logs
        cleanupImportLog();

        // Load data in chunks by type
        loadChunked(theFrom, theTo, theChunking, theReload);
    }

    private void loadChunked(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, boolean theReload) {

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

                loadInTransaction(aType, aChunk.getFrom(), aChunk.getTo(), theReload);

                myImportedChunks.add(aChunkKey);
            }
        }

        myImportedChunks.clear();
    }

    private void loadInTransaction(String theType, DateHour theFrom, DateHour theTo, boolean theReload) {

        try {
            myDbHandler.beginTransaction();

            loadPeriod(theType, theFrom, theTo, theReload);

            myDbHandler.commitTransaction();

        } catch (Exception e) {
            myDbHandler.rollbackTransaction();
            throw e;
        }
    }

    private void loadPeriod(String theType, DateHour theFrom, DateHour theTo, boolean theReload) {

        ourLogger.info("Load '%s' for %s - %s", theType, theFrom, theTo);

        // Delete old data if it should be replaced
        if (theReload) {
            deleteExisting(theType, theFrom, theTo);
        }

        // Resolves data files to be imported
        DataFiles aToImport = getNewFiles(theType, theFrom, theTo);

        // Import files
        copyIntoRedShift(aToImport);

        // Register the imported files into the analyser manager
        for (DateHour aHour : aToImport.getHours()) {
//            myAnalyseHandler.loaded(theType, aHour);
        }
    }

    private DataFilesByType getNewFiles(DateHour theFrom, DateHour theTo) {
        DataFiles aFiles = new DataFiles();

        for (String aType : myTypes) {
            aFiles.addAll(getNewFiles(aType, theFrom, theTo));
        }

        return new DataFilesByType(aFiles);
    }

    private DataFiles getNewFiles(String theType, DateHour theFrom, DateHour theTo) {

        // Get S3 data files within the range
        DataFiles aFiles = myDataFileHandler.findFilesByTimeRange(theFrom, theTo, Collections.singleton(theType));

        // Get imported files within the range
        DataFiles someImported = myImportedHandler.getImportedFiles(theFrom, theTo, theType);

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

        ourLogger.info("Found %s files for %s to import", aToImport.size(), theType);

        return aToImport;
    }


    private void copyIntoRedShift(DataFiles theToImport) {
        copyIntoRedShift(theToImport, theToImport);
    }

    private void copyIntoRedShift(DataFiles theToImport, DataFiles theMarkImported) {

        TableFiles aPartitionTableFiles = new TableFiles();

        // Resolve the files to import by table
        for (DataFile aFile : theToImport) {

            String aTable = myPartitionHandler.getPartitionTable(aFile.type, new DateHour(aFile.timestamp));

            aPartitionTableFiles.put(aTable, aFile);
        }

        // Import data per table
        for (String aPartitionTable : aPartitionTableFiles.getTables()) {

            String aType = aPartitionTableFiles.getType(aPartitionTable);
            DataFiles aToImport = aPartitionTableFiles.getFiles(aPartitionTable);

            // Import files in chronological order
            aToImport.sortAsc();

            // Create a manifest file with the data files to import in copy
            S3Url aManifest = generateManifest(aType, aToImport);

            // Point out the mappings file for which data to map to which db column
            S3Url aMappings = new S3Url(myMappingsDir, aType + ".json");

            Format aFormat = aToImport.get(0).isAvroFile() ? Format.AVRO : Format.JSON;

            StopWatch aWatch = new StopWatch();

            ourLogger.info("COPY %s data into DB. (Table: %s, Manifest: %s, Files: %s)", aFormat, aPartitionTable, aManifest, aToImport.size());

            ourRedShiftDao.copy(aPartitionTable, aManifest, aMappings, aFormat);

            ourLogger.info("COPY done (%s)", aWatch);

            deleteManifest(aManifest);
        }

        // Log imported rows
        List<ImportLog> aLogs = new ArrayList<>();
        for (DataFile aFile : theMarkImported) {
            ImportLog aLog = new ImportLog();
            aLog.filename = aFile.url.toString();
            aLog.tablename = aFile.type;
            aLog.timestamp = aFile.timestamp;
            aLogs.add(aLog);
        }

        ourImportLogDao.persist(aLogs, RedShiftDao.REDSHIFT_DATE_FORMAT);
    }

    private Set<String> syncDdls(boolean theInitiate, Set<String> theOldTypes, Set<String> theRequestedTypes) {
        try {

            DbHandler.getInstance().beginTransaction();

            Set<String> aTypes = theOldTypes;

            // Import new DDLs if any
            boolean isChanged = mySchemaHandler.syncDdls(theInitiate);

            // Do we need to resolve the types to work with
            if (isChanged || theInitiate) {
                aTypes = getTypes(theRequestedTypes);
            }

            DbHandler.getInstance().commitTransaction();

            return aTypes;

        } catch (RuntimeException e) {
            DbHandler.getInstance().rollbackTransaction();
            throw e;
        }
    }

    private void partitionTables(DateHour theFrom, DateHour theTo) {
        try {

            DbHandler.getInstance().beginTransaction();

            myPartitionHandler.partitionTable(myTypes, theFrom, theTo);

            DbHandler.getInstance().commitTransaction();

        } catch (RuntimeException e) {
            DbHandler.getInstance().rollbackTransaction();
            throw e;
        }
    }

    private void cleanupImportLog() {

        if (System.currentTimeMillis() > myLastImportLogCleanupTime + 24 * 60 * 60 * 1000l) {

            myLastImportLogCleanupTime = System.currentTimeMillis();

            for (String aType : myTypes) {

                DateHour aStart = myPartitionHandler.getStart(aType);

                if (aStart == null) {
                    continue;
                }

                ourLogger.info("Remove old import logs before: " + aStart);

                try {

                    myDbHandler.beginTransaction();

                    int aDeleted = ourImportLogDao.deleteByTimeRange(aType, new DateHour("2000-01-01:00"), aStart);

                    ourLogger.info("Removed %s old obsolete import logs", aDeleted);

                    myDbHandler.commitTransaction();

                } catch (Exception e) {
                    Alert.getAlert().alert("Failed to remove old import logs for " + aType + ", older than " + aStart, e);
                    myDbHandler.rollbackTransaction();
                }
            }
        }

    }


    private S3Url generateManifest(String theType, DataFiles theFiles) {

        S3Url anUrl = new S3Url(myManifestDir, theType + "-" + System.currentTimeMillis() + ".json");

        ourLogger.info("Create manifest for %s files: %s", theFiles.size(), anUrl);

        CopyManifest aManifest = new CopyManifest();

        for (DataFile aFile : theFiles) {

            ourLogger.info("Manifest - Add: %s", aFile.url);

            CopyManifest.Entry anEntry = new CopyManifest.Entry();

            anEntry.url = aFile.url.toString();
            anEntry.mandatory = true;

            aManifest.entries.add(anEntry);
        }

        String aJson = new Gson().toJson(aManifest);

        myS3Client.putObject(anUrl.bucket, anUrl.key, aJson);

        return anUrl;
    }

    private void deleteManifest(S3Url theManifestUrl) {
        try {
            myS3Client.deleteObjects(theManifestUrl);
        } catch (Exception e) {
            ourLogger.warn("Failed to delete manifest file: %s", theManifestUrl);
        }
    }

    private void deleteExisting(String theType, DateHour theFrom, DateHour theTo) {

        // Delete ALL data in table for type if no "timestamp" column exists, else just delete the data for period.

        if (!ourRedShiftDao.hasTimestampColumn(theType)) {

            ourLogger.info("No timestamp column for type '%s', delete all current rows", theType);

            ourRedShiftDao.deleteAll(theType);

            ourImportLogDao.deleteAllByType(theType);

        } else {

            ourLogger.info("Deleting rows in db table %s for period %s to %s", theType, theFrom, theTo);

            Set<String> allExistingPartitions = myPartitionHandler.getPartitionTables(theType);
            Set<String> allAffectedPartitions = myPartitionHandler.getPartitionTables(theType, theFrom, theTo);

            allAffectedPartitions.retainAll(allExistingPartitions);

            for (String aPartition : allAffectedPartitions) {
                ourRedShiftDao.deleteByTimeRange(aPartition, theFrom, theTo);
            }

            ourImportLogDao.deleteByTimeRange(theType, theFrom, theTo);
        }
    }

    private void deleteAll(String theType) {

        ourLogger.info("Delete all rows for type: %s", theType);

        for (String aTable : myPartitionHandler.getPartitionTables(theType)) {
            ourLogger.info("Delete all rows for table: %s", aTable);
            ourRedShiftDao.deleteAll(aTable);
        }
    }

    private Set<String> getTypes(Set<String> theRequestedTypes) {

        ourLogger.info("Fetch all registered types/tables...");

        // Get all tables/types imported
        Set<String> allTypes = ColumnMappingDao.getInstance().getTables();


        // Should we process all known types ?
        if (theRequestedTypes == null) {
            return allTypes;
        }

        // Validate that we do not have any misspelled or missing requested types
        for (String aType : theRequestedTypes) {
            if (!allTypes.contains(aType)) {
                throw new IllegalArgumentException("Type: " + aType + ", no table in DB exists to import it, must be one of: " + allTypes);
            }
        }

        return theRequestedTypes;
    }


    private class TableFiles {

        private Map<String, DataFiles> myTableFiles = new HashMap<>();

        private Map<String, String> myTableTypes = new HashMap<>();


        public Set<String> getTables() {
            return myTableFiles.keySet();
        }

        public String getType(String theTable) {
            return myTableTypes.get(theTable);
        }

        public void put(String theTable, DataFile theFile) {

            String aSameType = myTableTypes.put(theTable, theFile.type);

            if (aSameType != null && !aSameType.equals(theFile.type)) {
                throw new RuntimeException("Unexpected error");
            }

            DataFiles aFiles = myTableFiles.get(theTable);

            if (aFiles == null) {
                aFiles = new DataFiles();
                myTableFiles.put(theTable, aFiles);
            }

            aFiles.add(theFile);
        }

        public DataFiles getFiles(String theTable) {
            return myTableFiles.get(theTable);
        }
    }

}
