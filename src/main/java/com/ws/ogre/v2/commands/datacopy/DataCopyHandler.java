package com.ws.ogre.v2.commands.datacopy;

import com.amazonaws.services.s3.model.StorageClass;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFile;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesById;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateHour.Range;
import com.ws.ogre.v2.datetime.DateHour.Ranges;
import com.ws.ogre.v2.utils.JobExecutorService;
import com.ws.ogre.v2.utils.SleepUtil;
import com.ws.ogre.v2.utils.StopWatch;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Class copying data file from 1 s3 location to another. This is specially for copying cross-region wise data.
 * A practical example is like this:
 * - From Ireland redshift, we can only UNLOAD to Ireland s3 bucket (aws limitation).
 * - For saving data into Asia s3 bucket, we need to copy data from the s3.
 * <p/>
 * But be aware, this operation uses processing machine's disk. So, take care for big data size.
 */
public class DataCopyHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private Config myConfig;

    private DataFileHandler mySrcDataFileHandler;
    private S3Client mySrcS3Client;

    private DataFileHandler myDstDataFileHandler;
    private S3Client myDstS3Client;
    private S3BetterUrl myDstRoot;
    private StorageClass myDstStorageClass;

    private Set<String> myTypes;

    // Import states (to resume upon failures)
    private Set<String> myImportedChunks = new HashSet<>();

    public DataCopyHandler(Config theConfig, Set<String> theCliTypes) {

        myConfig = theConfig;

        mySrcS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
        mySrcDataFileHandler = new DataFileHandler(mySrcS3Client, theConfig.srcRoot);

        myDstS3Client = new S3Client(theConfig.dstAccessKey, theConfig.dstSecret);
        myDstRoot = theConfig.dstRoot;
        myDstDataFileHandler = new DataFileHandler(myDstS3Client, theConfig.dstRoot);
        myDstStorageClass = theConfig.dstClass;

        myTypes = getTypesToWorkWith(theCliTypes, theConfig.types);
    }

    public void scanAndLoad(int theScanIntervalS, int theLookbackUnits, DateHour.Range.Chunking theChunking, int theThreads) {

        ourLogger.info("Scan and copy new files every %s second and with a lookback of %s, %s", theScanIntervalS, theLookbackUnits, theChunking);

        while (true) {
            StopWatch aWatch = new StopWatch();

            long aNextScan = System.currentTimeMillis() + theScanIntervalS * 1000;

            DateHour aFrom = DateHour.getChunkStart(new Date().getTime(), theLookbackUnits, theChunking);
            DateHour aTo = DateHour.getChunkEnd(new Date().getTime(), theChunking);

            // Scan for new files and copy them
            copyWithRetry(aFrom, aTo, theChunking, theThreads, false, 30);

            ourLogger.info("Scan done (%s)", aWatch);

            if (theScanIntervalS < 0) {
                break;
            }

            ourLogger.info("Wait for next scan...");

            SleepUtil.sleepInsideScan(aNextScan - System.currentTimeMillis());
        }
    }

    public void copyWithRetry(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, int theThreads, boolean theReplace, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {

                copyChunked(theFrom, theTo, theChunking, theThreads, theReplace);

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

    private void copyChunked(DateHour theFrom, DateHour theTo, DateHour.Range.Chunking theChunking, int theThreads, boolean theReplace) {

        ourLogger.info("Copy files for period %s - %s", theFrom, theTo);

        // Calc chunks
        Range aPeriod = new Range(theFrom, theTo);
        Ranges aChunks = aPeriod.getChunkedRanges(theChunking);

        // Load data chunk by chunks
        for (Range aChunk : aChunks) {
            String aChunkKey = aChunk.toString();

            // Skip if already imported this chunk (in case we had a failure and this is a retry)?
            if (myImportedChunks.contains(aChunkKey)) {
                ourLogger.info("Already imported %s, wind forward.", aChunk);
                continue;
            }

            copyPeriod(aChunk.getFrom(), aChunk.getTo(), theThreads, theReplace);

            myImportedChunks.add(aChunkKey);
        }

        myImportedChunks.clear();
    }

    private void copyPeriod(DateHour theFrom, DateHour theTo, int theThreads, boolean theReplace) {

        ourLogger.info("Load '%s' for %s - %s", myTypes, theFrom, theTo);

        // Delete (if told so) before finding out "new files to import".
        if (theReplace) {
            deleteFilesInDestination(myTypes, theFrom, theTo);
        }

        // Fetch for new file to copy
        DataFiles aFiles = getNewFilesToImport(myTypes, theFrom, theTo);
        if (aFiles.size() <= 0) {
            ourLogger.info("No '%s' data files found for %s - %s to import.", myTypes, theFrom, theTo);
            return;
        }

        // Copy them
        batchCopy(theThreads, aFiles);

        ourLogger.info("Batch copy done");
    }

    private DataFiles getNewFilesToImport(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        ourLogger.info("Check %s - %s for new files", theFrom, theTo);

        DataFiles aNew = new DataFiles();

        // Fetch destination files for period
        DataFiles aDstFiles = myDstDataFileHandler.findFilesByTimeRange(theFrom, theTo, theTypes);

        // Index them by id
        DataFilesById aDstFilesById = new DataFilesById(aDstFiles);

        // Fetch files for period
        DataFiles aFiles = mySrcDataFileHandler.findFilesByTimeRange(theFrom, theTo, theTypes);

        // Check which files that is new
        for (DataFile aFile : aFiles) {
            if (!aDstFilesById.contains(aFile)) {
                aNew.add(aFile);
            }
        }

        return aNew;
    }

    private void batchCopy(int theThreads, DataFiles theFiles) {

        // Sort files to copy so we do it in "chronological" kind of order
        theFiles.sortAsc();

        // Single threaded requested?
        if (theThreads == 1) {
            for (DataFile aFile : theFiles) {
                copy(aFile);
            }
            return;
        }

        // Spawn up a number of threads to share the load...

        JobExecutorService<DataFile> anExecutor = new JobExecutorService<>(theThreads);

        anExecutor.addTasks(theFiles);

        anExecutor.execute(new JobExecutorService.JobExecutor<DataFile>() {
            public void execute(DataFile theFile) throws Exception {
                copy(theFile);
            }
        });
    }

    private void copy(DataFile theFile) {
        File aSrcFile = mySrcDataFileHandler.downloadDataFile(theFile);
        aSrcFile.deleteOnExit(); // Safe guard to delete even though we will manually delete it later.

        S3BetterUrl aDest = DataFile.createUrl(myDstRoot, theFile.date, theFile.hour, theFile.type, theFile.name, theFile.ext);
        myDstDataFileHandler.uploadDataFile(aSrcFile, new DataFile(aDest), myDstStorageClass);

        ourLogger.info("Deleting temporary file: %s", aSrcFile.getAbsolutePath());

        if (!FileUtils.deleteQuietly(aSrcFile)) {
            Alert.getAlert().alert("Unable to delete temporary file: %s", aSrcFile.getAbsolutePath());
        }
    }

    private void deleteFilesInDestination(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        ourLogger.warn("Deleting files from %s to %s", theFrom, theTo);

        myDstDataFileHandler.deleteFilesByTimeRange(theFrom, theTo, theTypes);
    }

    private Set<String> getTypesToWorkWith(Set<String> theCliTypes, String[] theConfigTypes) {
        if (theCliTypes != null && !theCliTypes.isEmpty()) {
            return theCliTypes;
        }

        if (theConfigTypes == null || theConfigTypes.length == 0) {
            throw new IllegalArgumentException("No 'types' configured");
        }

        return new HashSet<>(Arrays.asList(theConfigTypes));
    }
}
