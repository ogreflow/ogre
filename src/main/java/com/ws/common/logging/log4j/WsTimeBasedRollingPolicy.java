package com.ws.common.logging.log4j;

import org.apache.log4j.rolling.RolloverDescription;
import org.apache.log4j.rolling.RolloverDescriptionImpl;
import org.apache.log4j.rolling.helper.Action;
import org.apache.log4j.rolling.helper.ActionBase;
import org.apache.log4j.rolling.helper.GZCompressAction;
import org.apache.log4j.rolling.helper.ZipCompressAction;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Same as org.apache.log4j.rolling.TimeBasedRollingPolicy, but we added the ability to delete old backup files.
 * Use MaxBackupIndex to control the max number of backup files to keep.
 * Use compressWaitFileCount  to control the nr of files to keep uncompressed in the active dir, before compressiong and "archiving" it.
 *
 * Other params:
 * NumberOfBackupFilenamesToCheckForRegularCleanup       (int, default 24)
 * NumberOfExtraBackupFilenamesToCheckForFirstCleanup    (int, default 365)
 *
 *
 * Inspired by:
 * http://www.boyunjian.com/javasrc/com.butor/butor-utils/0.0.2/_/org/butor/log4j/TimeBasedRollingPolicyFromApacheLog4jExtras.java
 */
public class WsTimeBasedRollingPolicy extends NonFinalTimeBasedRollingPolicy {

    private static final long MAX_POWER_OF_TWO = 1L << (Long.SIZE - 2);

    private Long timeDeltaForFilenameChange;

    private int maxCompressAndMoveWaitIndex = 0;

    private int maxBackupIndex = 0;

    private int numberOfBackupFilenamesToCheckForRegularCleanup = 24;

    private int numberOfExtraBackupFilenamesToCheckForFirstCleanup = 365;

    private int asyncThreadLowerPrioSteps = 1;

    private boolean firstCleanupRan = false;

    public void setMaxBackupIndex(int maxBackupIndex) {
        this.maxBackupIndex = maxBackupIndex;
    }

    public void setNumberOfBackupFilenamesToCheckForRegularCleanup(int numberOfBackupFilenamesToCheckForRegularCleanup) {
        this.numberOfBackupFilenamesToCheckForRegularCleanup = numberOfBackupFilenamesToCheckForRegularCleanup;
    }

    public void setNumberOfExtraBackupFilenamesToCheckForFirstCleanup(int numberOfExtraBackupFilenamesToCheckForFirstCleanup) {
        this.numberOfExtraBackupFilenamesToCheckForFirstCleanup = numberOfExtraBackupFilenamesToCheckForFirstCleanup;
    }

    public void setMaxCompressAndMoveWaitIndex(int compressWaitTimeS) {
        this.maxCompressAndMoveWaitIndex = compressWaitTimeS;
    }

    public void setAsyncThreadLowerPrioSteps(int theAsyncThreadLowerPrioSteps) {
        asyncThreadLowerPrioSteps = theAsyncThreadLowerPrioSteps;
    }

    @Override
    public RolloverDescription rollover(String currentActiveFile) {

        RolloverDescription rolloverDescription = super.rollover(currentActiveFile);

        if (rolloverDescription == null) {
            // We'll wait until next rollover before deleting anything
            return null;
        }

        //We get a hold of our own WsFileRenameAction object, to affect the sync renaming.
        final WsFileRenameAction aWsFileRenameAction = (WsFileRenameAction) rolloverDescription.getSynchronous();
        String aSourceFileDirectory = aWsFileRenameAction.source.getParent();

        if( maxCompressAndMoveWaitIndex > 0 ) {
            //Ensure the rename-action keeps the renamed file in the same directory as the source file,
            aWsFileRenameAction.destination = new File(aSourceFileDirectory + File.separatorChar + aWsFileRenameAction.destination.getName());
        }

        //We save a timestamp of when the rollover was initiated (needed for the wait-time)
        final long aRolloverTimestamp = System.currentTimeMillis();

        if (maxBackupIndex <= 0 && maxCompressAndMoveWaitIndex <= 0 ) {
            // This means cleanup is deactivated
            return rolloverDescription;
        }

        final Action asynchronous = rolloverDescription.getAsynchronous();

        Action newAsynchronous = new ActionBase() {

            @Override
            public boolean execute() throws IOException {

                //To not disturb the normal threads too much, we lower the priority a bit
                Thread.currentThread().setPriority(Thread.NORM_PRIORITY - asyncThreadLowerPrioSteps );
                System.out.println("  Thread.currentThread().prio()  = " +   Thread.currentThread().getPriority());

                boolean success = true;

                //The nr of files to search for
                int searchFileCount = maxBackupIndex + numberOfBackupFilenamesToCheckForRegularCleanup;

                if (!firstCleanupRan) {
                    //The first time we search for extra many files back.
                    searchFileCount += numberOfExtraBackupFilenamesToCheckForFirstCleanup;
                }

                //Generate all the file names to search for
                List<String> someOlderFilenames = getBackupFiles(searchFileCount, aRolloverTimestamp);

                //Find the uncompressed ones we need to compress first
                if (asynchronous != null ) {

                    //If we should leave some files in place uncompressed, but renamed - provide an alternate source dir
                    String anAlternateSourceDir = ( maxCompressAndMoveWaitIndex > 0 ? aWsFileRenameAction.source.getParent() : null);

                    //Create and do compression of the found files
                    List<Action> someCompressActions = getCompressActions(someOlderFilenames, anAlternateSourceDir);
                    for( Action aCompressAction: someCompressActions ) {
                        success &= aCompressAction.execute();
                    }
                }

                //Delete old files
                Iterator<String> aBackupFilesIterator = someOlderFilenames.iterator();

                //If the nr of old files to check for is less than the max nr to keep - simply skip the deletetion.
                if( someOlderFilenames.size() > maxBackupIndex ) {

                    //Skip the first 'maxBackupIndex' files
                    for( int i = 0; i <= maxBackupIndex; i++ ) {
                        aBackupFilesIterator.next();
                    }

                    while( aBackupFilesIterator.hasNext() ) {

                        String backupFileToDelete = aBackupFilesIterator.next();

                        //Here the actual compressed file is removed
                        success &= deleteFileWithoutSuffix(backupFileToDelete, "");

                        //Here the any uncompressed of the file is removed
                        switch( suffixLength ) {
                            // This is not very nice, but it was also not very nice in org.apache.log4j.raBackupFilesIterator.next()olling.TimeBasedRollingPolicy

                            case 0: //No compression
                                success &= deleteFileWithoutSuffix(backupFileToDelete, "");
                                break;

                            case 3: //.gz
                                success &= deleteFileWithoutSuffix(backupFileToDelete, ".gz");
                                break;

                            case 4: //.zip
                                success &= deleteFileWithoutSuffix(backupFileToDelete, ".zip");
                                break;
                        }
                    }
                }

                firstCleanupRan = true;

                return success;
            }

            private boolean deleteFileWithoutSuffix(String filename, String suffix) {
                if (filename.endsWith(suffix)) {
                    File file = new File(filename.substring(0, filename.length() - suffix.length()));
                    if (file.exists()) {
//                        System.out.println("Deleted file: "+ file.getName() + ", suffix:  "+ suffix +" at: "+ new Date());
                        return file.delete();
                    }
                }
                return true;
            }
        };

        return new RolloverDescriptionImpl(
                rolloverDescription.getActiveFileName(),
                rolloverDescription.getAppend(),
                rolloverDescription.getSynchronous(),
                newAsynchronous);

    }

    private List<Action> getCompressActions(List<String> theOlderFilenames, String theAlternateSourceDirectory ) {

        List<Action> aActionList = new ArrayList<>();

//        int aMaxFileCount = (maxBackupIndex > 0 ? maxBackupIndex : theOlderFilenames.size() );
        int aMaxFileCount = theOlderFilenames.size();

        for( int i = 0; i < aMaxFileCount; i++ ) {

            if(  i > maxCompressAndMoveWaitIndex ) {

                File anUncompressedFileName = new File(theOlderFilenames.get(i).substring(0, theOlderFilenames.get(i).length() - suffixLength));
                if( theAlternateSourceDirectory != null ) {
                    anUncompressedFileName = new File(theAlternateSourceDirectory + File.separatorChar + anUncompressedFileName.getName());
                }

                File aCompressedFileName = new File(theOlderFilenames.get(i));

                if( anUncompressedFileName.exists() && !aCompressedFileName.exists()) {
                    aActionList.add(  suffixLength == 3 ?
                            new GZCompressAction(
                                    anUncompressedFileName, aCompressedFileName, true)
                            :
                            new ZipCompressAction(
                                    anUncompressedFileName, aCompressedFileName, true) );

//                    System.out.println("Filename to be compressed async: " + anUncompressedFileName + " -> "+ aCompressedFileName);
                }
            }
        }

        return aActionList;
    }

    private List<String> getBackupFiles(int searchFileCount, long theFromTimestamp) {

        // The time delta could vary according to the current time.
        // For instance, if we have a formatting with month precision ('yyyy-MM'), some months are longer than other.
        // We would not want to miss a shorther month (February), because the delta was calculated on the month of January.
        // So dividing the delta by 2 should be more than enough to prevent that
        long timeIncrement = -getTimeDeltaForFilenameChange() / 2;

        if (timeIncrement == 0) {
            return Collections.emptyList();
        }

        List<String> backupFiles = new ArrayList<>();
        for (String filename : getNextFilenames(searchFileCount, theFromTimestamp, timeIncrement)) {
            backupFiles.add(filename);
        }

        return backupFiles;
    }

    private Iterable<String> getNextFilenames(int filenameCount, long fromTime, long timeIncrement) {
        Set<String> nextFilenames = new LinkedHashSet<>();
        for (long time = fromTime; nextFilenames.size() < filenameCount; time += timeIncrement) {
            nextFilenames.add(getFilename(time));
        }
        return nextFilenames;
    }

    private long getTimeDeltaForFilenameChange() {
        if (timeDeltaForFilenameChange == null) {
            long nextTimeThatFilenameChange = findNextTimeThatFilenameChange(System.currentTimeMillis());
            long nextTimeThatFilenameChangeAgain = findNextTimeThatFilenameChange(nextTimeThatFilenameChange);
            long timeDelta = nextTimeThatFilenameChangeAgain - nextTimeThatFilenameChange;
            timeDeltaForFilenameChange = Long.valueOf(timeDelta);
        }
        return timeDeltaForFilenameChange.longValue();
    }

    private long findNextTimeThatFilenameChange(long baseTime) {
        String baseFilename = getFilename(baseTime);
        long lowerBound = baseTime;
        long delta = 1;
        long upperBound = baseTime + delta;
        while (delta < MAX_POWER_OF_TWO && getFilename(upperBound).equals(baseFilename)) {
            lowerBound = upperBound;
            delta <<= 1;
            upperBound = baseTime + delta;
        }

        if (!(delta < MAX_POWER_OF_TWO)) {
            // The filename never changes!!
            return baseTime;
        }

        while (upperBound - lowerBound > 1) {
            long newBound = (lowerBound + upperBound) / 2;
            if (getFilename(newBound).equals(baseFilename)) {
                lowerBound = newBound;
            } else {
                upperBound = newBound;
            }
        }
        return upperBound;
    }

    private String getFilename(long n) {
        StringBuffer buf = new StringBuffer();
        formatFileName(new Date(n), buf);
        return buf.toString();
    }
}
