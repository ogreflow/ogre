package com.ws.ogre.v2.commands.db2avro;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.db.DependencySqlFailedException;
import com.ws.ogre.v2.utils.CommandSyncer;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DbToAvroSyncHandler extends DbToAvroHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private static final int MAX_TIME_FOR_FAILED_CHUNK_FIX_MS = 24 * 60 * 60 * 1000 /* 1 day */;

    private Map<Integer /* Chunk id (hash code) */, ChunkSyncStatus> mySyncFailedChunk = new HashMap<>();

    public DbToAvroSyncHandler(Config theConfig, Set<String> theCliTypes, Map<String, String> theVariables) {
        super(theConfig, theCliTypes, theVariables);
    }

    public void scanAndDump(int theScanIntervalS, int theLookbackUnits, int theSyncEligibleDiffHour, DateHour.Range.Chunking theChunking) {
        CommandSyncer.sync(
                theScanIntervalS,
                theLookbackUnits,
                theChunking,
                theRangeToRun -> scanAndDump(theRangeToRun.getChunkedRanges(theChunking), theSyncEligibleDiffHour)
        );
    }

    private void scanAndDump(DateHour.Ranges theUnitChunks, int theSyncEligibleDiffHour) {
        long theSyncEligibleDiffMs = theSyncEligibleDiffHour * 60 * 60 * 1000L;

        // Load data chunk by chunks
        for (DateHour.Range aChunk : theUnitChunks) {
            try {
                initForDoingQueryForChunk(aChunk);

                if (aChunk.getEndTimeDiffFromCurrentTime() < theSyncEligibleDiffMs) {
                    dumpTimeRange(aChunk,
                            false /* Sync should never replace. If any problem, do the "load" command */,
                            true /* For recent running range (hour / day), no need to check dependency. */,
                            true /* This time, the file is a partial as the range (hour / day) is not completed yet. */
                    );
                } else {
                    dumpTimeRange(aChunk,
                            false /* Sync should never replace. If any problem, do the "load" command */,
                            false /* Must check dependency. */,
                            false /* This time, its not a sync file. We expect it to load fully. */
                    );
                }

                chunkSucceeded(aChunk);

            } catch (DependencySqlFailedException e) {
                chunkFailed(aChunk);
                ourLogger.warn("[ Chunk %s ] Dependency failed. Don't worry. Will try soon.", aChunk, e);

            } catch (Exception e) {
                chunkFailed(aChunk);
                ourLogger.warn("[ Chunk %s ] Failed to dump for data source problem. Don't worry. Will try soon.", aChunk, e);
            }
        }

        // If failed chunk were not recovered withing 1 day, then its a problem.
        mySyncFailedChunk.entrySet().stream()
                .filter(anEntry -> anEntry.getValue().myChunk.getEndTimeDiffFromCurrentTime() >= MAX_TIME_FOR_FAILED_CHUNK_FIX_MS)
                .forEach(anEntry -> Alert.getAlert().alert("[ Chunk %s/%s ] is in black hole.", myTypes, anEntry.getValue()));
    }

    private void chunkFailed(DateHour.Range theChunk) {
        ChunkSyncStatus aStatus = mySyncFailedChunk.get(theChunk.hashCode());
        if (aStatus == null) {
            aStatus = new ChunkSyncStatus(theChunk, 0);
            mySyncFailedChunk.put(theChunk.hashCode(), aStatus);
        }

        aStatus.myFailCount++;
    }

    private void chunkSucceeded(DateHour.Range theChunk) {
        ChunkSyncStatus aStatus = mySyncFailedChunk.get(theChunk.hashCode());
        if (aStatus == null) {
            return; // No earlier failure.
        }

        if (aStatus.myFailCount > 5) {
            Alert.getAlert().alert("[ Chunk %s ] Successfully loaded after " + aStatus.myFailCount + " failure. Might be data unavailability issue in data source.", theChunk);
        }

        mySyncFailedChunk.remove(theChunk.hashCode());
    }

    private class ChunkSyncStatus {
        private DateHour.Range myChunk;
        private int myFailCount = 0;

        public ChunkSyncStatus(DateHour.Range theChunk, int theFailCount) {
            myChunk = theChunk;
            myFailCount = theFailCount;
        }

        @Override
        public String toString() {
            return "chunk=" + myChunk + ", failCount=" + myFailCount;
        }
    }
}
