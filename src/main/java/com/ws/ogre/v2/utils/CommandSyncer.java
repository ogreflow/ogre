package com.ws.ogre.v2.utils;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.datetime.DateHour;

public class CommandSyncer {
    private static final Logger ourLogger = Logger.getLogger();

    public static void sync(int theScanIntervalS, int theLookbackUnits, DateHour.Range.Chunking theChunking, CommandSyncProcess theSyncProcess) {
        ourLogger.info("Scan and run at every %s second, with a lookback of %s, %s", theScanIntervalS, theLookbackUnits, theChunking);

        while (true) {
            StopWatch aWatch = new StopWatch();

            // Calculate next scan time before the dump execution.
            long aNextScan = System.currentTimeMillis() + theScanIntervalS * 1000;

            DateHour.Range aRangeToRun = DateHour.Range.getChunkRangeOfCurrentTime(theLookbackUnits, theChunking);
            ourLogger.info("Run sync for range: %s", aRangeToRun);
            theSyncProcess.run(aRangeToRun);

            ourLogger.info("Scan done (%s)", aWatch);

            if (theScanIntervalS <= 0) {
                break; /* Running once is used (a) in test case and (b) when sync is run from external scheduler */
            }

            ourLogger.info("Wait for next scan...");
            SleepUtil.sleepInsideScan(aNextScan - System.currentTimeMillis());
        }
    }

    public interface CommandSyncProcess {
        void run(DateHour.Range theRangeToRun);
    }
}
