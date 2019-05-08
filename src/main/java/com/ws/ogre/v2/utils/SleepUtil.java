package com.ws.ogre.v2.utils;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;

/**
 * Helper class for removing the ugliness of InterruptedException.
 */
public class SleepUtil {

    private static final Logger ourLogger = Logger.getLogger();

    public static final long MIN_SLEEP_TIME_IN_SCAN_MS = 60000L; /* 1min */
    public static final long SLEEP_TIME_IN_RETRY_MS = 300000L; /* 5min */

    public static void sleepInsideScan(long theMs) {
        if (theMs <= MIN_SLEEP_TIME_IN_SCAN_MS) {
            Alert.getAlert().alert("No breathing time in continuous scanning. Might introduce pressure on data stores.");
        }

        sleep(theMs);
    }

    public static void sleepForRetry() {
        sleep(SLEEP_TIME_IN_RETRY_MS);
    }

    public static void sleep(long theMs) {
        try {
            if (theMs < 0) {
                return;
            }

            ourLogger.info("Sleeping for %s ms...", theMs);
            Thread.sleep(theMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
