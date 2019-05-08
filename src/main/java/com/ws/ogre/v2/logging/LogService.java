package com.ws.ogre.v2.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Service used to initiate log4j.
 */
public class LogService {

    private LogService() {
    }

    public static void setupLogging(String theFilename) {
        if (theFilename != null) {
            DOMConfigurator.configureAndWatch(theFilename, 60000);
        }
    }

    public static void tearDown() {
        // Closes all the appenders specially our Sns appender.
        LogManager.shutdown();
    }
}
