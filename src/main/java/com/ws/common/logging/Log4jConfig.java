package com.ws.common.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.FileWatchdog;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Handles scanning of log4j configuration and will redirect system out to configured logs.
 * This makes it possible to use one (log4j) log file for all logging.
 * Note, using console logger in log4j will still result in having all logs in JBoss logging, i.e. duplicating the logs.
 * You can avoid this by disable the "console" in log4j.
 */
public class Log4jConfig extends FileWatchdog {

    private static transient Logger ourLog ;

    public Log4jConfig(String theFilename, int theDelayMs) {
        super(theFilename);
        setName("Log4jFileWatcher");
        setDelay(theDelayMs);
    }

    protected void doOnChange() {

        //Only log if we have a confed and created logger
        if( ourLog != null ) {
            ourLog.info("Reload log4j configuration from file: %s", filename);
        }

        Logger.unredirectSystemOutToLog();

        new DOMConfigurator().doConfigure(filename, LogManager.getLoggerRepository());

        /* Set the LogLog.QuietMode. As of log4j1.2.8 this needs to be set to
         avoid deadlock on exception at the appender level. See jboss bug#696819. */
        LogLog.setQuietMode(true);

        //...and start it again.
        Logger.redirectSystemOutToLog();

        //we create the logger here, to not get warnings when the class is first created. (no log4j conf loaded yet)
        if( ourLog == null ) {
            ourLog = Logger.getLogger(Log4jConfig.class);
        }

        ourLog.info("Log4j configuration reloaded");
    }
}
