package com.ws.common.logging;

import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;

import java.nio.file.Files;
import java.nio.file.Paths;

public class LogConfigService {
    private static LogConfigService ourInstance = new LogConfigService();

    public static LogConfigService getInstance() {
        return ourInstance;
    }

    private String myWatchedConfigFile;

    private LogConfigService() {}

    public void startWatchingExternalConfigFile(String theFilename, long theRefreshPeriodMs) {

        //Only start watching it if it exits
        if( Files.exists(Paths.get(theFilename) )) {
            DOMConfigurator.configureAndWatch(theFilename, theRefreshPeriodMs);
            myWatchedConfigFile = theFilename;
        }
    }

    public void stopWatching() {

        //Only stop if we started, i.e. if the
        if( myWatchedConfigFile != null ) {
            LogManager.shutdown();
            myWatchedConfigFile = null;
        }
    }
}
