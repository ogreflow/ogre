package com.ws.ogre.v2.utils;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import org.apache.commons.io.IOUtils;

public class SystemUtils {
    private static final Logger ourLogger = Logger.getLogger();

    public static void runCommand(String theCommand) {
        try {
            StopWatch aWatch = new StopWatch();

            ourLogger.info("Execute system command: %s", theCommand);

            Process aProcess = Runtime.getRuntime().exec(theCommand);
            aProcess.waitFor();

            IOUtils.readLines(aProcess.getInputStream()).forEach(ourLogger::info);

            ourLogger.info("Done with executing system command. Took %s", aWatch);

        } catch (Exception e) {
            Alert.getAlert().alert("Unable to execute the system command: %s", theCommand, e);
        }
    }
}
