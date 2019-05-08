package com.ws.ogre.v2.logging;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.SnsClient;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Paths;

/**
 */
public class Alerter {

    private static SnsClient ourSnsClient;
    private static String ourTopicArn;
    private static String ourPath;

    public static void init(String theAccessKeyId, String theSecretKey, String theTopicArn) {
        ourSnsClient = new SnsClient(theAccessKeyId, theSecretKey);
        ourTopicArn = theTopicArn;
        ourPath = Paths.get(".").toAbsolutePath().normalize().toString();
    }

    public static void alert(Throwable theThrowable, String theMessage) {

        String aSubject = "OGRE Alarm: " + ourPath;

        if (theThrowable != null) {
            theMessage += "\n\nError:\n" + getStackTrace(theThrowable);
        }

        Alert.getAlert(getLogger()).alert(theMessage);

        ourSnsClient.publish(ourTopicArn, aSubject, theMessage);
    }

    public static void close() {
        ourSnsClient.close();
    }

    private static String getStackTrace(Throwable theThrowable) {

        StringWriter aWriter = new StringWriter();

        theThrowable.printStackTrace(new PrintWriter(aWriter));

        return aWriter.toString();
    }

    private static Logger getLogger() {
        StackTraceElement[] anEls = Thread.currentThread().getStackTrace();

        String aLoggerName = "";

        for (int i = 1; i < anEls.length; i++) {
            StackTraceElement anEl = anEls[anEls.length - i];

            if (anEl.getClassName().equals(Alerter.class.getName()) || anEl.getMethodName().equals("getStackTrace")) {
                break;
            }

            aLoggerName = anEl.getClassName();
        }

        return Logger.getLogger(aLoggerName);
    }
}
