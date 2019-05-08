package com.ws.common.logging;

import org.apache.log4j.MDC;

import java.util.UUID;

/**
 * Logging utility for connecting log rows with a common ID
 *
 */
public class LogTrace {

    public static final String LOGTRACEID = "logtraceid";

    public static void startTrace() {
        String anUUID = UUID.randomUUID().toString();
        String aLogTraceId = anUUID.substring(anUUID.lastIndexOf('-') + 1);
        continueTrace(aLogTraceId);
    }

    public static void continueTrace(String theLogTraceId) {
        if (theLogTraceId != null) {
            MDC.put(LOGTRACEID, theLogTraceId);
        } else {
            stopTrace();
        }
    }

    public static void stopTrace() {
        MDC.remove(LOGTRACEID);
    }

    public static String getCurrentLogTraceId() {
        return (String) MDC.get(LOGTRACEID);
    }

}
