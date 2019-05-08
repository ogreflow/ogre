package com.ws.common.logging;

/**
 * This is the interface for generating alerts (events that might trigger alarms) in the system.
 */
public class Alert {


    private Logger myLog;


    protected Alert() {
        myLog =  Logger.getLogger(Thread.currentThread().getStackTrace()[3].getClassName());
    }

    protected Alert(String theClassName) {
        myLog =  Logger.getLogger(theClassName);
    }

    protected Alert(Logger theLogger) {
        myLog =  theLogger;
    }

    public static Alert getAlert() {
        return new Alert();
    }

    /**
     * For getting an alert object connected to an arbitrary logger.
     *
     * Use getAlert() for automatic detection of calling class'es logger.
     */
    public static Alert getAlert(Logger theLogger) {
        return new Alert(theLogger);
    }

    /**
     * @param theMessage The message to log out
     * @param theArgs note: any last Throwable in the argument list will be treated as Throwable and logged with stacktrace
     */
    public void alert(String theMessage, Object... theArgs) {
        myLog.error(theMessage, theArgs);

    }

    public void alert(String theMessage, Throwable theThrowable) {
        myLog.error(theMessage, theThrowable);
    }

    public void alert(String theMessage) {
        myLog.error(theMessage);
    }



}
