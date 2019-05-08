package com.ws.common.logging;


import org.apache.log4j.Level;
import org.apache.log4j.Priority;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Formatter;

/**
 * Widespace Logger API
 *
 * This class wraps a native logger for convenience of common logWithParameters behaviour,
 * possibility to change logWithParameters framework etc.
 *
 * @see Formatter for information about parameterized logging format
 */
public class Logger {

    private static final Logger ourLog = Logger.getLogger();
    // Thread local logger usage is off by default, in order to work with e.g. sl4j bridges
    protected static final String SYSTEM_PROPERTY_ENABLE_THREADLOCAL = "com.ws.common.logging.Logger.threadlocal.enabled";


    private org.apache.log4j.Logger myWrappedLogger;

    private static ThreadLocal<Formatter> ourThreadLocalFormatter = new ThreadLocalFormatter();
    private static ThreadLocal<ThreadLogger> ourThreadLocalLogger = new ThreadLocalLogger();

    /**
     * The name of this wrapper class
     * so the Log4j logger can parse ourSystemOut the correct calling
     * class name and line in source code info.
     */
    private static final String FQCN = Logger.class.getName();

    /** The previous value of System.out. */
    private static PrintStream out;

    /** The previous value of System.err. */
    private static PrintStream err;


    /**
     * Constructs a new logger.
     *
     * @return a new Logger instance.
     */
    public static Logger getLogger(Class theClass) {
        return new Logger(theClass.getName());
    }

    /**
     * Constructs a new logger.
     *
     * @return a new Logger instance.
     */
    public static Logger getLogger(String theName) {
        return new Logger(theName);
    }

    /**
     * Constructs a logger. The name of the Logger will be taken from the calling class.
     *
     * @return a new Logger instance.
     */
    public static Logger getLogger() {
        return new Logger(Thread.currentThread().getStackTrace()[2].getClassName());
    }

    /**
     * Constructs a logger
     *
     * @param theName name of the logger (usually the classname of the owning class)
     */
    private Logger(String theName) {
        myWrappedLogger = org.apache.log4j.Logger.getLogger(theName);
    }

    /**
     * Returns category for this logger.
     *
     * @return the category name
     */
    public String getName() {
        return myWrappedLogger.getName();
    }




    // Plain simple static message signatures
    public void trace(String theMessage) {
        logPlain(Level.TRACE, theMessage, null);
    }
    public void debug(String theMessage) {
        logPlain(Level.DEBUG, theMessage, null);
    }
    public void info(String theMessage) {
        logPlain(Level.INFO, theMessage, null);
    }
    /** Used for logging for which we have dependencies on the logWithParameters format and output */
    public void contractInfo(String theMessage) {
        logPlain(Level.INFO, theMessage, null);
    }
    public void warn(String theMessage) {
        logPlain(Level.WARN, theMessage, null);
    }
    protected void error(String theMessage) {
        logPlain(Level.ERROR, theMessage, null);
    }



    // The classic with printed stack trace
    public void trace(String theMessage, Throwable e) {
        logPlain(Level.TRACE, theMessage, e);
    }
    public void debug(String theMessage, Throwable e) {
        logPlain(Level.DEBUG, theMessage, e);
    }
    public void info(String theMessage, Throwable e) {
        logPlain(Level.INFO, theMessage, e);
    }
    public void contractInfo(String theMessage, Throwable e) {
        logPlain(Level.INFO, theMessage, e);
    }
    public void warn(String theMessage, Throwable e) {
        logPlain(Level.WARN, theMessage, e);
    }
    protected void error(String theMessage, Throwable e) {
        logPlain(Level.ERROR, theMessage, e);
    }


    // Parameterized methods. Note: if there is a Throwable as last argument, it will be printed with stacktrace

    public void trace(String theMessage, Object... theArgs) {
        logParameterized(Level.TRACE, theMessage, theArgs);
    }
    public void debug(String theMessage, Object... theArgs) {
        logParameterized(Level.DEBUG, theMessage, theArgs);
    }
    public void info(String theMessage, Object... theArgs) {
        logParameterized(Level.INFO, theMessage, theArgs);
    }
    public void contractInfo(String theMessage, Object... theArgs) {
        logParameterized(Level.INFO, theMessage, theArgs);
    }
    public void warn(String theMessage, Object... theArgs) {
        logParameterized(Level.WARN, theMessage, theArgs);
    }
    protected void error(String theMessage, Object... theArgs) {
        logParameterized(Level.ERROR, theMessage, theArgs);
    }

    private void logPlain(Level theLevel, String theMessage, Throwable theThrowable) {
        logWithThrowable(theLevel, theMessage, theThrowable);
        ThreadLogger aThreadLogger = ourThreadLocalLogger.get();
        if (aThreadLogger != null && aThreadLogger.getLog().isEnabledFor(theLevel)) {
            aThreadLogger.getLog().logWithThrowable(theLevel, getOriginalCategory() + theMessage, theThrowable);
        }
    }

    private void logParameterized(Level theLevel, String theMessage, Object... theArgs) {
        logWithParameters(theLevel, theMessage, theArgs);
        ThreadLogger aThreadLogger = ourThreadLocalLogger.get();
        if (aThreadLogger !=  null && aThreadLogger.getLog().isEnabledFor(theLevel)) {
            aThreadLogger.getLog().logWithParameters(theLevel,  getOriginalCategory() + theMessage, theArgs);
        }
    }

    private void logWithParameters(Level theLevel, String theMessage, Object[] theArgs) {
        if (!isEnabledFor(theLevel)) {
            return;
        }

        // This signature should never be called with null or empty arguments. If it did, something else are wrong!
        if (theArgs[theArgs.length -1] instanceof Throwable) {
            // Find out if there is a throwable as last argument, it is then to be treated as throwable with stacktrace in log4j
            // Logging with stacktrace is not frequent, i.e. performance is not an issue
            myWrappedLogger.log(FQCN, theLevel, format(theMessage, Arrays.copyOf(theArgs, theArgs.length - 1)), (Throwable) theArgs[theArgs.length - 1]);
        } else {
            myWrappedLogger.log(FQCN, theLevel, format(theMessage, theArgs), null);
        }
    }

    private void logWithThrowable(Level theLevel, String theMessage, Throwable theThrowable) {
        if (!isEnabledFor(theLevel)) {
            return;
        }
        myWrappedLogger.log(FQCN, theLevel, theMessage, theThrowable);

    }


    public boolean isTraceEnabled() {
        return myWrappedLogger.isTraceEnabled() || (ourThreadLocalLogger.get() != null && ourThreadLocalLogger.get().getLog().myWrappedLogger.isTraceEnabled());
    }

    public boolean isDebugEnabled() {
        return myWrappedLogger.isDebugEnabled() || (ourThreadLocalLogger.get() != null && ourThreadLocalLogger.get().getLog().myWrappedLogger.isDebugEnabled());
    }


    public boolean isInfoEnabled() {
        return myWrappedLogger.isInfoEnabled() || (ourThreadLocalLogger.get() != null && ourThreadLocalLogger.get().getLog().myWrappedLogger.isInfoEnabled());

    }

    private boolean isEnabledFor(Priority thePriority) {
        return myWrappedLogger.isEnabledFor(thePriority);
    }


    public static void enableThreadLocalLogger(String theLogLevel) {
        ThreadLogger aThreadLogger = ourThreadLocalLogger.get();
        if (aThreadLogger != null) {
            aThreadLogger.getLog().setLevel(theLogLevel);
        }
    }


    public static String getThreadLocalLogRows() {
        ThreadLogger aThreadLogger =  ourThreadLocalLogger.get();
        if (aThreadLogger != null) {
            return aThreadLogger.getLogRows();
        } else {
            return "";
        }
    }

    public static void disableThreadLocalLogger() {
        ThreadLogger aThreadLogger = ourThreadLocalLogger.get();
        if (aThreadLogger != null) {
            aThreadLogger.clearEvents();
            aThreadLogger.getLog().setLevel(Level.OFF.toString()); // Turns the logger off
        }
    }

    public static String format(String theMessage, Object[] theArgs) {
        Formatter aFormatter = ourThreadLocalFormatter.get();
        try {
            aFormatter.format(theMessage, theArgs);
            StringBuilder aBuilder = (StringBuilder) aFormatter.out();
            String aReturn = aBuilder.toString();
            aBuilder.setLength(0);
            return aReturn;
        } catch (Throwable e) {
            ourLog.warn("Invalid formatted message: '%s', args: %s, exception: %s", theMessage, Arrays.toString(theArgs), e.toString(), e);
            // Format errors must never affect application performance, just use original string
            return theMessage;
        }
    }


    /** For runtime modifications of the logWithParameters level */
    public void setLevel(String theLevel) {
        myWrappedLogger.setLevel(Level.toLevel(theLevel));
    }

    /** Wrapped logger */
    protected org.apache.log4j.Logger getWrappedLogger() {
        return myWrappedLogger;
    }

    private String getOriginalCategory() {
        String aName = getName();
        return "[" + aName.substring(aName.lastIndexOf('.') + 1) + "] ";
    }

    /**
     * Call this to redirect System.out and system.err to the logger.
     *
     * You should call unredirectSystemOutToLog before re-configuring LOG4J !
     *
     */
    public static void redirectSystemOutToLog() {

        out = System.out;
        err = System.err;

        System.setOut(new LoggerStream(Logger.getLogger("STDOUT").myWrappedLogger, Level.INFO, System.out));
        System.setErr(new LoggerStream(Logger.getLogger("STDERR").myWrappedLogger, Level.ERROR, System.err));
    }

    /**
     * Call this to un-redirect System.out and system.err to the logger.
     */
    public static void unredirectSystemOutToLog() {
        // Remove System adapters
        if (out != null) {
            System.out.flush();
            System.setOut(out);
            out = null;
        }

        if (err != null) {
            System.err.flush();
            System.setErr(err);
            err = null;
        }
    }


    private static class ThreadLocalFormatter extends ThreadLocal<Formatter> {

        protected Formatter initialValue() {
            return new Formatter();
        }

    }

    public static class ThreadLocalLogger extends ThreadLocal<ThreadLogger>  {

        protected ThreadLogger initialValue() {
            if ("true".equals(System.getProperty(SYSTEM_PROPERTY_ENABLE_THREADLOCAL))) {
                return new ThreadLogger();
            } else {
                return null;
            }
        }
    }


}

