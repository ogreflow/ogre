package com.ws.common.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.AppenderAttachableImpl;
import org.apache.log4j.spi.AppenderAttachable;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Enumeration;

/**
 * This log4j appender adds a the current logtraceid in front of all log rows in e.g. stacktraces
 */
public class TraceIdAppender extends AppenderSkeleton implements AppenderAttachable {


    /**
     * Nested myAppenders.
     */
    private final AppenderAttachableImpl myAppenders = new AppenderAttachableImpl();



    @Override
    protected void append(LoggingEvent theEvent) {

        LoggingEvent aLoggingEvent;
        if (theEvent.getThrowableInformation() != null && theEvent.getMDC("logtraceid") != null) {
            aLoggingEvent = new WrappedLoggingEvent(theEvent);

        } else {
            aLoggingEvent = theEvent;
        }

        synchronized (myAppenders) {
            myAppenders.appendLoopOnAppenders(aLoggingEvent);
        }

        return;


    }

    /**
     * Add appender.
     *
     * @param newAppender appender to add, may not be null.
     */
    public void addAppender(final Appender newAppender) {
        synchronized (myAppenders) {
            myAppenders.addAppender(newAppender);
        }
    }

    public synchronized void close() {
        if(this.closed) {
            return;
        }
        this.closed = true;

        //
        //    close all attached myAppenders.
        //
        synchronized (myAppenders) {
            Enumeration iter = myAppenders.getAllAppenders();

            if (iter != null) {
                while (iter.hasMoreElements()) {
                    Object next = iter.nextElement();

                    if (next instanceof Appender) {
                        ((Appender) next).close();
                    }
                }
            }
        }
    }

    public boolean requiresLayout() {
        return false;
    }



    /**
     * Get iterator over attached myAppenders.
     * @return iterator or null if no attached myAppenders.
     */
    public Enumeration getAllAppenders() {
        synchronized (myAppenders) {
            return myAppenders.getAllAppenders();
        }
    }

    /**
     * Get appender by name.
     *
     * @param name name, may not be null.
     * @return matching appender or null.
     */
    public Appender getAppender(final String name) {
        synchronized (myAppenders) {
            return myAppenders.getAppender(name);
        }
    }

    /**
     * Determines if specified appender is attached.
     * @param appender appender.
     * @return true if attached.
     */
    public boolean isAttached(final Appender appender) {
        synchronized (myAppenders) {
            return myAppenders.isAttached(appender);
        }
    }


    /**
     * Removes and closes all attached myAppenders.
     */
    public void removeAllAppenders() {
        synchronized (myAppenders) {
            myAppenders.removeAllAppenders();
        }
    }

    /**
     * Removes an appender.
     * @param appender appender to remove.
     */
    public void removeAppender(final Appender appender) {
        synchronized (myAppenders) {
            myAppenders.removeAppender(appender);
        }
    }


    /**
     * Remove appender by name.
     * @param name name.
     */
    public void removeAppender(final String name) {
        synchronized (myAppenders) {
            myAppenders.removeAppender(name);
        }
    }

    public static class WrappedLoggingEvent extends LoggingEvent {

        private String[] myStackTrace;

        public WrappedLoggingEvent(LoggingEvent theEvent) {
            super(theEvent.fqnOfCategoryClass, theEvent.getLogger(), theEvent.getTimeStamp(), theEvent.getLevel(),
                    theEvent.getMessage(), theEvent.getThreadName(), null, null, null, theEvent.getProperties());

            String[] aStackTrace = theEvent.getThrowableStrRep();
            if (aStackTrace != null) {
                String aLogTraceId = (String) theEvent.getMDC("logtraceid");
                if (aLogTraceId != null) {
                    myStackTrace = new String[aStackTrace.length];
                    // This should never happen since we should not wrap the logevents if no stacktrace is present
                    for (int i = 0; i < aStackTrace.length; i++) {
                        myStackTrace[i] = "  " + aLogTraceId + "  " + aStackTrace[i];
                    }
                }

            }
        }



        /**
         *
         * @return traceid prefixed on each row in the stacktrace
         */
        @Override
        public String[] getThrowableStrRep() {
            return myStackTrace;
        }



    }

}
