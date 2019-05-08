package com.ws.common.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadLogger {

    // The plain logger for this class. It must never log directly to the wrapper!
    private static final Logger ourLog = Logger.getLogger(ThreadLogger.class);

    // The logger to which these log events are logged, note: instance variable
    private Logger myLog;
    private Appender myAppender;
    private StringWriter myStringWriter = new StringWriter(1024);
    private static AtomicInteger ourLoggerCounter = new AtomicInteger(0);


    public ThreadLogger() {
        // We need a unique logger instance per thread, so by having unique names, we achieve this
        myLog = Logger.getLogger("trace.ws.ThreadLogger-" + ourLoggerCounter.getAndIncrement());
        myLog.setLevel("OFF");
        myLog.getWrappedLogger().removeAllAppenders();
        myLog.getWrappedLogger().setAdditivity(false);  // If we want get the logs in the file as well

        // We exclude the category here since it will be of no significance
        // Note: System property replacement in log4j config does not work with programmatic initialization
        Layout aLayOut = new PatternLayout("%d %-5p " + System.getProperty("com.ws.engine.id") + " %X{logtraceid} (%t) %m%n");
        myAppender = new MyWriterAppender(aLayOut, myStringWriter);
        myLog.getWrappedLogger().addAppender(myAppender);
    }


    public String getLogRows() {
        return myStringWriter.toString();
    }

    public void clearEvents() {
        myStringWriter.getBuffer().setLength(0);
    }

    protected Logger getLog() {
        return myLog;
    }


    /** Subclassed mostly for test and debug convenience...*/
    class MyWriterAppender extends WriterAppender {
        private int myEventCount;

        MyWriterAppender(Layout layout, Writer writer) {
            super(layout, writer);
        }

        @Override
        public void append(LoggingEvent event) {
            myEventCount++;
            // ourLog.getWrappedLogger().info("Got log event: " + myEventCount);
            super.append(event);
        }
    }


}
