package com.ws.common.logging;

import junit.framework.Assert;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.AppenderAttachable;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.StringTokenizer;

public class LoggerTest {

    private static boolean isRunning;

    static {
        System.setProperty(Logger.SYSTEM_PROPERTY_ENABLE_THREADLOCAL, "true");
    }

//    @Test
//    public void testLoop() {
//
//        if (!isRunning) {
//            isRunning = true;
//            for (int i = 0; i < 100; i++) {
//                org.junit.runner.JUnitCore.main("com.ws.common.logging.LoggerTest");
//            }
//        }
//    }


    private static final Logger ourTestLogger = Logger.getLogger();
    private org.apache.log4j.Logger ourSnoopLogger = org.apache.log4j.Logger.getLogger(LoggerTest.class);

    private TestInMemoryAppender myInMemoryAppender;


    @Before
    public void initLog4jAppender() {
        myInMemoryAppender = new TestInMemoryAppender();
        ourSnoopLogger.addAppender(myInMemoryAppender);
        ourSnoopLogger.setLevel(Level.TRACE);
    }


    @Test
    public void testInit() {
        Assert.assertEquals("Implicit init",  this.getClass().getName(), Logger.getLogger().getName());
        Assert.assertEquals("Explicit init", this.getClass().getName(), Logger.getLogger(LoggerTest.class).getName());
        Assert.assertEquals("Explicit init", "apa", Logger.getLogger("apa").getName());
    }




    @Test
    public void testTrace() {
        Logger.getLogger().trace("apa");
        Assert.assertEquals("First log row", "apa", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.TRACE, myInMemoryAppender.getLogRowLevels().remove(0));
        ourTestLogger.trace("banan");
        Assert.assertEquals("Second log row", "banan", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.TRACE, myInMemoryAppender.getLogRowLevels().remove(0));
    }

    @Test
    public void testDebug() {
        Logger.getLogger().debug("apa");
        Assert.assertEquals("First log row", "apa", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.DEBUG, myInMemoryAppender.getLogRowLevels().remove(0));
        ourTestLogger.debug("banan");
        Assert.assertEquals("Second log row", "banan", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.DEBUG, myInMemoryAppender.getLogRowLevels().remove(0));
    }

    @Test
    public void testInfoParams() {
        Logger.getLogger().info("apa %s %s", 1, 2);
        Assert.assertEquals("log row", "apa 1 2", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));

        ourTestLogger.info("banan %s", "2", "1");
        Assert.assertEquals("log row", "banan 2", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));

        ourTestLogger.info("message");
        Assert.assertEquals("log row", "message", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));

    }

    @Test
    public void testNullParam() {
        Logger.getLogger().info("apa %s %s", null, 2);
        Assert.assertEquals("log row", "apa null 2", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));

        Logger.getLogger().info("apa %s %s", 1, null);
        Assert.assertEquals("log row", "apa 1 null", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));

    }

    @Test
    public void testThrowable() {
        Exception e = new Exception("Test exception message");


        ourTestLogger.warn("Got exception1: " + e, e);
        Assert.assertEquals("log row", "Got exception1: java.lang.Exception: Test exception message", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.WARN, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertEquals("throwable", e, myInMemoryAppender.getThrowables().remove(0));


        ourTestLogger.error("Got exception2: %s", e, e);
        Assert.assertEquals("log row", "Got exception2: java.lang.Exception: Test exception message", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.ERROR, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertEquals("throwable", e, myInMemoryAppender.getThrowables().remove(0));


        ourTestLogger.info("Got exception3: %s", e.toString());
        Assert.assertEquals("log row", "Got exception3: java.lang.Exception: Test exception message", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.INFO, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertTrue("no throwable", myInMemoryAppender.getThrowables().isEmpty());


        ourTestLogger.warn("Got exception4: %s", e);
        Assert.assertEquals("log row", "Got exception4: %s", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("level", Level.WARN, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertEquals("throwable", e, myInMemoryAppender.getThrowables().remove(0));

    }


    @Test
    public void testTraceIdOnStackTrace() {
//        System.setProperty("com.ws.engine.id", "ws1");
        Exception e = new Exception("Test exception message");

        Logger aLogger = Logger.getLogger("stacktraceprefixed");
        aLogger.getWrappedLogger().removeAllAppenders();
        AppenderAttachable anAppender = new TraceIdAppender();
        anAppender.addAppender(new ConsoleAppender(new PatternLayout("%d %-5p ws1 (%t) [%c{1}] %m%n"), "System.out"));
        aLogger.getWrappedLogger().addAppender((Appender) anAppender);

        aLogger.warn("Test stacktrace", e);

        LogTrace.startTrace();
        aLogger.warn("Test stacktrace", e);

        LogTrace.stopTrace();
        // System.out.println("myInMemoryAppender.get = " + myInMemoryAppender.get);



    }


    @Test
    public void testFormatError() {
        // Must not throw exception in application layer
        Logger.getLogger().info("apa %1 %2", 1, 2);
    }


    @Test
    public void testLogTrace() {
        Logger aLogger = Logger.getLogger();
        aLogger.setLevel("INFO");
        aLogger.info("Test message");
        Assert.assertNotNull("log row", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertNull("No logtrace", myInMemoryAppender.getLogTraceIds().remove(0));

        LogTrace.startTrace();
        String aLogTrace = LogTrace.getCurrentLogTraceId();
        Assert.assertNotNull("Got logtrace: " + aLogTrace);
        aLogger.info("Test message");
        Assert.assertNotNull("log row", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("Logtrace id", aLogTrace, myInMemoryAppender.getLogTraceIds().remove(0));

        LogTrace.stopTrace();
        aLogger.info("Test message");
        Assert.assertNotNull("log row", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertNull("No logtrace", myInMemoryAppender.getLogTraceIds().remove(0));


    }

    @Test
    public void testNativeWarmUp() {
        testPerformanceNative();
    }

    @Test
    public void testPerformanceNative() {
        org.apache.log4j.Logger aLogger = org.apache.log4j.Logger.getLogger(LoggerTest.class);
        int aLoopSize = 1000;
        long aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event " + i);
        }

        System.out.println(aLoopSize + " native parameterized info took " + (System.currentTimeMillis() - aStart));

        aLogger.setLevel(Level.WARN);
        aLoopSize = 10000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event " + i);
        }

        System.out.println(aLoopSize + " native parameterized info took " + (System.currentTimeMillis() - aStart));

        aLoopSize = 10000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event " + i);
        }

        System.out.println(aLoopSize + " native non-parameterized info took " + (System.currentTimeMillis() - aStart));

    }

    @Test
    public void testPeformanceWrapperWarmup() {
        testPerformanceWrapper();
    }

    @Test
    public void testPerformanceWrapper() {
        Logger aLogger = Logger.getLogger("apa");
        aLogger.setLevel("INFO");
        int aLoopSize = 1000;
        long aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event %s", i);
        }

        System.out.println(aLoopSize + " parameterized info took " + (System.currentTimeMillis() - aStart));

        aLogger.setLevel("WARN");
        aLoopSize = 10000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event %s", i);
        }

        System.out.println(aLoopSize + " parameterized info took " + (System.currentTimeMillis() - aStart));


        aLogger.setLevel("WARN");
        aLoopSize = 10000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event");
        }

        System.out.println(aLoopSize + " non-parameterized info took " + (System.currentTimeMillis() - aStart));

    }


    @Test
    public void testPerformanceThrowableWrapper() {

        Logger aLogger = Logger.getLogger(LoggerTest.class);
        aLogger.setLevel("WARN");

        Exception e = new Exception("eat this");
        int aLoopSize = 1000;
        long aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event %s", i, e);
        }

        System.out.println(aLoopSize + " parameterized info took " + (System.currentTimeMillis() - aStart));

        aLogger.setLevel("WARN");
        aLoopSize = 1000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.info("Log event %s", i, e);
        }

        System.out.println(aLoopSize + " parameterized info took " + (System.currentTimeMillis() - aStart));

    }

    @Test
    public void testThreadLocalLogger() {
        Logger aLogger = Logger.getLogger("apa");
        aLogger.setLevel("INFO");

        aLogger.debug("Test 1");
        Assert.assertEquals("No thread local logs", 0, getRowCount(Logger.getThreadLocalLogRows()));



        Logger.enableThreadLocalLogger("DEBUG");

        aLogger.debug("test message 2");
        System.out.println("Logger.getThreadLocalLogRows() = " + Logger.getThreadLocalLogRows());
        Assert.assertEquals("Got thread local logs", 1, getRowCount(Logger.getThreadLocalLogRows()));



        Logger.disableThreadLocalLogger();

        Assert.assertEquals("No thread local logs", 0, getRowCount(Logger.getThreadLocalLogRows()));
        aLogger.info("Test 3");
        Assert.assertEquals("No thread local logs", 0, getRowCount(Logger.getThreadLocalLogRows()));





        int aLoopSize = 1000000;
        long aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.debug("Log event %s", i);
        }

        System.out.println(aLoopSize + " disabled thread log  took " + (System.currentTimeMillis() - aStart));

        Logger.enableThreadLocalLogger("DEBUG");
        aLoopSize = 100000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.debug("Log event %s", i);
        }

        Assert.assertEquals("Expected log events", aLoopSize, getRowCount(Logger.getThreadLocalLogRows()));
        System.out.println(aLoopSize + " enabled thread log  took " + (System.currentTimeMillis() - aStart));

        // System.out.println("Logger.getThreadLocalLogRows() = " + Logger.getThreadLocalLogRows());

        // Finally try to trigger OOME if something is wrong
        Logger.enableThreadLocalLogger("DEBUG");
        aLoopSize = 10000000;
        aStart = System.currentTimeMillis();
        for (int i = 0; i < aLoopSize; i++) {
            aLogger.trace("Log event %s", i);
        }
        System.out.println(aLoopSize + " low level enabled thread log  took " + (System.currentTimeMillis() - aStart));


    }


    /**
     * We want all logging to use %s for portability and avoid unexpected errors in logging. This test case
     * just verifies the behavior for invalid formatting
     */
    @Test
    public void testFailSafeFormatting() {
        Logger aLogger = Logger.getLogger();
        aLogger.setLevel("INFO");
        aLogger.info("Test integer native %d, integer wrapper %d", 1, Double.valueOf(2.0d));
        String aLogRow = myInMemoryAppender.getLogRows().remove(0);
        Assert.assertNotNull("No log row", aLogRow);
        Assert.assertEquals("Invalid log row", "Test integer native %d, integer wrapper %d", aLogRow);
    }



    private int getRowCount(String theString) {
        return new StringTokenizer(theString, "\r\n").countTokens();
    }

}
