package com.ws.common.logging;

import junit.framework.Assert;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

public class AlertTest {

    private org.apache.log4j.Logger ourSnoopLogger = org.apache.log4j.Logger.getLogger(AlertTest.class);
    private TestInMemoryAppender myInMemoryAppender;


    @Before
    public void initLog4jAppender() {
        myInMemoryAppender = new TestInMemoryAppender();
        ourSnoopLogger.addAppender(myInMemoryAppender);
        // ourSnoopLogger.setLevel(Level.TRACE);

    }

    @Test
    public void testAlert() {
        Alert.getAlert().alert("This is an alert");
        Assert.assertEquals("Got alert", "This is an alert", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("Error level", Level.ERROR, myInMemoryAppender.getLogRowLevels().remove(0));


        // With throwable
        Throwable anException = new Exception();
        Alert.getAlert().alert("This is an alert", anException);
        Assert.assertEquals("Got alert", "This is an alert", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("Error level", Level.ERROR, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertEquals("Throwable", anException, myInMemoryAppender.getThrowables().remove(0));

        // Paremeterized
        Alert.getAlert().alert("This is an alert %s", 1, anException);
        Assert.assertEquals("Got alert", "This is an alert 1", myInMemoryAppender.getLogRows().remove(0));
        Assert.assertEquals("Error level", Level.ERROR, myInMemoryAppender.getLogRowLevels().remove(0));
        Assert.assertEquals("Throwable", anException, myInMemoryAppender.getThrowables().remove(0));


    }

}
