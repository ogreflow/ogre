package com.ws.ogre.v2.utils;

import com.ws.ogre.AbstractBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class SnsAsyncAppenderTest extends AbstractBaseTest {
    @Test
    public void testClassPath() {
        // This class path is used as string in the data pipeline log4j.xml
        Assert.assertEquals("com.ws.ogre.v2.utils.SnsAsyncAppender", SnsAsyncAppender.class.getName());
    }
}
