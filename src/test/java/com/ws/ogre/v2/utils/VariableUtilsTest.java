package com.ws.ogre.v2.utils;

import com.ws.ogre.AbstractBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class VariableUtilsTest extends AbstractBaseTest {

    @Test
    public void testInsertVars_Plain() {
        Assert.assertEquals("from=2018-01-01 00:00:00", VariableUtils.replaceVariables("from=${from}", asMap("from", "2018-01-01 00:00:00")));

        // No replacement.
        Assert.assertEquals("from=from", VariableUtils.replaceVariables("from=from", asMap("from", "2018-01-01 00:00:00")));
        Assert.assertEquals("from={from}", VariableUtils.replaceVariables("from={from}", asMap("from", "2018-01-01 00:00:00")));
        Assert.assertEquals("abc=${abc}", VariableUtils.replaceVariables("abc=${abc}", asMap("from", "2018-01-01 00:00:00")));
    }

    @Test
    public void testInsertVars_DateOperation() {
        Assert.assertEquals("from=2018-01-02 00:00:00", VariableUtils.replaceVariables("from=${from-10DAY}", asMap("from", "2018-01-12 00:00:00")));
        Assert.assertEquals("from=2018-01-04 00:00:00", VariableUtils.replaceVariables("from=${from - 8 DAY}", asMap("from", "2018-01-12 00:00:00")));
        Assert.assertEquals("from=2018-01-04 17:00:00", VariableUtils.replaceVariables("from=${from - 8 DAY}", asMap("from", "2018-01-12 17:00:00")));

        Assert.assertEquals("fromDate=2018-01-02", VariableUtils.replaceVariables("fromDate=${fromDate-10DAY}", asMap("fromDate", "2018-01-12")));
        Assert.assertEquals("fromDate=2018-01-04", VariableUtils.replaceVariables("fromDate=${fromDate - 8 DAY}", asMap("fromDate", "2018-01-12")));

        Assert.assertEquals("from=2018-01-11 20:00:00", VariableUtils.replaceVariables("from=${from - 4 HOUR}", asMap("from", "2018-01-12 00:00:00")));
        Assert.assertEquals("from=2018-01-09 22:00:00", VariableUtils.replaceVariables("from=${from - 50 HOUR}", asMap("from", "2018-01-12 00:00:00")));
    }
}
