package com.ws.ogre.v2.datetime;

import com.ws.ogre.AbstractBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class DateHourTest extends AbstractBaseTest {

    @Test
    public void testGetChunkStart() {
        long aTime = 1475064666000L; // 2016-09-28 12:11:06

        Assert.assertEquals("2016-09-28:10", DateHour.getChunkStart(aTime, 2, DateHour.Range.Chunking.Hourly).toString());
        Assert.assertEquals("2016-09-28:11", DateHour.getChunkStart(aTime, 1, DateHour.Range.Chunking.Hourly).toString());
        Assert.assertEquals("2016-09-28:12", DateHour.getChunkStart(aTime, 0, DateHour.Range.Chunking.Hourly).toString());

        Assert.assertEquals("2016-09-26:00", DateHour.getChunkStart(aTime, 2, DateHour.Range.Chunking.Daily).toString());
        Assert.assertEquals("2016-09-27:00", DateHour.getChunkStart(aTime, 1, DateHour.Range.Chunking.Daily).toString());
        Assert.assertEquals("2016-09-28:00", DateHour.getChunkStart(aTime, 0, DateHour.Range.Chunking.Daily).toString());

        Assert.assertEquals("2016-09-12:00", DateHour.getChunkStart(aTime, 2, DateHour.Range.Chunking.Weekly).toString());
        Assert.assertEquals("2016-09-19:00", DateHour.getChunkStart(aTime, 1, DateHour.Range.Chunking.Weekly).toString());
        Assert.assertEquals("2016-09-26:00", DateHour.getChunkStart(aTime, 0, DateHour.Range.Chunking.Weekly).toString());

        Assert.assertEquals("2016-07-01:00", DateHour.getChunkStart(aTime, 2, DateHour.Range.Chunking.Monthly).toString());
        Assert.assertEquals("2016-08-01:00", DateHour.getChunkStart(aTime, 1, DateHour.Range.Chunking.Monthly).toString());
        Assert.assertEquals("2016-09-01:00", DateHour.getChunkStart(aTime, 0, DateHour.Range.Chunking.Monthly).toString());

        Assert.assertEquals("2016-09-28:12", DateHour.getChunkStart(aTime, 2, DateHour.Range.Chunking.Disabled).toString());
        Assert.assertEquals("2016-09-28:12", DateHour.getChunkStart(aTime, 1, DateHour.Range.Chunking.Disabled).toString());
        Assert.assertEquals("2016-09-28:12", DateHour.getChunkStart(aTime, 0, DateHour.Range.Chunking.Disabled).toString());
    }

    @Test
    public void testGetChunkEnd() {
        long aTime = 1475064666000L; // 2016-09-28 12:11:06

        Assert.assertEquals("2016-09-28:12", DateHour.getChunkEnd(aTime, DateHour.Range.Chunking.Hourly).toString());
        Assert.assertEquals("2016-09-28:23", DateHour.getChunkEnd(aTime, DateHour.Range.Chunking.Daily).toString());
        Assert.assertEquals("2016-10-02:23", DateHour.getChunkEnd(aTime, DateHour.Range.Chunking.Weekly).toString());
        Assert.assertEquals("2016-09-30:23", DateHour.getChunkEnd(aTime, DateHour.Range.Chunking.Monthly).toString());
        Assert.assertEquals("2016-09-28:12", DateHour.getChunkEnd(aTime, DateHour.Range.Chunking.Disabled).toString());
    }

    @Test
    public void testGetNextDateHour() {
        Assert.assertEquals("2016-09-28:06", new DateHour("2016-09-28:05").getNextDateHour().toString());
        Assert.assertEquals("2016-09-29:00", new DateHour("2016-09-28:23").getNextDateHour().toString());
        Assert.assertEquals("2016-10-01:00", new DateHour("2016-09-30:23").getNextDateHour().toString());
    }

    @Test
    public void testRangeChunking() {
        DateHour.Range aRange = new DateHour.Range("2016-07-04:02", "2016-09-28:14");
        Assert.assertEquals("2016-07-04:02", aRange.getFrom().toString());
        Assert.assertEquals("2016-09-28:14", aRange.getTo().toString());

        DateHour.Ranges aChunkHourly = aRange.getChunkedRanges(DateHour.Range.Chunking.Hourly);
        Assert.assertEquals(2077, aChunkHourly.size());
        Assert.assertEquals("2016-07-04:02", aChunkHourly.get(0).getFrom().toString());
        Assert.assertEquals("2016-07-04:02", aChunkHourly.get(0).getTo().toString());
        Assert.assertEquals("2016-07-04:03", aChunkHourly.get(1).getFrom().toString());
        Assert.assertEquals("2016-07-04:03", aChunkHourly.get(1).getTo().toString());
        Assert.assertEquals("2016-08-03:02", aChunkHourly.get(720).getFrom().toString());
        Assert.assertEquals("2016-08-03:02", aChunkHourly.get(720).getTo().toString());
        Assert.assertEquals("2016-09-02:02", aChunkHourly.get(1440).getFrom().toString());
        Assert.assertEquals("2016-09-02:02", aChunkHourly.get(1440).getTo().toString());
        Assert.assertEquals("2016-09-28:14", aChunkHourly.get(2076).getFrom().toString());
        Assert.assertEquals("2016-09-28:14", aChunkHourly.get(2076).getTo().toString());

        DateHour.Ranges aChunkDaily = aRange.getChunkedRanges(DateHour.Range.Chunking.Daily);
        Assert.assertEquals(87, aChunkDaily.size());
        Assert.assertEquals("2016-07-04:02", aChunkDaily.get(0).getFrom().toString());
        Assert.assertEquals("2016-07-04:23", aChunkDaily.get(0).getTo().toString());
        Assert.assertEquals("2016-07-05:00", aChunkDaily.get(1).getFrom().toString());
        Assert.assertEquals("2016-07-05:23", aChunkDaily.get(1).getTo().toString());
        Assert.assertEquals("2016-08-01:00", aChunkDaily.get(28).getFrom().toString());
        Assert.assertEquals("2016-08-01:23", aChunkDaily.get(28).getTo().toString());
        Assert.assertEquals("2016-09-28:00", aChunkDaily.get(86).getFrom().toString());
        Assert.assertEquals("2016-09-28:14", aChunkDaily.get(86).getTo().toString());

        DateHour.Ranges aChunkWeekly = aRange.getChunkedRanges(DateHour.Range.Chunking.Weekly);
        Assert.assertEquals(13, aChunkWeekly.size());
        Assert.assertEquals("2016-07-04:02", aChunkWeekly.get(0).getFrom().toString());
        Assert.assertEquals("2016-07-10:23", aChunkWeekly.get(0).getTo().toString());
        Assert.assertEquals("2016-07-11:00", aChunkWeekly.get(1).getFrom().toString());
        Assert.assertEquals("2016-07-17:23", aChunkWeekly.get(1).getTo().toString());
        Assert.assertEquals("2016-08-29:00", aChunkWeekly.get(8).getFrom().toString());
        Assert.assertEquals("2016-09-04:23", aChunkWeekly.get(8).getTo().toString());
        Assert.assertEquals("2016-09-26:00", aChunkWeekly.get(12).getFrom().toString());
        Assert.assertEquals("2016-09-28:14", aChunkWeekly.get(12).getTo().toString());

        DateHour.Ranges aChunkMonthly = aRange.getChunkedRanges(DateHour.Range.Chunking.Monthly);
        Assert.assertEquals(3, aChunkMonthly.size());
        Assert.assertEquals("2016-07-04:02", aChunkMonthly.get(0).getFrom().toString());
        Assert.assertEquals("2016-07-31:23", aChunkMonthly.get(0).getTo().toString());
        Assert.assertEquals("2016-08-01:00", aChunkMonthly.get(1).getFrom().toString());
        Assert.assertEquals("2016-08-31:23", aChunkMonthly.get(1).getTo().toString());
        Assert.assertEquals("2016-09-01:00", aChunkMonthly.get(2).getFrom().toString());
        Assert.assertEquals("2016-09-28:14", aChunkMonthly.get(2).getTo().toString());
    }
}
