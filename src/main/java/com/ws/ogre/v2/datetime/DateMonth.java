package com.ws.ogre.v2.datetime;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Represents a month: yyyyMM.
 */
public class DateMonth {

    private Date myMonth;

    public DateMonth(String theMonth) {
        myMonth = DateUtil.parse(theMonth, "yyyyMM");
    }

    public DateMonth(Date theDate) {
        Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

        aCal.setTime(theDate);
        aCal.set(Calendar.DAY_OF_MONTH, 1);
        aCal.set(Calendar.HOUR_OF_DAY, 0);
        aCal.set(Calendar.MINUTE, 0);
        aCal.set(Calendar.SECOND, 0);
        aCal.set(Calendar.MILLISECOND, 0);

        myMonth = aCal.getTime();
    }

    public DateMonth getNext() {
        Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aCal.setTime(myMonth);
        aCal.add(Calendar.MONTH, 1);

        return new DateMonth(aCal.getTime());
    }

    public Date getStart() {
        return myMonth;
    }

    public Date getEnd() {
        Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aCal.setTime(myMonth);
        aCal.add(Calendar.MONTH, 1);
        aCal.add(Calendar.MILLISECOND, -1);

        return aCal.getTime();
    }

    public DateHour.DateHours getHours() {
        DateHour aStart = new DateHour(getStart());
        DateHour anEnd = new DateHour(getEnd());

        return aStart.getHoursTo(anEnd);
    }

    public String getName() {
        return DateUtil.format(myMonth, "MMM");
    }

    public String format(String theSimpleDateFormat) {
        return DateUtil.format(myMonth, theSimpleDateFormat);
    }

    @Override
    public String toString() {
        return DateUtil.format(myMonth, "yyyyMM");
    }
}
