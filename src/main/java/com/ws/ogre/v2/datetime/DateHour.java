package com.ws.ogre.v2.datetime;

import java.util.*;

/**
 * Class representing and for working on Date + Hour entities.
 */
public class DateHour {
    private static final int FIRST_DAY_OF_WEEK = Calendar.MONDAY;

    private Date myDateHour;

    public DateHour(String theDateHour) {
        this(theDateHour, "yyyy-MM-dd:HH");
    }

    public DateHour(String theTime, String thePattern) {
        myDateHour = DateUtil.parse(theTime, thePattern);
    }

    public DateHour(String theTime, TimeZone theTimeZone) {
        myDateHour = DateUtil.parse(theTime, "yyyy-MM-dd:HH", theTimeZone);
    }

    public DateHour(Date theDateHour) {
        myDateHour = DateUtil.getDateHourPart(theDateHour);
    }

    public long getTime() {
        return myDateHour.getTime();
    }

    public long getEndTime() {
        return getTime() + 60*60*1000l - 1l;
    }

    public boolean isBefore(DateHour theDateHour) {
        return getTime() < theDateHour.getTime();
    }

    public DateHour getPrevDateHour() {
        return getPrevDateHour(1);
    }

    public DateHour getPrevDateHour(int theLookbackHour) {
        return new DateHour(new Date(myDateHour.getTime() - theLookbackHour * 60 * 60 * 1000l));
    }

    public DateHour getNextDateHour() {
        return new DateHour(new Date(myDateHour.getTime() + 60 * 60 * 1000l));
    }

    public static DateHour getChunkStart(long theTimeMs, int theNUnitAgo, Range.Chunking theChunking) {
        Calendar aCal = Calendar.getInstance();
        aCal.setTimeInMillis(theTimeMs);

        switch (theChunking) {
            case Hourly:
                aCal.add(Calendar.HOUR_OF_DAY, -theNUnitAgo);
                break;

            case Daily:
                aCal.add(Calendar.DAY_OF_MONTH, -theNUnitAgo);
                aCal.set(Calendar.HOUR_OF_DAY, 0);
                break;

            case Weekly:
                aCal.add(Calendar.WEEK_OF_MONTH, -theNUnitAgo);
                aCal.set(Calendar.DAY_OF_WEEK, FIRST_DAY_OF_WEEK);
                aCal.set(Calendar.HOUR_OF_DAY, 0);
                break;

            case Monthly:
                aCal.add(Calendar.MONTH, -theNUnitAgo);
                aCal.set(Calendar.DAY_OF_MONTH, 1);
                aCal.set(Calendar.HOUR_OF_DAY, 0);
                break;
        }

        // We calculate on hour level. No need the minute, second etc.
        aCal.set(Calendar.MINUTE, 0);
        aCal.set(Calendar.SECOND, 0);
        aCal.set(Calendar.MILLISECOND, 0);
        return new DateHour(aCal.getTime());
    }

    public static DateHour getChunkEnd(long theTimeMs, Range.Chunking theChunking) {
        Calendar aCal = Calendar.getInstance();
        aCal.setTimeInMillis(theTimeMs);

        switch (theChunking) {
            case Hourly:
                // Nothing to do for hour. Current time's hour is chunk end.
                break;

            case Daily:
                aCal.add(Calendar.DAY_OF_MONTH, 1); // Next day
                aCal.set(Calendar.HOUR_OF_DAY, 0); // Next day's first hour
                aCal.add(Calendar.HOUR_OF_DAY, -1); // Today's last hour
                break;

            case Weekly:
                aCal.add(Calendar.WEEK_OF_MONTH, 1); // Next week
                aCal.set(Calendar.DAY_OF_WEEK, FIRST_DAY_OF_WEEK); // Next week's first day
                aCal.set(Calendar.HOUR_OF_DAY, 0); // Next week's first day first hour
                aCal.add(Calendar.HOUR_OF_DAY, -1); // This week's last day's last hour
                break;

            case Monthly:
                aCal.add(Calendar.MONTH, 1); // Next month.
                aCal.set(Calendar.DAY_OF_MONTH, 1); // Next month's first day
                aCal.set(Calendar.HOUR_OF_DAY, 0); // Next month's first day's first hour
                aCal.add(Calendar.HOUR_OF_DAY, -1); // This month's last day's last hour
                break;
        }

        // We calculate on hour level. No need the minute, second etc.
        aCal.set(Calendar.MINUTE, 0);
        aCal.set(Calendar.SECOND, 0);
        aCal.set(Calendar.MILLISECOND, 0);
        return new DateHour(aCal.getTime());
    }

    public Date getDateHour() {
        return myDateHour;
    }

    public Date getDatePart() {
        return DateUtil.getDatePart(myDateHour);
    }

    public boolean isSameDate(DateHour theOther) {
        return this.getDatePart().getTime() == theOther.getDatePart().getTime();
    }

    public String format(String theSimpleDateFormat) {
        return DateUtil.format(myDateHour, theSimpleDateFormat);
    }

    public DateHours getHoursTo(DateHour theTo) {

        Calendar aFrom = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aFrom.setTime(myDateHour);

        Calendar aTo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aTo.setTime(theTo.myDateHour);


        DateHours anHours = new DateHours();

        while (!aFrom.after(aTo)) {

            anHours.add(new DateHour(aFrom.getTime()));

            aFrom.add(Calendar.HOUR_OF_DAY, 1);
        }

        return anHours;
    }

    @Override
    public int hashCode() {
        // We need to implement this for being able to store DateHour into a HashSet correctly.
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // We need to implement this for being able to store DateHour into a HashSet correctly.
        return toString().equals(obj.toString());
    }

    @Override
    public String toString() {
        return DateUtil.format(myDateHour, "yyyy-MM-dd:HH");
    }

    public static class Range {
        public enum Chunking {Disabled, Hourly, Daily, Weekly, Monthly}

        DateHour myFrom;
        DateHour myTo;

        public Range(String theFrom, String theTo) {
            myFrom = new DateHour(theFrom);
            myTo = new DateHour(theTo);
        }

        public Range(DateHour theFrom, DateHour theTo) {
            myFrom = theFrom;
            myTo = theTo;

            if (myFrom.getTime() > myTo.getTime()) {
                throw new RuntimeException("Negative time range: " + theFrom + " is after " + theTo);
            }
        }

        public DateHour getFrom() {
            return myFrom;
        }

        public DateHour getTo() {
            return myTo;
        }

        public boolean inRange(long theTimestamp) {
            return (myFrom.getTime() <= theTimestamp && theTimestamp <= myTo.getEndTime());
        }

        public long getEndTimeDiffFromCurrentTime() {
            return System.currentTimeMillis() - getTo().getEndTime();
        }

        public DateHours getHours() {
            return myFrom.getHoursTo(myTo);
        }

        private Ranges splitHourly() {
            return Util.split(myFrom, myTo, Util.Span.HOURLY);
        }

        private Ranges splitDaily() {
            return Util.split(myFrom, myTo, Util.Span.DAILY);
        }

        private Ranges splitWeekly() {
            return Util.split(myFrom, myTo, Util.Span.WEEKLY);
        }

        private Ranges splitMonthly() {
            return Util.split(myFrom, myTo, Util.Span.MONTHLY);
        }

        public Ranges getChunkedRanges(Chunking theChunking) {
            switch (theChunking) {
                case Hourly:
                    return splitHourly();

                case Daily:
                    return splitDaily();

                case Weekly:
                    return splitWeekly();

                case Monthly:
                    return splitMonthly();

                default:
                    Ranges aChunks = new Ranges();
                    aChunks.add(this);
                    return aChunks;
            }
        }

        public static DateHour.Range getChunkRangeOfCurrentTime(int theLookbackUnits, DateHour.Range.Chunking theChunking) {
            return new DateHour.Range(
                    DateHour.getChunkStart(new Date().getTime(), theLookbackUnits, theChunking),
                    DateHour.getChunkEnd(new Date().getTime(), theChunking)
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(myFrom, myTo);
        }

        @Override
        public String toString() {
            return myFrom + " - " + myTo;
        }
    }

    public static class Ranges extends ArrayList<Range> {

        public void add(DateHour theFrom, DateHour theTo) {
            add(new Range(theFrom, theTo));
        }

        public void add(String theFrom, String theTo, String thePattern) {
            add(new DateHour(theFrom, thePattern), new DateHour(theTo, thePattern));
        }

        public void sort() {
            Collections.sort(this, (the1, the2) -> {
                long a1 = the1.getFrom().getTime();
                long a2 = the2.getFrom().getTime();
                if (a1 == a2) return 0;
                if (a1 > a2) return 1; else return -1;
            });
        }
    }

    public static class DateHours extends ArrayList<DateHour> {

        public List<String> getFullDates() {

            List<DateHour> anHours = new ArrayList<>(this);
            List<String> aFullDates = new ArrayList<>();

            // Sort by hours ascending
            Collections.sort(anHours, new AscComparator());

            Deque<DateHour> aPrevs = new ArrayDeque<>();

            for (DateHour anHour : anHours) {

                // Switched day? then start over
                if (!aPrevs.isEmpty() && !anHour.isSameDate(aPrevs.getFirst())) {
                    aPrevs.clear();
                }

                aPrevs.addLast(anHour);

                // Collected hours for a whole date?
                if (aPrevs.size() == 24) {
                    aFullDates.add(aPrevs.getFirst().format("yyyyMMdd"));
                }
            }
            return aFullDates;
        }
    }

    public static class AscComparator implements Comparator<DateHour> {
        @Override
        public int compare(DateHour the1, DateHour the2) {
            long a1 = the1.getTime();
            long a2 = the2.getTime();
            if (a1 == a2) return 0;
            if (a1 > a2) return 1; else return -1;
        }
    }

    private static class Util {

        public enum Span {HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY}

        public static DateHour.Ranges split(DateHour theFrom, DateHour theTo, Span theSpan) {

            Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aCal.setTimeInMillis(theFrom.getTime());
            aCal.set(Calendar.MINUTE, 0);
            aCal.set(Calendar.SECOND, 0);
            aCal.set(Calendar.MILLISECOND, 0);

            int aCalendarField = Calendar.MONTH;

            switch (theSpan) {
                case HOURLY:
                    aCalendarField = Calendar.HOUR_OF_DAY;
                    break;

                case DAILY:
                    aCal.set(Calendar.HOUR_OF_DAY, 0);
                    aCalendarField = Calendar.DATE;
                    break;

                case WEEKLY:
                    aCal.set(Calendar.DAY_OF_WEEK, FIRST_DAY_OF_WEEK);
                    aCal.set(Calendar.HOUR_OF_DAY, 0);

                    aCalendarField = Calendar.WEEK_OF_YEAR;
                    break;

                case MONTHLY:
                    aCal.set(Calendar.DATE, 1);
                    aCal.set(Calendar.HOUR_OF_DAY, 0);

                    aCalendarField = Calendar.MONTH;
                    break;

                case YEARLY:
                    aCal.set(Calendar.MONTH, 0);
                    aCal.set(Calendar.DATE, 1);
                    aCal.set(Calendar.HOUR_OF_DAY, 0);

                    aCalendarField = Calendar.YEAR;
                    break;

            }

            DateHour.Ranges aRanges = new DateHour.Ranges();

            aCal.add(aCalendarField, 1);

            // Add partial month in beginning
            if (aCal.getTimeInMillis() < theTo.getEndTime()) {
                aRanges.add(theFrom, new DateHour(aCal.getTime()).getPrevDateHour());
            } else {
                aRanges.add(theFrom, theTo);
            }

            DateHour aFrom;

            while (true) {

                aFrom = new DateHour(aCal.getTime());

                aCal.add(aCalendarField, 1);

                if (aCal.getTimeInMillis() > theTo.getEndTime()) {
                    break;
                }

                aRanges.add(aFrom, new DateHour(aCal.getTime()).getPrevDateHour());
            }

            if (aFrom.getTime() <= theTo.getTime()) {
                aRanges.add(aFrom, theTo);
            }

            return aRanges;
        }

    }
}
