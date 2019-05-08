package com.ws.ogre.v2.commands.data2rds.db;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.datetime.DateHour;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 */
public class PartitionHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private PartitionTableSpec myPartitionTableSpec;
    private PartitioningsByType myPartitionings;

    public PartitionHandler(PartitionTableSpec thePartitionTableSpec, Collection<Partitioning> thePartitionings) {
        myPartitionTableSpec = thePartitionTableSpec;
        myPartitionings = new PartitioningsByType(thePartitionings);
    }

    public String getPartitionTableName(String theType) {
        String aTableName = myPartitionTableSpec.getTableName(theType);
        return StringUtils.defaultString(aTableName, theType);
    }

    public void partitionTable(Set<String> theTypes, DateHour theFrom, DateHour theTo) {
        // If theTypes is empty, then partition all tables defined in conf.
        // Otherwise, only partition that type's table.

        for (Partitioning aPartitioning : myPartitionings.values()) {

            if (theTypes != null && !theTypes.contains(aPartitioning.type)) {
                continue;
            }

            partitionTable(aPartitioning, theFrom, theTo);
        }
    }

    private void partitionTable(Partitioning thePartitioning, DateHour theFrom, DateHour theTo) {
        String aTableName = getPartitionTableName(thePartitioning.type);

        Set<String> someExistingPartitionNames = RdsDao.getInstance().getAllPartitionNames(aTableName);
        Set<String> someNewPartitionNames = new HashSet<>();

        DateHour aNow = new DateHour(new Date());

        // Resolve new partitions to create for period. I.e. iterate through period and check if any partition(s) are missing.
        for (DateHour aHour : theFrom.getHoursTo(theTo)) {

            if (aHour.getDateHour().after(aNow.getDateHour())) {
                continue; /* No future partition */
            }

            String aPartitionName = thePartitioning.getPartitionName(aHour);

            if (!someExistingPartitionNames.contains(aPartitionName) && !someNewPartitionNames.contains(aPartitionName)) {
                someNewPartitionNames.add(aPartitionName);
            }
        }

        // Resolve the partitions to remove by joining existing and new, if total size exceeds max partition number,
        // then cut away the tail (oldest).

        List<String> allPartitions = getAllPartitionNamesSorted(someExistingPartitionNames, someNewPartitionNames);
        Set<String> allToRemove = new HashSet<>();

        if (allPartitions.size() > thePartitioning.count) {
            allToRemove.addAll(allPartitions.subList(thePartitioning.count, allPartitions.size()));
            allPartitions.removeAll(allToRemove);
        }


        // Make sure we are not trying to load old / new hours that is not within partition range

        for (String aTable : allToRemove) {
            if (someNewPartitionNames.contains(aTable)) {
                throw new IllegalArgumentException("Trying to load data into partition '" + aTable + "', partition is too old to fit into partitioning schema: " + thePartitioning);
            }
        }

        // Any changes ?
        if (someNewPartitionNames.isEmpty() && allToRemove.isEmpty()) {
            return;
        }

        // Create new partition tables
        for (String aPartitionName : someNewPartitionNames) {
            ourLogger.info("Adding new partition %s in table %s", aPartitionName, aTableName);
            RdsDao.getInstance().createPartition(aTableName, aPartitionName);
        }

        // Remove evicted tables
        for (String aPartitionName : allToRemove) {
            ourLogger.info("Removing evicted partition %s from table %s", aPartitionName, aTableName);
            RdsDao.getInstance().dropPartition(aTableName, aPartitionName);
        }
    }

    private List<String> getAllPartitionNamesSorted(Set<String> someExistingPartitionNames, Set<String> someNewPartitionNames) {
        Set<String> aTmp = new HashSet<>();

        aTmp.addAll(someExistingPartitionNames);
        aTmp.addAll(someNewPartitionNames);

        List<String> allPartitions = new ArrayList<>(aTmp);

        Collections.sort(allPartitions);
        Collections.reverse(allPartitions);

        return allPartitions;
    }

    public DateHour getStart(String theType) {
        Partitioning aPartitioning = myPartitionings.get(theType);

        if (aPartitioning == null) {
            return null;
        }

        return new DateHour(aPartitioning.getStart());
    }

    public static class PartitioningsByType extends HashMap<String, Partitioning> {
        public PartitioningsByType(Collection<Partitioning> thePartitionings) {
            for (Partitioning aPartitioning : thePartitionings) {
                put(aPartitioning.type, aPartitioning);
            }
        }
    }

    public static class PartitionTableSpec {
        private Map<String, String> tableNames = new HashMap<>();

        public void addSpec(String theType, String theTableName) {
            tableNames.put(theType, theTableName);
        }

        private String getTableName(String theType) {
            return tableNames.get(theType);
        }
    }

    public static class Partitioning {
        private String type;
        private String scheme;
        private int count;

        private SimpleDateFormat myPattern;

        public Partitioning(String theType, String theScheme, int theCount) {
            type = theType;
            count = theCount;
            scheme = theScheme;

            myPattern = getFormat(theScheme);
        }

        private String getPartitionName(DateHour theHour) {
            return "partition_" + myPattern.format(theHour.getDateHour());
        }


        private SimpleDateFormat getFormat(String theScheme) {

            switch (theScheme) {

                case "hourly":
                    return new SimpleDateFormat("yyyyMMddHH");

                case "daily":
                    return new SimpleDateFormat("yyyyMMdd");

                case "weekly":
                    return new SimpleDateFormat("yyyyww");

                case "monthly":
                    return new SimpleDateFormat("yyyyMM");

                case "yearly":
                    return new SimpleDateFormat("yyyy");

                default:
                    throw new IllegalArgumentException("Unsupported scheme: " + theScheme);
            }
        }

        private Date getStart() {

            Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aCal.setTimeInMillis(System.currentTimeMillis());
            aCal.set(Calendar.MINUTE, 0);
            aCal.set(Calendar.SECOND, 0);
            aCal.set(Calendar.MILLISECOND, 0);

            int aCount = count - 1;

            switch (scheme) {

                case "hourly":
                    aCal.add(Calendar.HOUR_OF_DAY, -aCount);
                    return aCal.getTime();

                case "daily":
                    aCal.set(Calendar.HOUR_OF_DAY, 0);
                    aCal.add(Calendar.DATE, -aCount);
                    return aCal.getTime();

                case "weekly":
                    aCal.set(Calendar.HOUR_OF_DAY, 0);
                    aCal.set(Calendar.DAY_OF_WEEK, 2);
                    aCal.add(Calendar.WEEK_OF_YEAR, -aCount);
                    return aCal.getTime();

                case "monthly":
                    aCal.set(Calendar.HOUR_OF_DAY, 0);
                    aCal.set(Calendar.DAY_OF_MONTH, 1);
                    aCal.add(Calendar.MONTH, -aCount);
                    return aCal.getTime();

                case "yearly":
                    aCal.set(Calendar.HOUR_OF_DAY, 0);
                    aCal.set(Calendar.DAY_OF_MONTH, 1);
                    aCal.set(Calendar.MONTH, 0);
                    aCal.add(Calendar.YEAR, -aCount);
                    return aCal.getTime();

                default:
                    throw new IllegalArgumentException("Unsupported scheme: " + scheme);
            }
        }

        @Override
        public String toString() {
            return "Partitioning{" +
                    "type=" + type +
                    ", scheme=" + scheme +
                    ", count=" + count +
                    '}';
        }
    }


    public static void main(String[] args) {
        System.out.println(new DateHour(new Date()));

//        Partitioning aP = new Partitioning("", "", "hourly", 2);
//        Partitioning aP = new Partitioning("", "", "weekly", 4);
//        Partitioning aP = new Partitioning("", "", "monthly", 12);
        Partitioning aP = new Partitioning("", "yearly", 2);

        System.out.println(new DateHour(aP.getStart()));
    }

}
