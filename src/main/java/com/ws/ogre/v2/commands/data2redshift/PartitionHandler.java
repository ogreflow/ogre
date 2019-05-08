package com.ws.ogre.v2.commands.data2redshift;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.commands.data2redshift.db.RedShiftDao;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateHour.DateHours;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 */
public class PartitionHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private PartitioningsByType myPartitionings;

    public PartitionHandler(Collection<Partitioning> thePartitionings) {

        myPartitionings = new PartitioningsByType(thePartitionings);
    }


    /**
     * Will handle partitioning of a table for requested period.
     *
     * Redshift do not have native support for partitioning. To solve this we create
     * dedicated tables for each partition and join them using a union view.
     */
    public void partitionTable(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        for (Partitioning aPartitioning : myPartitionings.values()) {

            if (theTypes != null && !theTypes.contains(aPartitioning.type)) {
                continue;
            }

            partitionTable(aPartitioning, theFrom, theTo);
        }
    }

    public boolean isPartitioned(String theType) {
        return myPartitionings.containsKey(theType);
    }

    public String getPartitionTable(String theType, DateHour theHour) {

        Partitioning aPartitioning = myPartitionings.get(theType);

        if (aPartitioning == null) {
            return theType;
        }

        return aPartitioning.getPartitionTableName(theHour);
    }

    public Set<String> getPartitionTables(String theType, DateHour theFrom, DateHour theTo) {

        Partitioning aPartitioning = myPartitionings.get(theType);

        if (aPartitioning == null) {
            return Collections.singleton(theType);
        }

        Set<String> aTables = new HashSet<>();

        for (DateHour aHour : theFrom.getHoursTo(theTo)) {
            aTables.add(aPartitioning.getPartitionTableName(aHour));
        }

        return aTables;
    }

    public Set<String> getPartitionTables(String theType) {

        Partitioning aPartitioning = myPartitionings.get(theType);

        if (aPartitioning == null) {
            return Collections.singleton(theType);
        }

        return RedShiftDao.getInstance().getTablesStartingWith(theType + "_partition_");
    }

    public void partitionTable(Partitioning thePartitioning, DateHour theFrom, DateHour theTo) {

        // Get existing partition tables for type in database
        Set<String> allExisting = getPartitionTables(thePartitioning.type);

        // Get all hours in period to handle partitioning for
        DateHours aHours = theFrom.getHoursTo(theTo);


        // Resolve new partitions to create for period. I.e. iterate through period and check if any partition(s) are missing.

        Set<String> allToAdd = new HashSet<>();

        DateHour aNow = new DateHour(new Date());

        for (DateHour aHour : aHours) {

            // We should not partition into the future
            if (aHour.getDateHour().after(aNow.getDateHour())) {
                continue;
            }

            String aPartitionName = thePartitioning.getPartitionTableName(aHour);

            // Already added
            if (allToAdd.contains(aPartitionName)) {
                continue;
            }

            if (!allExisting.contains(aPartitionName)) {
                allToAdd.add(aPartitionName);
            }
        }

        // Resolve the partitions to remove by joining existing and new, if total size exceeds max partition count then
        // cut away the tail (oldest).

        Set<String> aTmp = new HashSet<>();

        aTmp.addAll(allExisting);
        aTmp.addAll(allToAdd);

        List<String> allPartitions = new ArrayList<>(aTmp);

        Collections.sort(allPartitions);
        Collections.reverse(allPartitions);

        Set<String> allToRemove = new HashSet<>();

        if (allPartitions.size() > thePartitioning.count) {
            allToRemove.addAll(allPartitions.subList(thePartitioning.count, allPartitions.size()));
            allPartitions.removeAll(allToRemove);
        }


        // Make sure we are not trying to load old hours that is not within partition range

        // Note 2019-05-01: We got exception on this for a tables with only one partitions sat. Then there will load data
        //                  into previous month. Maybe not remove old partition within load interval to prevent this. I.e.
        //                  instead of throwing exception, move table back into allToAdd (not remove it yet) and continue.
        //                  Table will be removed later when the from passed partition to remove...
        for (String aTable : allToRemove) {
            if (allToAdd.contains(aTable)) {
                throw new IllegalArgumentException("Trying to load data into partition '" + aTable + "', partition is to old to fit into partitioning schema: " + thePartitioning);
            }
        }

        // Any changes ?
        if (allToAdd.isEmpty() && allToRemove.isEmpty()) {
            return;
        }

        // Create new partition tables
        for (String aTable : allToAdd) {
            ourLogger.info("Adding partition table: %s", aTable);
            RedShiftDao.getInstance().createTableLike(thePartitioning.type, aTable);
        }

        // Recreate view
        ourLogger.info("Recreating view for partition tables: %s", allPartitions);
        RedShiftDao.getInstance().recreateView(thePartitioning.view, allPartitions);

        // Remove evicted tables
        for (String aTable : allToRemove) {
            ourLogger.info("Removing evicted partition table: %s", aTable);
            RedShiftDao.getInstance().dropTable(aTable);
        }
    }

    public void recreatePartitionView(String theType) {

        Partitioning aPartitioning = myPartitionings.get(theType);

        if (aPartitioning == null) {
            return;
        }

        // Get existing partition tables for type in database
        Set<String> allExisting = getPartitionTables(aPartitioning.type);

        List<String> allPartitions = new ArrayList<>(allExisting);

        Collections.sort(allPartitions);

        // Recreate view
        ourLogger.info("Recreating view for partition tables: %s", allPartitions);
        RedShiftDao.getInstance().recreateView(aPartitioning.view, allPartitions);
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

    public static class Partitioning {
        public String type;
        public String view;
        private String schema;
        public int count;

        private SimpleDateFormat myPattern;

        public Partitioning(String theType, String theView, String theSchema, int theCount) {
            type = theType;
            count = theCount;
            view = theView;
            schema = theSchema;

            myPattern = getFormat(theSchema);
        }

        private String getPartitionTableName(DateHour theHour) {
            return type + "_partition_" + myPattern.format(theHour.getDateHour());
        }


        private SimpleDateFormat getFormat(String theSchema) {

            switch (theSchema) {

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
                    throw new IllegalArgumentException("Unsupported schema: " + theSchema);
            }
        }

        private Date getStart() {

            Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aCal.setTimeInMillis(System.currentTimeMillis());
            aCal.set(Calendar.MINUTE, 0);
            aCal.set(Calendar.SECOND, 0);
            aCal.set(Calendar.MILLISECOND, 0);

            int aCount = count -1;

            switch (schema) {

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
                    throw new IllegalArgumentException("Unsupported schema: " + schema);
            }
        }

        @Override
        public String toString() {
            return "Partitioning{" +
                    "type='" + type + '\'' +
                    ", view='" + view + '\'' +
                    ", schema='" + schema + '\'' +
                    ", count=" + count +
                    '}';
        }
    }


    public static void main(String[] args) {
        System.out.println(new DateHour(new Date()));

//        Partitioning aP = new Partitioning("", "", "hourly", 2);
//        Partitioning aP = new Partitioning("", "", "weekly", 4);
//        Partitioning aP = new Partitioning("", "", "monthly", 12);
        Partitioning aP = new Partitioning("", "", "yearly", 2);

        System.out.println(new DateHour(aP.getStart()));
    }

}
