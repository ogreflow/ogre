package com.ws.ogre.v2.commands.data2rds.db;

import com.ws.ogre.v2.data2dbcommon.db.ImportLog;
import com.ws.ogre.v2.datetime.DateHour;
import org.apache.commons.lang.StringUtils;

import javax.persistence.EntityManager;
import java.text.SimpleDateFormat;
import java.util.*;

public class ImportLogDao {

    private static ImportLogDao ourInstance = new ImportLogDao();

    public static ImportLogDao getInstance() {
        return ourInstance;
    }

    public Collection<ImportLog> findByTimeRange(DateHour theFrom, DateHour theTo) {

        EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

        // Add 1h to end to include it in search
        Calendar aTo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        aTo.setTime(theTo.getDateHour());
        aTo.add(Calendar.HOUR_OF_DAY, 1);

        return aManager.createNamedQuery("ImportLog.findByTimestamp", ImportLog.class)
                .setParameter("from", theFrom.getDateHour())
                .setParameter("to", aTo.getTime())
                .getResultList();

    }

    public Set<String> findTablesByTimeRange(DateHour theFrom, DateHour theTo) {

        Collection<ImportLog> aLogs = findByTimeRange(theFrom, theTo);

        Set<String> aTables = new HashSet<>();

        for (ImportLog aLog : aLogs) {
            aTables.add(aLog.tablename);
        }

        return aTables;
    }

    public Set<String> findFilesByTypeAndTimeRange(DateHour theFrom, DateHour theTo, String theType) {

        Collection<ImportLog> aLogs = findByTimeRange(theFrom, theTo);

        Set<String> aFiles = new HashSet<>();

        for (ImportLog aLog : aLogs) {
            if (aLog.tablename.equals(theType)) {
                aFiles.add(aLog.filename);
            }
        }

        return aFiles;
    }

    public void persist(List<ImportLog> theLogs, String theTimestampFormat) {

        try {
            // Could not get JPA batch inserts to work, do it this not so
            // beautiful way instead :(...

            if (theLogs.isEmpty()) {
                return;
            }

            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            SimpleDateFormat aFormat = new SimpleDateFormat(theTimestampFormat);

            List<String> aVals = new ArrayList<>();

            for (ImportLog aLog : theLogs) {
                String aVal = String.format("('%s', '%s', '%s')", aLog.filename, aLog.tablename, aFormat.format(aLog.timestamp));

                aVals.add(aVal);
            }

            String aSql = "insert into ogre_importlog values " + StringUtils.join(aVals, ", ");

            aManager.createNativeQuery(aSql).executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist new Ogre ImportLog row: " + theLogs, e);
        }
    }

    public int deleteByTimeRange(String theType, DateHour theFrom, DateHour theTo) {

        try {
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            // Add 1h to end to include it in search
            Calendar aTo = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aTo.setTime(theTo.getDateHour());
            aTo.add(Calendar.HOUR_OF_DAY, 1);

            return aManager.createNamedQuery("ImportLog.deleteByTypeAndTimestamp")
                    .setParameter("type", theType)
                    .setParameter("from", theFrom.getDateHour())
                    .setParameter("to", aTo.getTime())
                    .executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete rows in Ogre ImportLog. Type: " + theType + ", from: " + theFrom + ", to: " + theTo, e);
        }
    }

    public void deleteAllByType(String theType) {

        try {
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            aManager.createNamedQuery("ImportLog.deleteByType")
                    .setParameter("type", theType)
                    .executeUpdate();

        } catch (Exception e) {
            throw new RuntimeException("Failed to delete all rows in Ogre ImportLog. Type: " + theType, e);
        }
    }
}
