package com.ws.ogre.v2.commands.data2redshift;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.commands.data2redshift.db.DbHandler;
import com.ws.ogre.v2.commands.data2redshift.db.RedShiftDao;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.utils.StopWatch;

import java.util.*;

/**
 */
public class AnalyseHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private PartitionHandler myPartitionHandler;
    private Timer myAnalyseTimer;
    private TimerTask myAnalyseTask;

    private Multimap<String, String> myLoaded = ArrayListMultimap.create();

    private Set<String> myToAnalyse = new HashSet<>();

    public AnalyseHandler(PartitionHandler thePartitionHandler) {
        myPartitionHandler = thePartitionHandler;

        myAnalyseTask = new BgAnalyser();
        myAnalyseTimer = new Timer(true);
        myAnalyseTimer.schedule(myAnalyseTask, 60000, 60000);
    }

    public void loaded(String theType, DateHour theHour) {

        String aTable = myPartitionHandler.getPartitionTable(theType, theHour);

        myLoaded.put(theType, aTable);

        Collection<String> aLoadedTables = myLoaded.get(theType);

        // Either analyse a table if a new partition is used or if more than 24 loads been committed

        // First check for old partitions to analyse
        for (String aLoaded : new HashSet<>(aLoadedTables)) {
            if (!aLoaded.equals(aTable)) {
                ourLogger.info("Add %s for ANALYZE", aLoaded);
                myToAnalyse.add(aLoaded);
                myLoaded.remove(theType, aLoaded);
            }
        }

        // Then check if more that 24 loads since last analyse...
        if (myLoaded.get(theType).size() > 26) {
            String aToAnalyse = myLoaded.get(theType).iterator().next();

            ourLogger.info("Add %s for ANALYZE", aToAnalyse);
            myToAnalyse.add(aToAnalyse);
            myLoaded.removeAll(theType);
        }
    }

    public void stopAnalyse() {
        myAnalyseTask.cancel();
        myAnalyseTimer.cancel();

        // As we are stopping and we do not know whether the task ran once or not. Lets run for very last time.
        myAnalyseTask.run();
    }

    private class BgAnalyser extends TimerTask {
        @Override
        public void run() {

            ourLogger.debug("Check for tables to analyse...");

            Collection<String> aToAnalyse = new ArrayList<>(myToAnalyse);

            myToAnalyse.clear();

            for (String aTable : aToAnalyse) {
                analyse(aTable);
            }
        }

        private void analyse(String theTable) {
            try {
                DbHandler.getInstance().beginTransaction();

                ourLogger.info("Analyse table %s", theTable);

                StopWatch aWatch = new StopWatch();

                RedShiftDao.getInstance().analyze(theTable);

                ourLogger.info("Analyse table %s took %s", theTable, aWatch.getAndReset());

                DbHandler.getInstance().commitTransaction();

            } catch (Exception e) {
                ourLogger.info("Failed to Analyse table %s: %s", theTable, e.getMessage());
                DbHandler.getInstance().rollbackTransaction();
            }
        }
    }


}
