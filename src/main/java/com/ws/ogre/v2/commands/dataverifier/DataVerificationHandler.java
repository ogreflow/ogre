package com.ws.ogre.v2.commands.dataverifier;

import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import com.ws.ogre.v2.db.SqlScript;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;


public class DataVerificationHandler {

    private enum MismatchType {
        MISMATCH_FOR_MISSING_IN_REF, MISMATCH_FOR_MISSING_IN_TEST, MISMATCH_FOR_COLUMN_VALUE_DIFF, MISMATCH_FOR_WHOLE_SAMPLE;
    }

    private static final Logger ourLogger = Logger.getLogger();

    private final List<SingleVerificationDetail> myVerificationDetails;
    private final boolean iWillSuppressAlert;

    private final Map<MismatchType, MismatchStat> myLoggedMismatchCount = new HashMap<>();
    private final Map<Integer, Integer> myMismatchedRowCount = new HashMap<>();
    private final Map<String, String> myVars = new HashMap<>();

    private final JdbcDbHandler myRefDbHandler;
    private final JdbcDbHandler myTestDbHandler;

    public DataVerificationHandler(Config theConfig, boolean theIsToSuppressAlert) {
        iWillSuppressAlert = theIsToSuppressAlert;
        myVerificationDetails = theConfig.verificationDetails;
        myRefDbHandler = JdbcDbHandlerBuilder.getInstance().buildJdbcDbHandler(theConfig.getRefDbConfig());
        myTestDbHandler = JdbcDbHandlerBuilder.getInstance().buildJdbcDbHandler(theConfig.getTestDbConfig());
    }

    public void verify(DateHour.Range theTimeRange, DateHour.Range.Chunking theChunking, Set<String> theTypes) {
        ourLogger.info("Run verification for '%s' with chunking '%s'", theTimeRange, theChunking);

        // Calc chunks
        DateHour.Ranges aChunks = theTimeRange.getChunkedRanges(theChunking);

        // Load data chunk by chunks
        for (DateHour.Range aChunk : aChunks) {
            verifyChunk(aChunk, theTypes);
        }

        ourLogger.info("DONE verification for '%s' with chunking '%s'", theTimeRange, theChunking);
    }

    private void verifyChunk(DateHour.Range aChunk, Set<String> theTypes) {
        List<SingleVerificationDetail> someTypesToVerify = getTypesToVerify(theTypes);
        if (CollectionUtils.isEmpty(someTypesToVerify)) {
            ourLogger.warn("Nothing to verify.");
            return;
        }

        for (SingleVerificationDetail aDetail : someTypesToVerify) {
            try {
                ourLogger.info("Run verification for the chunk %s for '%s'", aChunk, aDetail.getName());
                initForQueryAndMatch(aChunk);
                doQueryAndMatchResult(aDetail, aChunk);

            } catch (Exception e) {
                logAlert("Unable to verify for '%s' for chunk '%s'", aDetail.getName(), aChunk, e);
            }
        }
    }

    private List<SingleVerificationDetail> getTypesToVerify(Set<String> theTypes) {
        if (CollectionUtils.isEmpty(theTypes)) {
            return myVerificationDetails;
        }

        List<SingleVerificationDetail> someDetails = new ArrayList<>();
        for (String aType : theTypes) {
            SingleVerificationDetail aDetail = findType(aType);
            if (aDetail != null) {
                someDetails.add(aDetail);
            }
        }

        return someDetails;
    }

    private SingleVerificationDetail findType(String theType) {
        for (SingleVerificationDetail aDetail : myVerificationDetails) {
            if (StringUtils.equals(aDetail.getName(), theType)) {
                return aDetail;
            }
        }

        return null;
    }

    private void initForQueryAndMatch(DateHour.Range theChunk) {
        myVars.clear();

        myVars.putAll(System.getenv());

        // Make time range available to the query. Send year, month day separately in case we want to read from partition.

        DateHour aFromHour = theChunk.getFrom();
        myVars.put("from", aFromHour.format("yyyy-MM-dd HH:mm:ss"));
        myVars.put("fromDate", aFromHour.format("yyyy-MM-dd"));
        myVars.put("fromYear", aFromHour.format("yyyy"));
        myVars.put("fromMonth", aFromHour.format("MM"));
        myVars.put("fromDay", aFromHour.format("dd"));
        myVars.put("fromHour", aFromHour.format("HH"));

        DateHour aToHour = theChunk.getTo().getNextDateHour();
        myVars.put("before", aToHour.format("yyyy-MM-dd HH:mm:ss")); // Exclusive.
        myVars.put("to", aToHour.format("yyyy-MM-dd HH:mm:ss")); // Exclusive. Legacy. Do not use it. Use 'before'. 'to' doesn't sound exclusive.
        myVars.put("beforeDate", aToHour.format("yyyy-MM-dd")); // Exclusive.
        myVars.put("beforeYear", aToHour.format("yyyy"));
        myVars.put("beforeMonth", aToHour.format("MM"));
        myVars.put("beforeDay", aToHour.format("dd"));
        myVars.put("beforeHour", aToHour.format("HH"));

        myVars.put("now", new DateHour(new Date()).format("yyyy-MM-dd HH:mm:ss"));
        myVars.put("nowDate", new DateHour(new Date()).format("yyyy-MM-dd"));
        myVars.put("nowYear", new DateHour(new Date()).format("yyyy"));
        myVars.put("nowMonth", new DateHour(new Date()).format("MM"));
        myVars.put("nowDay", new DateHour(new Date()).format("dd"));
        myVars.put("nowHour", new DateHour(new Date()).format("HH"));

        myLoggedMismatchCount.clear();
        myMismatchedRowCount.clear();
    }

    private void doQueryAndMatchResult(final SingleVerificationDetail theDetail, final DateHour.Range theChunk) throws Exception {
        SqlScript aRefScript = new SqlScript(theDetail.getRefSql(), myVars);
        final SqlScript aTestScript = new SqlScript(theDetail.getTestSql(), myVars);

        // => Ref storage's query.
        ourLogger.info("Execute (ref): %s", aRefScript.getQuerySql());
        myRefDbHandler.query(aRefScript.getQuerySql(), (ResultSet theRefResultSet) -> {
            // => Test storage's query.
            ourLogger.info("Execute (test): %s", aTestScript.getQuerySql());
            myTestDbHandler.query(aTestScript.getQuerySql(), (ResultSet theTestResultSet) -> {
                matchResultSet(theDetail, theChunk, theRefResultSet, theTestResultSet);
                theTestResultSet.close();
            });

            theRefResultSet.close();
        });
    }

    private void matchResultSet(SingleVerificationDetail theDetail, DateHour.Range theChunk, ResultSet theRefResultSet, ResultSet theTestResultSet) throws SQLException {
        ResultSetMetaData aRefMetaData = theRefResultSet.getMetaData();
        ResultSetMetaData aTestMetaData = theTestResultSet.getMetaData();

        if (aRefMetaData.getColumnCount() != aTestMetaData.getColumnCount()) {
            logMismatch(theDetail, MismatchType.MISMATCH_FOR_COLUMN_VALUE_DIFF, "Ref table column count (" + aRefMetaData.getColumnCount() + ") != test table column count (" + aTestMetaData.getColumnCount() + ").");
            return;
        }

        int aRefRowsCount = 0;
        int aTestRowsCount = 0;

        while (theRefResultSet.next()) {
            aRefRowsCount++;

            if (!theTestResultSet.next()) {
                logMismatch(theDetail, MismatchType.MISMATCH_FOR_MISSING_IN_TEST, "Missing in TEST table: " + getColumnValuesCSVForLogging(theRefResultSet));
                continue;
            }

            aTestRowsCount++;

            List<String> someColumnMismatches = getColumnMismatches(theRefResultSet, theTestResultSet, theDetail.getTolerances());
            if (someColumnMismatches.size() > 0) {
                logMismatch(theDetail, MismatchType.MISMATCH_FOR_COLUMN_VALUE_DIFF, StringUtils.join(someColumnMismatches, ", "));
            }
        }

        while (theTestResultSet.next()) {
            aTestRowsCount++;
            logMismatch(theDetail, MismatchType.MISMATCH_FOR_MISSING_IN_REF, "Missing in REF table: " + getColumnValuesCSVForLogging(theTestResultSet));
        }

        // For those tolerance type that checks in whole sample in an aggregated fashion.
        if (myMismatchedRowCount.size() > 0) {
            final int aTotalRowsCount = aTestRowsCount; /* Lambda needs final variable. */

            myMismatchedRowCount.entrySet().stream().forEach(anEntry -> {
                MismatchTolerance aTolerance = theDetail.getTolerances().get(anEntry.getKey());
                int aMismatchCount = anEntry.getValue();

                ourLogger.info("Checking tolerance in whole dataset: %s. Total: %s, mismatched: %s", aTolerance, aTotalRowsCount, aMismatchCount);
                if (!aTolerance.isEqualDataInWholeSample(aTotalRowsCount, aMismatchCount)) {
                    logMismatch(theDetail, MismatchType.MISMATCH_FOR_WHOLE_SAMPLE, "Mismatch is " + aMismatchCount + "/" + aTotalRowsCount + " which exceeds tolerance " + aTolerance.getAsString());
                }
            });
        }

        if (myLoggedMismatchCount.size() <= 0) {
            ourLogger.info("No mismatch found for: %s :)", theDetail.getName());
            ourLogger.info(":)"); // A new small line for making log file a but nicer.
            return;
        }

        logAlert("Mismatch found for: %s for chunk '%s'. See details in log file. Summary:<br>\n" +
                        " Total rows in REF:  %s <br>\n" +
                        "Total rows in TEST:  %s <br>\n<br>\n" +
                        " Whole sample mismatch in TEST:  %s\n" +
                        "               Missing in TEST:  %s\n" +
                        "                 Extra in TEST:  %s\n" +
                        "              Mismatch in TEST:  %s\n",

                theDetail.getName(), theChunk, aRefRowsCount, aTestRowsCount,
                getMismatchStat(MismatchType.MISMATCH_FOR_WHOLE_SAMPLE),
                getMismatchStat(MismatchType.MISMATCH_FOR_MISSING_IN_TEST),
                getMismatchStat(MismatchType.MISMATCH_FOR_MISSING_IN_REF),
                getMismatchStat(MismatchType.MISMATCH_FOR_COLUMN_VALUE_DIFF)
        );
    }

    private List<String> getColumnMismatches(ResultSet theRefResultSet, ResultSet theTestResultSet, List<MismatchTolerance> theTolerances) throws SQLException {
        List<String> someColumnMismatches = new ArrayList<>();

        ResultSetMetaData aRefMetaData = theRefResultSet.getMetaData();
        ResultSetMetaData aTestMetaData = theTestResultSet.getMetaData();

        for (int i = 1; i <= aRefMetaData.getColumnCount(); i++) {
            String aRefValue = theRefResultSet.getString(i);
            String aTestValue = theTestResultSet.getString(i);
            int toleranceIndex = i - 1;

            MismatchTolerance aTolerance = (i >= 1 && i <= theTolerances.size()) ? theTolerances.get(toleranceIndex) : MismatchTolerance.ZERO_TOLERANCE;

            if (!aTolerance.isEqualData(aRefMetaData.getColumnType(i), aRefValue, aTestValue)) {
                if (aTolerance.isForWholeSample()) {
                    // For whole sample tolerance, just keep the count.
                    logMismatchForWholeSample(toleranceIndex);

                } else {
                    someColumnMismatches.add(
                            "" +
                                    String.format("%30s", (aRefMetaData.getColumnName(i) + "#" + aRefValue)) + "  !=  " +
                                    String.format("%30s", (aTestMetaData.getColumnName(i) + "#" + aTestValue)) +
                                    " (+/- " + aTolerance.getAsString() + ")"
                    );
                }
            }
        }

        return someColumnMismatches;
    }

    private String getColumnValuesCSVForLogging(ResultSet theResultSet) throws SQLException {
        List<String> someValues = new ArrayList<>();

        ResultSetMetaData aMetaData = theResultSet.getMetaData();
        for (int i = 1; i <= aMetaData.getColumnCount(); i++) {
            someValues.add(aMetaData.getColumnName(i) + "=" + theResultSet.getString(i));
        }

        return StringUtils.join(someValues, ", ");
    }

    private void logMismatch(SingleVerificationDetail theDetail, MismatchType theMismatchType, String theMismatch) {
        MismatchStat aStat = getMismatchStat(theMismatchType);
        myLoggedMismatchCount.put(theMismatchType, aStat);

        aStat.increment(); // 1 more mismatch.
        aStat.addMessage("[ " + theDetail.getName() + " ] " + theMismatch);
    }

    private MismatchStat getMismatchStat(MismatchType theMismatchType) {
        MismatchStat aStat = myLoggedMismatchCount.get(theMismatchType);
        return aStat == null ? new MismatchStat() : aStat;
    }

    private void logMismatchForWholeSample(int theColumnIndex) {
        Integer aCount = myMismatchedRowCount.get(theColumnIndex);
        if (aCount == null) {
            aCount = 0;
        }

        myMismatchedRowCount.put(theColumnIndex, aCount + 1);
    }

    private void logAlert(String theMessage, Object... theParams) {
        if (iWillSuppressAlert) {
            ourLogger.warn(theMessage, theParams);
        } else {
            Alert.getAlert().alert(theMessage, theParams);
        }
    }

    private class MismatchStat {
        private static final int MAX_MISMATCH_TO_DISPLAY = 20;

        private int myCount = 0;
        private List<String> myMessages = new ArrayList<>();

        public void increment() {
            myCount++;
        }

        public void addMessage(String theMessage) {
            // Do not flood logs when too many differences.
            if (myCount < MAX_MISMATCH_TO_DISPLAY) {
                myMessages.add(theMessage);
            }
        }

        @Override
        public String toString() {
            return myCount + " rows. " + myMessages.size() + " of them are: <br>\n" + StringUtils.join(myMessages, "\n");
        }
    }
}
