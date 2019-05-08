package com.ws.common.avrologging.writer.v2;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;


public class AvroRollingFileWriterTest {

    private static final int HOUR_MS = 60*60*1000;
    private static final int DAY_MS = 24*HOUR_MS;

    private static final String ROOT_DIR = "/tmp/AvroFilePosterTest/";


    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File(ROOT_DIR));
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(ROOT_DIR));
    }


    @Test
    public void testMaxCountRoll() throws Exception {

        String FILENAME = ROOT_DIR + "%t/d=%d{yyyy-MM-dd}/h=%d{HH}/%c{HHmmss}-%i.avro";
//        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/type=%t/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 10;     // Max number of records in a file
        int MAX_AGE_S = 1000;           // Max age of file, will never kick in
        int TOTAL_ROWS = 1000;          // Total number of records to write

        RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour + 10 mins
        long aTime = System.currentTimeMillis() /HOUR_MS *HOUR_MS + 10*60*1000;

        // Write some records (all within same hour)
        for (long i = 0; i < TOTAL_ROWS; i++) {

            GenericRecord anUser = AvroUserFactory.createUser("" + i, aTime);

            aWriter.write(aTime, anUser);

            // Forward time one second for every iteration
            aTime += 1000;
        }

        // Close all open files
        aWriter.close();


        // Read generated avro files and validate them.

        // Fetch all files under root dir
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        assertEquals("Wrong number of files written", TOTAL_ROWS/MAX_ROWS_PER_FILE, aFiles.size());

        Set<Integer> aNames = new HashSet<>();

        for (File aFile : aFiles) {

            int aRecords = 0;

            for (GenericRecord anUser : readEntries(aFile, GenericRecord.class)) {
                int aName = Integer.valueOf(AvroUserFactory.getName(anUser));
                long aTimestamp = AvroUserFactory.getTimestamp(anUser);


                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(aTimestamp), getHH(aTimestamp));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);

                aNames.add(aName);
                aRecords++;
            }

            assertEquals("Wrong number of records in file", MAX_ROWS_PER_FILE, aRecords);
        }

        assertEquals("Wrong number of records in total", TOTAL_ROWS, aNames.size());
    }

    @Test
    public void testHourRoll() throws Exception {

        // This has HH as smallest rolling date part
        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/type=%t/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 5000000;     // Max number of records in a file. This will never kick in.
        int MAX_AGE_S = 1000;                // Max age of a file. This will never kick in.
        int TOTAL_ROWS = 20;                 // Total number of records to write.

        RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /HOUR_MS *HOUR_MS;

        // Write one record per hour, every record should trigger a roll
        for (long i = 0; i < TOTAL_ROWS; i++) {

            GenericRecord anUser = AvroUserFactory.createUser("" + i, aTime);

            aWriter.write(aTime, anUser);

            // Forward time 1 hour for every iteration
            aTime += HOUR_MS;
        }

        // Close all open files
        aWriter.close();

        // Read generated avro files and validate them.

        // Fetch all files under root dir
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        // We should have one row per file
        assertEquals("Wrong number of files written", TOTAL_ROWS, aFiles.size());

        Set<Integer> aNames = new HashSet<>();

        for (File aFile : aFiles) {

            int aRecords = 0;

            for (GenericRecord anUser : readEntries(aFile, GenericRecord.class)) {
                int aName = Integer.valueOf(AvroUserFactory.getName(anUser));
                long aTimestamp = AvroUserFactory.getTimestamp(anUser);


                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user/%s-";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(aTimestamp), getHH(aTimestamp), getYyyyMMddHH(aStartTime));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);

                aNames.add(aName);
                aRecords++;
            }

            assertEquals("Wrong number of records in file", 1, aRecords);
        }

        assertEquals("Wrong number of records in total", TOTAL_ROWS, aNames.size());

    }

    @Test
    public void testAgeRoll() throws Exception {

        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/type=%t/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 10;     // Max number of records in a file
        int TOTAL_ROWS = 3;             // Total number of records to write
        int MAX_AGE_S = 1;              // Max age

        RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour + 10 mins
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /HOUR_MS *HOUR_MS + 10*60*1000;

        // Post object of every type (all into same hour)
        for (long i = 0; i < TOTAL_ROWS; i++) {

            GenericRecord anUser = AvroUserFactory.createUser("" + i, aTime);

            aWriter.write(aTime, anUser);

            // Forward time one second for every iteration
            aTime += 1500;

            // Sleep a while to let files roll over
            Thread.sleep(MAX_AGE_S*1000 + 500);
        }

        // Close all open files
        aWriter.close();


        // Read back generated avro files and validate them.

        // Fetch all files under root dir
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        // We should have one row per file
        assertEquals("Wrong number of files written", TOTAL_ROWS, aFiles.size());

        Set<Integer> aNames = new HashSet<>();

        for (File aFile : aFiles) {

            int aRecords = 0;

            for (GenericRecord anUser : readEntries(aFile, GenericRecord.class)) {
                int aName = Integer.valueOf(AvroUserFactory.getName(anUser));
                long aTimestamp = AvroUserFactory.getTimestamp(anUser);


                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user/%s-";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(aTimestamp), getHH(aTimestamp), getYyyyMMddHH(aStartTime));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);

                aNames.add(aName);
                aRecords++;
            }

            assertEquals("Wrong number of records in file", 1, aRecords);
        }

        assertEquals("Wrong number of records in total", TOTAL_ROWS, aNames.size());

    }

    @Test
    public void testDateRoll() throws Exception {

        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 5000000;     // Max number of records in a file. This will never kick in.
        int MAX_AGE_S = 1000;                // Max age of a file. This will never kick in.
        int TOTAL_ROWS = 24*40;              // Total number of records to write.

        RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current date
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /DAY_MS *DAY_MS;

        // Write one record per hour, they should span 40 days = number of files written
        for (long i = 0; i < TOTAL_ROWS; i++) {

            GenericRecord anUser = AvroUserFactory.createUser("" + i, aTime);

            aWriter.write(aTime, anUser);

            // Forward time 1 hour for every iteration
            aTime += HOUR_MS;
        }

        // Close all open files
        aWriter.close();

        // Read back generated avro files and validate them.

        // Fetch all files under root dir
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        // We should have one row per file
        assertEquals("Wrong number of files written", TOTAL_ROWS/24, aFiles.size());

        Set<Integer> aNames = new HashSet<>();

        for (File aFile : aFiles) {

            int aRecords = 0;

            for (GenericRecord anUser : readEntries(aFile, GenericRecord.class)) {
                int aName = Integer.valueOf(AvroUserFactory.getName(anUser));
                long aTimestamp = AvroUserFactory.getTimestamp(anUser);


                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/%s-";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(aTimestamp), getYyyyMMddHH(aStartTime));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);

                aNames.add(aName);
                aRecords++;
            }

            assertEquals("Wrong number of records in file", 24, aRecords);
        }

        assertEquals("Wrong number of records in total", TOTAL_ROWS, aNames.size());

    }

    @Test
    public void testProgressAndCleanup() throws Exception {

        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 5000000;     // Max number of records in a file. This will never kick in.
        int MAX_AGE_S = 1;                   // Max age of a file. This will never kick in.
        int TOTAL_ROWS = 24*40;              // Total number of records to write.

        RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current date
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /DAY_MS *DAY_MS;

        // Write one record per hour, they should span 40 days = number of files written
        for (long i = 0; i < TOTAL_ROWS; i++) {

            GenericRecord anUser = AvroUserFactory.createUser("" + i, aTime);

            aWriter.write(aTime, anUser);

            // Forward time 1 hour for every iteration
            aTime += HOUR_MS;
        }

        // Fetch all files under root dir, all should be in progress
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        for (File aFile : aFiles) {
            assertTrue("File do not end with .progress: " + aFile.getAbsolutePath(), aFile.getAbsolutePath().endsWith(".progress"));
        }

        assertEquals("Wrong number of files written", TOTAL_ROWS, aFiles.size());

        // Wait for all to be closed
        Thread.sleep(1000+15000);

        // Fetch all files under root dir, none should be in progress
        aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        for (File aFile : aFiles) {
            assertTrue("File should not end with .progress: " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith(".progress"));
        }

        assertEquals("Wrong number of files written", TOTAL_ROWS, aFiles.size());

        // Close all open files
        aWriter.close();

    }

    @Test
    public void testThreaded() throws Exception {

        // A more evil test launching up a number of threads writing records from a testplan concurrently to the same
        // writer.

        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/%c{yyyyMMddHH}-%i.avro";

        int DAYS = 10;                      // Number of days to spread records over
        int RECORDS_PER_HOUR = 1000;        // The number of records to write in each hour
        int MAX_RECORDS_PER_FILE = 100;     // The number of records to write into same file
        int MAX_AGE_S = 1000;               // Will never kick in
        int THREADS = 4;                   // The number of threads to race

        // Today date
        long aTime = System.currentTimeMillis() /DAY_MS *DAY_MS;

        // All records in testplan
        final TestRecords allRecords = new TestRecords();

        System.out.println("Creating testplan...");

        int anId = 0;

        // Create a test plan, spread the desired records out evenly over test range
        for (int d = 0; d < DAYS; d++) {

            for (int j = 0; j < 24; j++) {

                // Calc the folder where record container file is written
                String aFilePartPattern = ROOT_DIR + "date=%s/hour=%s/";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(aTime), getHH(aTime));

                for (int i = 0; i < RECORDS_PER_HOUR; i++) {

                    TestRecord aRecord = new TestRecord();
                    aRecord.id = anId++;
                    aRecord.timestamp = aTime;
                    aRecord.pathPart = aFilePart;

                    allRecords.add(aRecord);

                    aTime += HOUR_MS/RECORDS_PER_HOUR;
                }
            }
        }

        System.out.println("Testplan records: " + anId);

        // Create writer
        final RollingFileWriter<GenericRecord> aWriter = new AvroRollingFileWriter<>(AvroUserFactory.SCHEMA, FILENAME, ".progress", MAX_RECORDS_PER_FILE, MAX_AGE_S);


        List<Thread> aThreads = new ArrayList<>();

        // Runnable writing all records in testplan in random order
        for (int i = 0; i < THREADS; i++) {

            Thread aThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {

                        List<TestRecord> aRnd = new ArrayList<>(allRecords);

                        Collections.shuffle(aRnd);

                        for (TestRecord aRecord : aRnd) {

                            GenericRecord anUser = AvroUserFactory.createUser("" + aRecord.id, aRecord.timestamp, aRecord.pathPart);

                            aWriter.write(aRecord.timestamp, anUser);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            aThread.start();
            aThreads.add(aThread);
        }

        for (Thread aThread : aThreads) {
            aThread.join();
        }

        aWriter.close();

        // Read back generated avro files and validate them.

        // Fetch all files under root dir
        Collection<File> aFiles = FileUtils.listFiles(new File(ROOT_DIR), null, true);

        assertEquals("Wrong number of files written", THREADS * allRecords.size() / MAX_RECORDS_PER_FILE, aFiles.size());

        Set<Integer> aNames = new HashSet<>();

        for (File aFile : aFiles) {

            int aRecords = 0;

            for (GenericRecord anUser : readEntries(aFile, GenericRecord.class)) {
                int aName = Integer.valueOf(AvroUserFactory.getName(anUser));
                String aPart = AvroUserFactory.getFilePart(anUser);
                long aTimestamp = AvroUserFactory.getTimestamp(anUser);

                assertTrue("Wrong path, it should contain " + aPart + ", it was " + aFile.getAbsolutePath() + ", ts=" + getYyyyMMddHHmmss(aTimestamp) + ": " + anUser, aFile.getAbsolutePath().startsWith(aPart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < allRecords.size());

                aNames.add(aName);
                aRecords++;
            }

            assertEquals("Wrong number of records in file", MAX_RECORDS_PER_FILE, aRecords);
        }

        assertEquals("Wrong number of records in total", allRecords.size(), aNames.size());

    }

    private class TestRecords extends ArrayList<TestRecord> {}

    private class TestRecord {
        int id;
        long timestamp;
        String pathPart;
    }







        // Deserialize all records in file
    private <T> List<T> readEntries(File theFile, Class<T> theType) throws Exception {

        List<T> aList = new ArrayList<>();

        DatumReader<T> aDatumReader = new GenericDatumReader<>();

        DataFileReader<T> aFileReader = new DataFileReader<>(theFile, aDatumReader);

        while (aFileReader.hasNext()) {
            aList.add(aFileReader.next());
        }

        return aList;
    }


    private <T> List<T> toList(T... theItems) {
        return new ArrayList<>(Arrays.asList(theItems));
    }

    private static String getHH(long theTimestamp) {
        SimpleDateFormat aFormat = new SimpleDateFormat("HH");
        aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return aFormat.format(new Date(theTimestamp));
    }

    private static String getYyyyMMdd(long theTimestamp) {
        SimpleDateFormat aFormat = new SimpleDateFormat("yyyy-MM-dd");
        aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return aFormat.format(new Date(theTimestamp));
    }

    private static String getYyyyMMddHH(long theTimestamp) {
        SimpleDateFormat aFormat = new SimpleDateFormat("yyyyMMddHH");
        aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return aFormat.format(new Date(theTimestamp));
    }

    private static String getYyyyMMddHHmmss(long theTimestamp) {
        SimpleDateFormat aFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        aFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return aFormat.format(new Date(theTimestamp));
    }

    private void assertApproxiate(String theMessage, int theExpected, int theActual, int theDiff) {
        if (Math.abs(theExpected - theActual) > theDiff) {
            fail(theMessage + " [Expected: " + theExpected + ", actual: " + theActual + ", allowed diff: " + theDiff + "]");
        }
    }

    private static class AvroUserFactory {

        private static String SCHEMA_STRING =
                "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"timestamp\", \"type\": \"long\"},\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"filepart\",  \"type\": [\"string\", \"null\"]},\n" +
                "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
                " ]\n" +
                "}";

        public static Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

        public static GenericRecord createUser(String theName, long theTimestamp, String theFilePart) {
            GenericRecord anUser = new GenericData.Record(SCHEMA);
            anUser.put("name", theName);
            anUser.put("timestamp", theTimestamp);
            anUser.put("filepart", theFilePart);
            return anUser;
        }

        public static GenericRecord createUser(String theName, long theTimestamp) {
            return createUser(theName, theTimestamp, null);
        }

        public static String getName(GenericRecord theRecord) {
            return theRecord.get("name").toString();
        }

        public static String getFilePart(GenericRecord theRecord) {
            return theRecord.get("filepart").toString();
        }

        public static long getTimestamp(GenericRecord theRecord) {
            return (long) theRecord.get("timestamp");
        }


    }

}
