package com.ws.common.avrologging.writer.v2;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;


public class ReflectionRollingFileWriterTest {

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

        String FILENAME = ROOT_DIR + "date=%d{yyyy-MM-dd}/hour=%d{HH}/type=%t/%c{yyyyMMddHH}-%i.avro";

        int MAX_ROWS_PER_FILE = 10;     // Max number of records in a file
        int MAX_AGE_S = 1000;           // Max age of file, will never kick in
        int TOTAL_ROWS = 1000;          // Total number of records to write

        RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour + 10 mins
        long aTime = System.currentTimeMillis() /HOUR_MS *HOUR_MS + 10*60*1000;

        // Write some records (all within same hour)
        for (long i = 0; i < TOTAL_ROWS; i++) {

            User anUser = new User("" + i, (int) i, aTime).setLanguages("en", "sv").setAddress("apa", "banan");

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

            for (User anUser : readEntries(aFile, User.class)) {

                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(anUser.timestamp), getHH(anUser.timestamp));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                int aName = Integer.parseInt(anUser.name);
                Arrays.sort(anUser.languages);

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);
                assertEquals(anUser.toString(), anUser.age, aName);
                assertEquals(anUser.toString(), "en", anUser.languages[0]);
                assertEquals(anUser.toString(), "sv", anUser.languages[1]);
                assertEquals(anUser.toString(), "apa", anUser.address.street);
                assertEquals(anUser.toString(), "banan", anUser.address.zip);

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

        RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /HOUR_MS *HOUR_MS;

        // Write one record per hour, every record should trigger a roll
        for (long i = 0; i < TOTAL_ROWS; i++) {

            User anUser = new User("" + i, (int) i, aTime).setLanguages("en", "sv").setAddress("apa", "banan");

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

            for (User anUser : readEntries(aFile, User.class)) {

                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(anUser.timestamp), getHH(anUser.timestamp));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                int aName = Integer.parseInt(anUser.name);
                Arrays.sort(anUser.languages);

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);
                assertEquals(anUser.toString(), anUser.age, aName);
                assertEquals(anUser.toString(), "en", anUser.languages[0]);
                assertEquals(anUser.toString(), "sv", anUser.languages[1]);
                assertEquals(anUser.toString(), "apa", anUser.address.street);
                assertEquals(anUser.toString(), "banan", anUser.address.zip);

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

        RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current hour + 10 mins
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /HOUR_MS *HOUR_MS + 10*60*1000;

        // Post object of every type (all into same hour)
        for (long i = 0; i < TOTAL_ROWS; i++) {

            User anUser = new User("" + i, (int) i, aTime).setLanguages("en", "sv").setAddress("apa", "banan");

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

            for (User anUser : readEntries(aFile, User.class)) {

                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/hour=%s/type=user";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(anUser.timestamp), getHH(anUser.timestamp));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                int aName = Integer.parseInt(anUser.name);
                Arrays.sort(anUser.languages);

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);
                assertEquals(anUser.toString(), anUser.age, aName);
                assertEquals(anUser.toString(), "en", anUser.languages[0]);
                assertEquals(anUser.toString(), "sv", anUser.languages[1]);
                assertEquals(anUser.toString(), "apa", anUser.address.street);
                assertEquals(anUser.toString(), "banan", anUser.address.zip);

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

        RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current date
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /DAY_MS *DAY_MS;

        // Write one record per hour, they should span 40 days = number of files written
        for (long i = 0; i < TOTAL_ROWS; i++) {

            User anUser = new User("" + i, (int) i, aTime).setLanguages("en", "sv").setAddress("apa", "banan");

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

            for (User anUser : readEntries(aFile, User.class)) {

                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/%s-";
                String aFilePart = String.format(aFilePartPattern, getYyyyMMdd(anUser.timestamp), getYyyyMMddHH(aStartTime));

                assertTrue("Wrong path, it should contain " + aFilePart + ", it was " + aFile.getAbsolutePath(), aFile.getAbsolutePath().contains(aFilePart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                int aName = Integer.parseInt(anUser.name);
                Arrays.sort(anUser.languages);

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < TOTAL_ROWS);
                assertEquals(anUser.toString(), anUser.age, aName);
                assertEquals(anUser.toString(), "en", anUser.languages[0]);
                assertEquals(anUser.toString(), "sv", anUser.languages[1]);
                assertEquals(anUser.toString(), "apa", anUser.address.street);
                assertEquals(anUser.toString(), "banan", anUser.address.zip);

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

        RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_ROWS_PER_FILE, MAX_AGE_S);

        // Calc current date
        long aStartTime = System.currentTimeMillis();
        long aTime = aStartTime /DAY_MS *DAY_MS;

        // Write one record per hour, they should span 40 days = number of files written
        for (long i = 0; i < TOTAL_ROWS; i++) {

            User anUser = new User("" + i, (int) i, aTime).setLanguages("en", "sv").setAddress("apa", "banan");

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
        final RollingFileWriter<User> aWriter = new ReflectionRollingFileWriter<>(User.class, FILENAME, ".progress", MAX_RECORDS_PER_FILE, MAX_AGE_S);


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

                            User anUser = new User("" + aRecord.id, 22, aRecord.timestamp).setFilepart(aRecord.pathPart).setLanguages("en", "sv").setAddress("apa", "banan");

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

            for (User anUser : readEntries(aFile, User.class)) {

                // Check that %d was correctly formatted
                String aFilePartPattern = "/date=%s/%s-";

                assertTrue("Wrong path, it should contain " + anUser.filepart + ", it was " + aFile.getAbsolutePath() + ", ts=" + getYyyyMMddHHmmss(anUser.timestamp) + ": " + anUser, aFile.getAbsolutePath().startsWith(anUser.filepart));
                assertTrue("Wrong path, it was " + aFile.getAbsolutePath(), !aFile.getAbsolutePath().endsWith("progress"));

                int aName = Integer.parseInt(anUser.name);
                Arrays.sort(anUser.languages);

                assertTrue("Wrong name, was " + aName, aName >= 0 && aName < allRecords.size());
                assertEquals(anUser.toString(), 22, anUser.age);
                assertEquals(anUser.toString(), "en", anUser.languages[0]);
                assertEquals(anUser.toString(), "sv", anUser.languages[1]);
                assertEquals(anUser.toString(), "apa", anUser.address.street);
                assertEquals(anUser.toString(), "banan", anUser.address.zip);

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

        DatumReader<T> aDatumReader = new ReflectDatumReader<>(theType);

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

    public static class User {
        public String name;
        public int age;
        public String[] languages;
        public Address address;
        @Nullable
        public String filepart;

        public long timestamp;

        public User() {
        }

        public User(String theName, int theAge, long theTimestamp) {
            name = theName;
            age = theAge;
            timestamp = theTimestamp;
        }

        public User setLanguages(String ... theLanguages) {
            languages = theLanguages;
            return this;
        }

        public User setFilepart(String theFilepart) {
            filepart = theFilepart;
            return this;
        }

        public User setAddress(String theStreet, String theZip) {
            address = new Address(theStreet, theZip);
            return this;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", languages=" + Arrays.toString(languages) +
                    ", address=" + address +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static class Address {
        public String street;
        public String zip;

        public Address() {
        }

        public Address(String theStreet, String theZip) {
            street = theStreet;
            zip = theZip;
        }

        @Override
        public String toString() {
            return "Address{" +
                    "street='" + street + '\'' +
                    ", zip='" + zip + '\'' +
                    '}';
        }
    }
}
