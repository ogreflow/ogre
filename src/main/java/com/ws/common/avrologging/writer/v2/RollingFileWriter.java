package com.ws.common.avrologging.writer.v2;

import com.ws.common.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Class writing Avro records to a rolling file. The file can only handle records of the same typ and files will be rolled
 * depending on:
 *
 * - The filename datetime patterns.
 * - The max records limit
 * - The file max age limit
 *
 * This class is initialized with a theFilenamePattern. This parameter defines in which directory and Avro container file
 * where to write the records. The pattern can contain tokens that is replaced with dynamic values. Following tokens are
 * available:
 *
 * - %d{datetime pattern}  Replaced with the result from SimpleDateFormat processing pattern for the RECORD timestamp
 * - %c{datetime pattern}  Replaced with the result from SimpleDateFormat processing pattern for the FILE CREATION timestamp
 * - %t                    Replaced with the AVRO type name (default: Schema.getName()) in lower case
 * - %T                    Replaced with the AVRO type name (default: Schema.getName())
 * - %i                    Replaced by a random 16 letter hex string
 *
 * NOTE: %d and %c date and times will be formatted and written in UTC.
 *
 * NOTE: %d{..} is replaced with a dynamic timestamp and may cause rolling over to a new file while %c{..} is a static
 *       timestamp replaced by the file creation date and will not cause any rolling to new files.
 *
 * NOTE: %i is mandatory and is used to avoid clashes between rolled files and also to files generated in other cluster
 *       nodes.
 *
 *
 * E.g. This filename pattern:
 *
 *     /x/y/z/date=%d{yyyy-MM-dd}/hour=%d{HH}/type=%t/%c{yyyyMMddHHmm}-%i.avro
 *
 *     ...will result in a record with the timestamp 2015-05-15 10:22:20 UTC to be written into:
 *
 *     /x/y/z/date=2015-05-15/hour=10/type=delivery/201505151010-a1b3c5d6e7.avro
 *
 *     ...asuming the file's creation date was 2015-05-15 10:10 UTC and a1b3c5d6e7 is a random value to avoid clashes
 *     among nodes in a cluster.
 *
 * The files will be rolled when any of the conditions below is met:
 *
 * - The record timestamp passed the most frequent changing %d datetime pattern component, in example above the file is
 *   rolled every hour since HH is changed more frequently than yyyy-MM-dd.
 * - The max numbers of records limit is passed.
 * - The fil max age been passed.
 *
 * This class can also be initialized with an optional theProgressPostfix that is used for files currently in progress,
 * it will be removed for rolled files. This is a useful marker to avoid shipping unfinished files somewhere else before
 * they are closed.
 */
public abstract class RollingFileWriter<T> extends AvroWriter<T> {

    private static final Logger ourLog = Logger.getLogger();

    private String myFilenamePattern;
    private String myProgressPostfix;
    private Integer myMaxRecords;
    private Integer myMaxAge;

    private FastDateFormat myHashFormat;

    private Writers myWriters = new Writers();

    private Timer myTimer = new Timer(true);

    /**
     * Create a new rolling Avro file writer.
     *
     * @param theFilenamePattern The file name pattern containing replaceable tokens
     * @param theProgressPostfix The file name postfix to use for files currently in progress
     * @param theMaxRecords      The max records to write before rolling file
     * @param theMaxAgeS         The max age in seconds before rolling file
     */
    public RollingFileWriter(String theFilenamePattern, String theProgressPostfix, int theMaxRecords, int theMaxAgeS) throws IOException {

        myFilenamePattern = theFilenamePattern;
        myProgressPostfix = theProgressPostfix != null ? theProgressPostfix : "";
        myMaxRecords = theMaxRecords;
        myMaxAge = theMaxAgeS*1000;

        if (myMaxRecords < 0) {
            throw new IllegalArgumentException("Invalid Max Records");
        }

        if (myMaxAge < 0) {
            throw new IllegalArgumentException("Invalid Max Time");
        }

        if (theFilenamePattern == null) {
            throw new IllegalArgumentException("No filename");
        }

        if (!theFilenamePattern.contains("%i")) {
            throw new IllegalArgumentException("Filename must contain %i");
        }

        // Create a formatter for fast hash generation of filenames
        myHashFormat = getHashFormat(myFilenamePattern);

        // Create a cleanup timer
        myTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                onCleanup();
            }
        }, 15000, 15000);
    }

    /**
     * Closing all underlying writers.
     */
    public void close() {

        ourLog.info("Close writers");

        myTimer.cancel();

        for (Writer aWriter : myWriters.values()) {
            aWriter.close();
        }

        myWriters.clear();
    }

    /**
     * Writes record to Avro container file.
     */
    public void write(T theRecord) throws Exception {
        write(System.currentTimeMillis(), theRecord);
    }

    /**
     * Writes record to Avro container file.
     *
     * Note: This method is synchronized to assure that the max count limit is strictly correct. This could
     * be removed if ok to relax the limit if not super important.
     *
     * @param theTimestamp The timestamp for record used to sort it into the correct avro container file
     */
    public synchronized void write(long theTimestamp, T theRecord) throws Exception {

        // Fetch writer to use based on filename pattern
        Writer aWriter = getWriter(theTimestamp);

        // Append record
        aWriter.write(theRecord);
    }

    /**
     * Implement this in sub classes and return the created and configured DataFileWriter of preferred type.
     */
    abstract protected DataFileWriter<T> getDataFileWriter(File theFile) throws IOException;

    /**
     * Implement this in sub classes and return the schema for AVROs of class <T>.
     */
    abstract protected Schema getSchema();

    /**
     * Override this for setting custom type for AVROs to be written.
     */
    protected String getTypeName() {
        return getSchema().getName();
    }


    /**
     * Returns a Writer based on the filename to write record into.
     */
    private synchronized Writer getWriter(long theTimestamp) throws IOException {

        // Get hash for filename based on record timestamp
        String aHash = getHash(theTimestamp);

        // Fetch writer for filename if any
        Writer aWriter = myWriters.get(aHash);

        // Create new file if none exists, or roll it if time
        if (aWriter == null || aWriter.getCount() >= myMaxRecords || aWriter.getAge() >= myMaxAge) {

            if (aWriter != null) {
                aWriter.close();
            }

            long aCreated = System.currentTimeMillis();
            String aFilename = getFilename(myFilenamePattern, theTimestamp, getTypeName(), aCreated);
            aWriter = new Writer(aFilename, aCreated);
            myWriters.put(aHash, aWriter);
        }

        return aWriter;
    }


    /**
     * Get fast hash of file depending on the timestamp to book record under.
     *
     * This is basically done by generating a string out of all date patterns in filename pattern. This works since the
     * date parts represents all moving parts in the file name with exception of %i that is handled later.
     */
    private String getHash(long theTimestamp) {
        return myHashFormat.format(theTimestamp);
    }

    /**
     * Generates a date formatter for all date patterns in file name pattern.
     */
    private FastDateFormat getHashFormat(String thePattern) {

        StringBuilder aBuilder = new StringBuilder();

        int aPos = thePattern.indexOf("%d{");

        while (aPos >= 0) {
            int anEnd = thePattern.indexOf("}", aPos);

            aBuilder.append(thePattern.substring(aPos+3, anEnd));
            aBuilder.append(".");

            aPos = thePattern.indexOf("%d{", anEnd);
        }

        return FastDateFormat.getInstance(aBuilder.toString(), TimeZone.getTimeZone("UTC"));
    }

    /**
     * Generates a filename for the filename pattern. Support for following tokens:
     *
     * - %d{datetime pattern}  Replaced by the datetime for the given pattern (see SimpleDateFormat)
     * - %c{datetime pattern}  Replaced by the datetime for created and the given pattern (see SimpleDateFormat)
     * - %t                    Replaced with the AVRO type name (default: Schema.getName()) in lower case
     * - %T                    Replaced with the AVRO type name (default: Schema.getName())
     * - %i                    Replaced is replaced by a random 8 letter hex string
     */
    public static String getFilename(String thePattern, long theTimestamp, String theType, long theCreated) {

        StringBuilder aBuilder = new StringBuilder();

        while (true) {

            int aPos = thePattern.indexOf("%");

            if (aPos < 0) {
                aBuilder.append(thePattern);
                break;
            }

            aBuilder.append(thePattern.substring(0, aPos));

            switch (thePattern.charAt(aPos+1)) {

                case 'd':
                    int anEnd = thePattern.indexOf("}", aPos);

                    FastDateFormat aFormat = FastDateFormat.getInstance(thePattern.substring(aPos + 3, anEnd), TimeZone.getTimeZone("UTC"));

                    aBuilder.append(aFormat.format(theTimestamp));
                    thePattern = thePattern.length() > anEnd ? thePattern.substring(anEnd+1) : "";
                    break;

                case 'c':
                    anEnd = thePattern.indexOf("}", aPos);

                    aFormat = FastDateFormat.getInstance(thePattern.substring(aPos + 3, anEnd), TimeZone.getTimeZone("UTC"));

                    aBuilder.append(aFormat.format(theCreated));
                    thePattern = thePattern.length() > anEnd ? thePattern.substring(anEnd+1) : "";
                    break;

                case 't':
                    aBuilder.append(theType.toLowerCase());
                    thePattern = thePattern.length() > 2 ? thePattern.substring(aPos + 2) : "";
                    break;

                case 'T':
                    aBuilder.append(theType);
                    thePattern = thePattern.length() > 2 ? thePattern.substring(aPos + 2) : "";
                    break;

                case 'i':
                    aBuilder.append(createRandomHex(8));
                    thePattern = thePattern.length() > 2 ? thePattern.substring(aPos + 2) : "";
                    break;

                default:
                    aBuilder.append("%");
                    thePattern = thePattern.length() > 1 ? thePattern.substring(aPos + 1) : "";
            }
        }

        return aBuilder.toString();
    }

    /**
     * Timer callback cleaning up over aged Writers.
     */
    private synchronized void onCleanup() {

        for (String aHash : myWriters.keySet()) {

            Writer aWriter = myWriters.get(aHash);

            if (aWriter.getCount() < myMaxRecords && aWriter.getAge() < myMaxAge) {
                continue;
            }

            aWriter.close();

            myWriters.remove(aHash);
        }
    }

    private static String createRandomHex(int theLength) {
        byte[] aBuff = new byte[theLength];
        new Random().nextBytes(aBuff);
        return Hex.encodeHexString(aBuff);
    }


    /**
     * Beautifies a map of Writers.
     */
    private class Writers extends ConcurrentHashMap<String, Writer> {}


    /**
     * Class containing the Avro writer for a file name and the stats for it.
     */
    private class Writer {

        private DataFileWriter<T> myWriter;
        private String myFilename;
        private long myCreated;
        private AtomicInteger myCount;

        public Writer(String theFilename, long theCreated) throws IOException {

            myFilename = theFilename;
            myCount = new AtomicInteger();
            myCreated = theCreated;

            myWriter = createAvroFile(theFilename + myProgressPostfix);
        }

        private DataFileWriter<T> createAvroFile(String theFilename) throws IOException {
            ourLog.debug("Create new AVRO file: %s", theFilename);

            File aFile = new File(theFilename);

            // Create dir if none
            if (aFile.getParentFile().mkdirs()) {
                ourLog.debug("Created new dir: %s", aFile.getParent());
            }

            return getDataFileWriter(aFile);
        }

        public void write(T theRecord) throws IOException {
            myCount.incrementAndGet();
            myWriter.append(theRecord);
        }

        @SuppressWarnings("all")
        public void close() {
            try {

                if (myWriter == null) {
                    return;
                }

                ourLog.debug("Closing file");

                myWriter.close();
                myWriter = null;

                if (!myProgressPostfix.isEmpty()) {
                    new File(myFilename + myProgressPostfix).renameTo(new File(myFilename));
                }

            } catch (IOException e) {
                ourLog.warn("Failed to close and rename file " + myFilename, e);
            }
        }

        public long getAge() {
            return System.currentTimeMillis()-myCreated;
        }

        public int getCount() {
            return myCount.get();
        }
    }

}






