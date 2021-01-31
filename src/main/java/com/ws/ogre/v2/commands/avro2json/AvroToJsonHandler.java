package com.ws.ogre.v2.commands.avro2json;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.avroutils.AvroPath;
import com.ws.ogre.v2.avroutils.AvroPathParser;
import com.ws.ogre.v2.avroutils.AvroPaths;
import com.ws.ogre.v2.avroutils.AvroRecordReader;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.*;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.utils.*;
import com.ws.ogre.v2.datetime.DateHour.DateHours;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Class converting and exploding avro files in S3 to json.
 */
public class AvroToJsonHandler {

    private static final Logger ourLogger = Logger.getLogger();


    private Config myConfig;

    private DataFileHandler myAvroDataFileHandler;
    private S3Client myAvroS3Client;

    private DataFileHandler myJsonDataFileHandler;
    private S3Client myJsonS3Client;
    private S3BetterUrl myJsonRoot;
    private StorageClass myJsonStorageClass;

    private Set<String> myTypes;


    public AvroToJsonHandler(Config theConfig, Set<String> theCliTypes) {

        myConfig = theConfig;

        myAvroS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
        myAvroDataFileHandler = new DataFileHandler(myAvroS3Client, theConfig.srcRoot);

        myJsonS3Client = new S3Client(theConfig.dstAccessKey, theConfig.dstSecret);
        myJsonRoot = theConfig.dstRoot;
        myJsonDataFileHandler = new DataFileHandler(myJsonS3Client, theConfig.dstRoot);
        myJsonStorageClass = theConfig.dstClass;

        myTypes = getTypes(theCliTypes, theConfig.types);
    }


    public void sync(int theScanIntervalS, int theLookbackHours, int theThreads) {

        ourLogger.info("Scan and convert new avro files every %s second and with a lookback of %s hour", theScanIntervalS, theLookbackHours);

        while (true) {
            // Calc next scan time
            long aNextScan = System.currentTimeMillis() + theScanIntervalS*1000;

            // Calc time to scan for new files from
            Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aCal.setTimeInMillis(System.currentTimeMillis());
            aCal.add(Calendar.HOUR_OF_DAY, -theLookbackHours);

            DateHour aFrom = new DateHour(aCal.getTime());
            DateHour aTo = new DateHour(new Date());

            // Scan for new avros and convert them
            convertPeriod(aFrom, aTo, theThreads, false, 10);

            ourLogger.info("Wait for next scan...");

            SleepUtil.sleep(aNextScan - System.currentTimeMillis());
        }
    }


    public void load(DateHour theFrom, DateHour theTo, int theThreads, boolean theReplace) {

        ourLogger.info("Load and convert avro files for period %s - %s", theFrom, theTo);

        convertPeriod(theFrom, theTo, theThreads, theReplace, 10);
    }

    private void convertPeriod(DateHour theFrom, DateHour theTo, int theThreads, boolean theReplace, int theRetries) {

        // Should we replace old existing rows, then delete old first
        if (theReplace) {
            deleteJsons(myTypes, theFrom, theTo);
        }

        // Get the hours to convert
        DateHours aTimeRange = theFrom.getHoursTo(theTo);

        // Iterate through hours and convert them one by one
        for (DateHour anHour : aTimeRange) {
            ourLogger.info("Convert hour %s for period %s - %s", anHour, theFrom, theTo);
            convertChunk(anHour, anHour, theThreads, theReplace, theRetries);
        }
    }



    private void convertChunk(DateHour theFrom, DateHour theTo, int theThreads, boolean theReplace, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {
                convertPeriod(theFrom, theTo, theThreads, theReplace);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully converted files after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alert.getAlert().alert("Failed to convert files, will delete converted files for period and retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                } else {
                    ourLogger.warn("Failed to convert files, will delete converted files for period and retry conversion in 60 s. (" + (theRetries - i) + " retries left)", e);
                }

                SleepUtil.sleepForRetry();

                // Remove old failed files
                theReplace = true;
            }
        }

        throw new RuntimeException("Failed to convert period " + theFrom + "-" + theTo + ".");
    }

    private void convertPeriod(DateHour theFrom, DateHour theTo, int theThreads, boolean theReplace) {

        ourLogger.info("Convert avro files for period %s - %s", theFrom, theTo);

        // Should we replace old existing rows, then delete old first
        if (theReplace) {
            deleteJsons(myTypes, theFrom, theTo);
        }

        // Fetch for new avros to convert
        DataFiles aFiles = scanForNewAvros(myTypes, theFrom, theTo);

        // Convert them
        batchConvert(theThreads, aFiles);

        ourLogger.info("Batch convert done");
    }

    private DataFiles scanForNewAvros(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        ourLogger.info("Scan %s - %s for new files", theFrom, theTo);

        DataFiles aNew = new DataFiles();

        // Fetch json files for period
        DataFiles aJsonFiles = myJsonDataFileHandler.findFilesByTimeRange(theFrom, theTo, theTypes);

        // Index them by id
        DataFilesById aJsonFilesById = new DataFilesById(aJsonFiles);


        // Fetch avro files for period
        DataFiles aAvroFiles = myAvroDataFileHandler.findFilesByTimeRange(theFrom, theTo, theTypes);

        // Check which avro files that is new
        for (DataFile aFile : aAvroFiles) {
            if (!aJsonFilesById.contains(aFile)) {
                aNew.add(aFile);
            }
        }

        return aNew;
    }


    private void batchConvert(int theThreads, DataFiles theFiles) {

        // Sort files to convert so we do it in "chronological" kind of order
        theFiles.sortAsc();

        // Single threaded requested?
        if (theThreads == 1) {
            for (DataFile aFile : theFiles) {
                convertToJson(aFile);
            }
            return;
        }

        // Spawn up a number of threads to share the load...

        JobExecutorService<DataFile> anExecutor = new JobExecutorService<>(theThreads);

        anExecutor.addTasks(theFiles);

        anExecutor.execute(new JobExecutorService.JobExecutor<DataFile>() {
            public void execute(DataFile theFile) throws Exception {
                convertToJson(theFile);
            }
        });
    }

    private void convertToJson(final DataFile theFile) {
        AvroRecordReader aReader = null;
        AvroToJsonGzWriters someWriters = null;
        File anAvroFile = null;

        try {

            ourLogger.info("Convert: %s", theFile.url);

            StopWatch aWatch = new StopWatch();

            // Download avro locally
            anAvroFile = downloadAvro(theFile);

            aReader = new AvroRecordReader(new FileInputStream(anAvroFile));

            someWriters = resolveWriters(aReader, theFile.type);

            // Walk through all records and convert them to gzipped jsons
            while (aReader.hasNext()) {
                GenericRecord aRecord = aReader.next();

                for (AvroToJsonGzWriter aWriter : someWriters) {
                    aWriter.println(aRecord);
                }
            }

            ourLogger.info("File fetched, converted and exploded into zipped json(s) (%s)", aWatch.getAndReset());

            // Upload converted files to destination
            upload(theFile, someWriters);

            ourLogger.info("Done converting: %s (%s)", theFile.url, aWatch);

        } catch (Exception e) {
            throw new RuntimeException("Failed to convert: " + theFile + ", got: " + e, e);

        } finally {
            IOUtils.closeQuietly(aReader);
            IOUtils.closeQuietly(someWriters);
            deleteAvro(anAvroFile);
        }
    }

    private void deleteAvro(File theFile) {
        try {
            if (theFile == null) {
                return;
            }

            theFile.delete();
        } catch (Exception e) {
            ourLogger.warn("Failed to delete file: %s", theFile);
        }
    }

    private File downloadAvro(DataFile theFile) {

        Exception aLastException = null;

        int aRetries = 3;

        while (aRetries-- > 0) {
            try {

                File aFile = File.createTempFile(theFile.name, theFile.ext);

                aFile.deleteOnExit(); // Safe guard to delete even though we will manually delete it later.

                ourLogger.info("Downloading: %s to %s", theFile, aFile.getAbsolutePath());

                myAvroS3Client.getObjectToFile(theFile.url.bucket, theFile.url.key, aFile);

                return aFile;

            } catch (Exception e) {
                ourLogger.warn("Failed to download file: %s, will retry...", theFile);
                aLastException = e;
            }
        }

        ourLogger.warn("Failed to download file: %s, no more retries... Last exception: %s", theFile, aLastException);

        throw new RuntimeException("Failed to download file: " + theFile + ", got: " + aLastException, aLastException);
    }

    private AvroToJsonGzWriters resolveWriters(AvroRecordReader theReader, String theType) {

        // Locate all first level avro arrays in record and create writers for them...

        AvroToJsonGzWriters someWriters = new AvroToJsonGzWriters();

        AvroPathParser aParser = new AvroPathParser(theReader.getSchema());

        // Locate all arrays paths in avro schema and
        // create writers for them converting to gz json
        for (AvroPath aPath : aParser.getArrayPaths()) {
            String aType = generateSubType(theType, aPath.getJsonPath());

            // Get avro paths for avro fields to include into exploded array
            AvroPaths someIncludePaths = aParser.getPaths(myConfig.getIncludes(aType));

            someWriters.add(new AvroArrayToJsonGzWriter(aType, someIncludePaths, aPath));
        }

        // Locate all maps paths in avro schema and
        // create writers for them converting to gz json
        for (AvroPath aPath : aParser.getMapPaths()) {
            String aType = generateSubType(theType, aPath.getJsonPath());

            // Get avro paths for avro fields to include into exploded map
            AvroPaths someIncludePaths = aParser.getPaths(myConfig.getIncludes(aType));

            someWriters.add(new AvroMapToJsonGzWriter(aType, someIncludePaths, aPath));
        }

        // Add the main avrp record writer
        someWriters.add(new AvroToJsonGzWriter(theType));

        return someWriters;
    }

    private void upload(DataFile theFile, AvroToJsonGzWriters theWriters) {

        TransferManager aMgr = myJsonS3Client.getTransferManager();

        try {

            List<Upload> anUploads = new ArrayList<>();

            // Initiate uploads...
            for (AvroToJsonGzWriter aWriter : theWriters) {

                S3BetterUrl aDest = DataFile.createUrl(myJsonRoot, theFile.date, theFile.hour, aWriter.getType(), theFile.name, "json.gz");

                if (aWriter.getRowCount() == 0) {
                    ourLogger.trace("File %s contains no records, skip it", aDest);
                    continue;
                }

                ourLogger.info("Upload %s record(s) to: %s", aWriter.getRowCount(), aDest);

                PutObjectRequest aRequest = new PutObjectRequest(aDest.bucket, aDest.key, new ByteArrayInputStream(aWriter.getData()), null)
                        .withStorageClass(myJsonStorageClass);

                Upload anUpload = aMgr.upload(aRequest);
                anUploads.add(anUpload);
            }

            // Wait for uploads to finnish
            for (Upload anUpload : anUploads) {
                anUpload.waitForCompletion();

                ourLogger.info("Uploaded: %s", anUpload.getDescription());
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } finally {
            aMgr.shutdownNow();
        }
    }

    private void deleteJsons(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        ourLogger.warn("Deleting json files from %s to %s", theFrom, theTo);

        myJsonDataFileHandler.deleteFilesByTimeRange(theFrom, theTo, theTypes);
    }

    private Set<String> getTypes(Set<String> theCliTypes, String[] theConfigTypes) {
        if (theCliTypes != null && !theCliTypes.isEmpty()) {
            return theCliTypes;
        }

        if (theConfigTypes == null || theConfigTypes.length == 0) {
            throw new IllegalArgumentException("No 'types' configured");
        }

        return new HashSet<>(Arrays.asList(theConfigTypes));
    }

    private String generateSubType(String theType, String theJsonPath) {
        return theType + theJsonPath.substring(1).replace('.', '_').toLowerCase();
    }

    /* Just hide away some ugliness */
    private class AvroToJsonGzWriters extends ArrayList<AvroToJsonGzWriter> implements Closeable {
        @Override
        public void close() throws IOException {
            for (AvroToJsonGzWriter aWriter : this) {
                IOUtils.closeQuietly(aWriter);
            }
        }
    }

    /**
     * Writer that will convert avro arrays to json and gzip it.
     */
    private class AvroArrayToJsonGzWriter extends AvroToJsonGzWriter {
        AvroPath myArrayPath;
        AvroPaths myFieldPaths;

        private AvroArrayToJsonGzWriter(String theType, AvroPaths theFieldPaths, AvroPath theArrayPath) {
            super(theType);

            myFieldPaths = theFieldPaths;
            myArrayPath = theArrayPath;
        }

        @Override
        public void println(GenericRecord theRecord) {
            GenericData.Array aVals = (GenericData.Array) myArrayPath.extract(theRecord);

            if (aVals == null || aVals.isEmpty()) {
                return;
            }

            // Include record id and timestamp
            StringBuilder aPrefix = new StringBuilder();

            for (AvroPath aPath : myFieldPaths) {
                aPrefix.append("\"");
                aPrefix.append(aPath.field);
                aPrefix.append("\": \"");
                aPrefix.append(aPath.extract(theRecord));
                aPrefix.append("\", ");
            }

            aPrefix.append("\"");
            aPrefix.append(myArrayPath.field);
            aPrefix.append("\": ");

            // ...add avro array values converted to json.
            for (Object aVal : aVals) {
                myRowCount++;

                String aJsonVal = AvroToJsonConverter.toString(aVal);

                String aJson = "{" + aPrefix + aJsonVal + "}\n";

                print(aJson);
            }
        }
    }

    /**
     * Writer that will convert avro maps to json and gzip it.
     */
    private class AvroMapToJsonGzWriter extends AvroToJsonGzWriter {
        AvroPath myMapPath;
        AvroPaths myFieldPaths;

        private AvroMapToJsonGzWriter(String theType, AvroPaths theFieldPaths, AvroPath theMapPath) {
            super(theType);

            myFieldPaths = theFieldPaths;
            myMapPath = theMapPath;
        }

        @Override
        public void println(GenericRecord theRecord) {
            Map<String, Object> aMap = (Map) myMapPath.extract(theRecord);

            if (aMap == null || aMap.isEmpty()) {
                return;
            }

            // Include record id and timestamp
            StringBuilder aPrefix = new StringBuilder();

            for (AvroPath aPath : myFieldPaths) {
                aPrefix.append("\"");
                aPrefix.append(aPath.field);
                aPrefix.append("\": \"");
                aPrefix.append(aPath.extract(theRecord));
                aPrefix.append("\", ");
            }

            aPrefix.append("\"");
            aPrefix.append(myMapPath.field);
            aPrefix.append("\": ");

            // ...add avro array values converted to json.
            for (Object aKey : aMap.keySet()) {
                Object aVal = aMap.get(aKey);

                myRowCount++;

                String aJsonVal = AvroToJsonConverter.toString(aVal);

                String aJson = "{" + aPrefix + "{\"name\": \"" + aKey + "\", \"value\": " + aJsonVal + "}}\n";

                print(aJson);
            }
        }
    }

    /**
     * Writer for converting avro records to gzipped json.
     */
    private class AvroToJsonGzWriter implements Closeable {
        private PrintWriter myWriter;
        private String myType;
        private ByteArrayOutputStream myMemOut;
        protected int myRowCount = 0;

        private AvroToJsonGzWriter(String theType) {
            try {
                myMemOut = new ByteArrayOutputStream();
                myWriter = new PrintWriter(new GZIPOutputStream(myMemOut));
                myType = theType;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        protected void print(String theString) {
            myWriter.print(theString);
        }

        public void println(GenericRecord theRecord) {
            myRowCount++;

            String aJsonVal = AvroToJsonConverter.toString(theRecord);

            print(aJsonVal);
            print("\n");
        }

        public String getType() {
            return myType;
        }

        public byte[] getData() {
            myWriter.flush();
            myWriter.close(); // Important, else the bytes are not flushed out through the GZIPOutputStream
            return myMemOut.toByteArray();
        }

        @Override
        public void close() throws IOException {
            IOUtils.closeQuietly(myMemOut);
            IOUtils.closeQuietly(myWriter);
        }

        public int getRowCount() {
            return myRowCount;
        }
    }


}
