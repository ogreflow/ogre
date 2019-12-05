package com.ws.ogre.v2.commands.avroconcat;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.gson.Gson;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFile;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFilesByType;
import com.ws.ogre.v2.datafile.DataFileManifest;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.logging.Alerter;
import com.ws.ogre.v2.utils.*;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Class converting and exploding avro files in S3 to json.
 */
public class AvroConcatHandler2 {

    private static final Logger ourLogger = Logger.getLogger();

    private DataFileHandler myAvroSrcHandler;
    private S3Client myAvroSrcS3Client;

    private DataFileHandler myAvroDstHandler;
    private S3Client myAvroDstS3Client;
    private S3BetterUrl myAvroDstRoot;
    private StorageClass myStorageClass;

    private Set<String> myTypes;


    public AvroConcatHandler2(Config theConfig, Set<String> theCliTypes) {

        myAvroSrcS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
        myAvroSrcHandler = new DataFileHandler(myAvroSrcS3Client, theConfig.srcRoot);

        myAvroDstS3Client = new S3Client(theConfig.dstAccessKey, theConfig.dstSecret);
        myAvroDstHandler = new DataFileHandler(myAvroDstS3Client, theConfig.dstRoot);
        myAvroDstRoot = theConfig.dstRoot;
        myStorageClass = theConfig.dstClass;

        myTypes = getTypes(theCliTypes, theConfig.types);
    }


    public void sync(int theGraceTimeMins, int theLookbackHours) {

        ourLogger.info("Continuously concat past hour at %02d minutes into current hour", theGraceTimeMins);

        while(true) {

            Calendar aCal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            aCal.setTimeInMillis(System.currentTimeMillis());
            aCal.set(Calendar.MINUTE, theGraceTimeMins);
            aCal.set(Calendar.SECOND, 0);
            aCal.set(Calendar.MILLISECOND, 0);
            aCal.add(Calendar.HOUR_OF_DAY, -theLookbackHours);

            // Sync all lookback hours
            while (aCal.getTimeInMillis() <= System.currentTimeMillis() - 60*60*1000l) {
                DateHour anHour = new DateHour(aCal.getTime());

                concatWithRetry(anHour, false, 30);

                aCal.add(Calendar.HOUR_OF_DAY, 1);
            }

            // Await next sync
            while (aCal.getTime().getTime() > System.currentTimeMillis() - 60*60*1000l) {
                ourLogger.debug("Sleep until %s", new Date(aCal.getTimeInMillis() + 60 * 60 * 1000l));
                SleepUtil.sleep(60000l);
            }
        }
    }


    public void load(DateHour theFrom, DateHour theTo, final boolean theReplace) {

        ourLogger.info("Load and convert avro files for period %s - %s", theFrom, theTo);

        // Get the hours to convert
        DateHour.DateHours aTimeRange = theFrom.getHoursTo(theTo);

        JobExecutorService<DateHour> anExecutor = new JobExecutorService<>(Math.min(6, aTimeRange.size()));

        // Iterate through hours and convert them one by one
        anExecutor.addTasks(aTimeRange);

        // Get files for full dates
        anExecutor.execute(new JobExecutorService.JobExecutor<DateHour>() {
            public void execute(DateHour theHour) throws Exception {
                concatWithRetry(theHour, theReplace, 30);
            }
        });
    }

    private void concatWithRetry(DateHour theHour, boolean theReplace, int theRetries) {

        boolean isNotifyOkAfterAlarm = false;

        for (int i = 0; i < theRetries; i++) {
            try {
                concatHour(theHour, theReplace);

                if (isNotifyOkAfterAlarm) {
                    Alert.getAlert().alert("Successfully concated avros for " + theHour + " after " + i + " retries.");
                }

                return;

            } catch (Exception e) {

                if (i % 5 == 4) {
                    isNotifyOkAfterAlarm = true;
                    Alerter.alert(e, "Failed to concat avros for hour " + theHour + ", will retry in 60 s. (" + (theRetries - i) + " retries left)");
                } else {
                    ourLogger.warn("Failed to concat avros for hour " + theHour + ", will retry in 60 s. (" + (theRetries - i) + " retries left)", e);
                }

                theReplace = true;

                SleepUtil.sleep(60000);
            }
        }

        throw new RuntimeException("Failed to concat hour " + theHour);
    }

    private void concatHour(DateHour theHour, boolean theReplace) throws Exception {

        ourLogger.info("Concat avro files for hour %s", theHour);

        // Should we replace old existing files, then delete old first
        if (theReplace) {
            delete(myTypes, theHour);
        }

        // Fetch files to concatenate if any
        DataFiles allToConcat = getToConcatenate(theHour);

        // Anything to concatenate?
        if (allToConcat.isEmpty()) {
            return;
        }

        // Sort them by type
        DataFilesByType allToConcatByType = new DataFilesByType(allToConcat);

        // Convert them type by type
        for (String aType : allToConcatByType.getTypes()) {
            DataFiles aToConvert = allToConcatByType.getForType(aType);

            concat(aToConvert);
        }

        ourLogger.info("Batch concat done");
    }

    private DataFiles getToConcatenate(DateHour theHour) {

        DataFiles isDone = new DataFiles();

        // Get manifest files (*.mf) containing info of the files already concatenated
        DataFiles aManifests = myAvroDstHandler.findFilesByTimeRange(theHour, theHour, myTypes, ".*\\.mf");

        // Collect all files already concatenated
        for (DataFile aManifestFile : aManifests) {
            // Fetch manifest json
            String aJson = myAvroDstS3Client.getObjectAsString(aManifestFile.url.bucket, aManifestFile.url.key);

            // Parse manifest
            DataFileManifest aManifest = new Gson().fromJson(aJson, DataFileManifest.class);

            // Register the already concatenated
            isDone.addAll(aManifest.getIncludes());
        }

        DataFiles aToConcat = new DataFiles();

        // Fetch all source files
        DataFiles aSrcFiles = myAvroSrcHandler.findFilesByTimeRange(theHour, theHour, myTypes);

        // Check which has not already been concatenated
        for (DataFile aSrcFile : aSrcFiles) {
            if (!isDone.contains(aSrcFile)) {
                ourLogger.info("Queue %s for concatenation", aSrcFile);
                aToConcat.add(aSrcFile);
            }
        }

        return aToConcat;
    }

    private void concat(DataFiles theFiles) throws Exception {

        MyAvroRecordWriter aWriter = null;
        MyAvroRecordReader aReader = null;
        String aSchema = null;
        File aDest = null;

        DataFiles allIncluded = new DataFiles();

        // Sort files to convert so we have them in "chronological" order
        theFiles.sortAsc();

        // Go through all files and concat their rows. If schema changes then continue concatenating into a new file.
        for (DataFile aFile : theFiles) {

            ourLogger.info("Fetch and concat: %s", aFile);

            // Create temp file
            File aDataFile = File.createTempFile(aFile.name, aFile.ext);

            // Download avro into temp file
            myAvroSrcS3Client.getObjectToFile(aFile.url.bucket, aFile.url.key, aDataFile);

            // Create Avro reader
            aReader = new MyAvroRecordReader(new FileInputStream(aDataFile));

            // Shift destination file if schema changed
            if (aSchema == null || !aSchema.equals(aReader.getSchema().toString())) {

                // Close old stuff first?
                if (aDest != null) {
                    ourLogger.info("Schema changed, start concatenate to a new file");
                    aReader.close();
                    aWriter.close();

                    // Upload it
                    upload(aDest, allIncluded);

                    allIncluded.clear();
                    aDest.delete();
                }

                // Create a new file to merge avros into
                aDest = File.createTempFile("concat-" + aFile.name, ".avro");

                aSchema = aReader.getSchema().toString();
                aWriter = new MyAvroRecordWriter(aReader.getSchema(), aDest);
            }

            // Concat all rows
            while (aReader.hasNext()) {
                GenericRecord aRecord = aReader.next();

                aWriter.append(aRecord);
            }

            allIncluded.add(aFile);

            // Clean up
            aDataFile.delete();
        }

        if (aDest != null) {
            aReader.close();
            aWriter.close();

            // Upload it
            upload(aDest, allIncluded);

            aDest.delete();
        }
    }

    private void upload(File theFile, DataFiles theIncluded) throws Exception {

        TransferManager aMgr = myAvroDstS3Client.getTransferManager();

        try {
            // Calc destination path
            DataFile aFile = theIncluded.get(0);
            S3BetterUrl aPath = DataFile.createUrl(myAvroDstRoot, aFile.date, aFile.hour, aFile.type, "concat-" + aFile.name, aFile.ext);

            ourLogger.info("Upload file containing %s concatenated files to %s", theIncluded.size(), aPath);

            PutObjectRequest aRequest = new PutObjectRequest(aPath.bucket, aPath.key, theFile)
                    .withStorageClass(myStorageClass);

            // Upload file
            Upload anUpload = aMgr.upload(aRequest);
            anUpload.waitForCompletion();
            ourLogger.info("Uploaded: %s", anUpload.getDescription());

            // Generate manifest
            DataFileManifest aManifest = new DataFileManifest();
            aManifest.setFile(aPath);
            aManifest.setIncludes(theIncluded);

            // Upload manifest
            aPath = new S3BetterUrl(aPath.toString() + ".mf");
            ourLogger.info("Upload manifest file to %s", aPath);
            myAvroDstS3Client.putObject(aPath.bucket, aPath.key, aManifest.serialize());

        } finally {
            aMgr.shutdownNow();
        }
    }

    private class Concated {
        File file;
        DataFiles included;

        public Concated(File theFile, DataFiles theIncluded) {
            file = theFile;
            included = theIncluded;
        }
    }

    private void delete(Set<String> theTypes, DateHour theHour) {

        ourLogger.info("Deleting concated files for %s", theHour);

        myAvroDstHandler.deleteFilesByTimeRange(theHour, theHour, theTypes);
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

    /* Just hide away some generics ugliness */
    static class MyAvroRecordReader extends DataFileStream<GenericRecord> {
        public MyAvroRecordReader(InputStream theIn) throws IOException {
            super(theIn, new GenericDatumReader<GenericRecord>());
        }
    }

    /* Just hide away some generics ugliness */
    static class MyAvroRecordWriter extends DataFileWriter<GenericRecord> {
        public MyAvroRecordWriter(Schema theSchema, File theFile) throws IOException {
            super(new SpecificDatumWriter<GenericRecord>(theSchema));
            setCodec(CodecFactory.snappyCodec());
            create(theSchema, theFile);
        }
    }


}
