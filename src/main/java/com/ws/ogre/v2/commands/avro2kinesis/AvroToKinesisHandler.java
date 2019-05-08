package com.ws.ogre.v2.commands.avro2kinesis;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.KinesisClient;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.datafile.DataFileHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.*;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateUtil;
import com.ws.ogre.v2.utils.*;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


public class AvroToKinesisHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private Done myDone = new Done();

    private DataFileHandler myAvroDataFileHandler;
    private S3Client myS3Client;

    private KinesisClient myKinesisClient;

    private String myStream;

    private Set<String> myTypes;

    public AvroToKinesisHandler(Config theConfig, Set<String> theCliTypes) {

        myS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
        myAvroDataFileHandler = new DataFileHandler(myS3Client, theConfig.srcRoot);

        myKinesisClient = new KinesisClient(theConfig.dstAccessKey, theConfig.dstSecret);

        myStream = theConfig.dstStream;

        myTypes = getTypes(theCliTypes, theConfig.types);
    }

    public void scanAndStream(int theScanIntervalS, int theLookbackH, int theThreads) {

        ourLogger.info("Scan and stream new avro files every %s second", theScanIntervalS);

        while (true) {
            // Calc next scan time
            long aNextScan = System.currentTimeMillis() + theScanIntervalS*1000;

            DateHour aFrom = new DateHour(DateUtil.getNHoursAgo(theLookbackH));
            DateHour aTo = new DateHour(new Date());

            if (myDone.isEmpty()) {
                ourLogger.info("Store current avro files state");

                DataFiles aAvroFiles = myAvroDataFileHandler.findFilesByTimeRange(aFrom, aTo, myTypes);

                for (DataFile aFile : aAvroFiles) {
                    myDone.add(aFile);
                }
            } else {
                myDone.evict(aFrom.getTime() - 2*theLookbackH*60*60*1000l);
            }

            // Scan for new avros and push them to stream
            stream(aFrom, aTo, theThreads);

            ourLogger.info("Wait for next scan...");

            SleepUtil.sleep(aNextScan - System.currentTimeMillis());
        }
    }

    public void stream(DateHour theFrom, DateHour theTo, int theThreads) {

        ourLogger.info("Stream new avro files for period %s - %s", theFrom, theTo);

        // Fetch for new avros to stream
        DataFiles aFiles = scanForNewAvros(myTypes, theFrom, theTo);

        stream(theThreads, aFiles);

        ourLogger.info("Batch streamed");
    }

    private DataFiles scanForNewAvros(Set<String> theTypes, DateHour theFrom, DateHour theTo) {

        ourLogger.info("Scan %s - %s", theFrom, theTo);

        DataFiles aNew = new DataFiles();

        DataFiles aAvroFiles = myAvroDataFileHandler.findFilesByTimeRange(theFrom, theTo, theTypes);

        for (DataFile aFile : aAvroFiles) {
            if (!myDone.contains(aFile)) {
                aNew.add(aFile);
                myDone.add(aFile);
            }
        }

        return aNew;
    }

    private void stream(int theThreads, DataFiles theFiles) {

        // Sort files to stream so we do it in "chronological" kind of order
        theFiles.sortAsc();

        // Single threaded requested?
        if (theThreads == 1) {
            for (DataFile aFile : theFiles) {
                stream(aFile);
            }
            return;
        }

        // Spawn up a number of threads to share the load...

        JobExecutorService<DataFile> anExecutor = new JobExecutorService<>(theThreads);

        anExecutor.addTasks(theFiles);

        anExecutor.execute(new JobExecutorService.JobExecutor<DataFile>() {
            public void execute(DataFile theFile) throws Exception {
                stream(theFile);
            }
        });

    }

    private void stream(final DataFile theFile) {

        try {
            ourLogger.info("Stream: %s", theFile.url);

            StopWatch aWatch = new StopWatch();

            InputStream anIn = myS3Client.getObjectStream(theFile.url.bucket, theFile.url.key);
            AvroRecordReader aReader = new AvroRecordReader(anIn);

            // Walk through all records and convert them to gzipped jsons
            while (aReader.hasNext()) {

                ByteArrayOutputStream anOut = new ByteArrayOutputStream();
                AvroRecordWriter aWriter = new AvroRecordWriter(aReader.getSchema(), anOut);

                while (aReader.hasNext() && anOut.size() < 1024*1024*0.9) {
                    GenericRecord aRecord = aReader.next();
                    aWriter.append(aRecord);
                }

                aWriter.close();

                ourLogger.info("Put: %s bytes", anOut.size());

                myKinesisClient.put(myStream, theFile.name, anOut.toByteArray());
            }

            ourLogger.info("Done streaming: %s (%s)", theFile.url, aWatch);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    private class AvroRecordReader extends DataFileStream<GenericRecord> {
        public AvroRecordReader(InputStream theIn) throws IOException {
            super(theIn, new GenericDatumReader<GenericRecord>());
        }
    }

    private class AvroRecordWriter extends DataFileWriter<GenericRecord> {
        public AvroRecordWriter(Schema theSchema, OutputStream theOut) throws IOException {
            super(new GenericDatumWriter<GenericRecord>(theSchema));

            setCodec(CodecFactory.snappyCodec());
            create(theSchema, theOut);
        }
    }


    private class Done extends HashMap<String, Long> {

        public void add(DataFile theFile) {
            put(theFile.type + "/" + theFile.name, System.currentTimeMillis());
        }

        public boolean contains(DataFile theFile) {
            return containsKey(theFile.type + "/" + theFile.name);
        }

        public void evict(long theFromTime) {
            Set<String> aKeys = new HashSet<>(keySet());

            for (String aKey : aKeys) {
                long aTimestamp = get(aKey);

                if (aTimestamp < theFromTime) {
                    remove(aKey);
                }
            }
        }
    }
}
