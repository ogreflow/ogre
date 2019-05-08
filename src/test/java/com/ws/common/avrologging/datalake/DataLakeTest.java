package com.ws.common.avrologging.datalake;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.ws.common.avrologging.writer.v2.AvroWriter;
import com.ws.common.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;


/**
 */
public class DataLakeTest {

    private static final String LOCAL_DIR = "/tmp/DataLakeTest/";
    private static final String DST_BUCKET = "engine-unittests";
    private static final String DST_PREFIX = "DataLakeTest";

    private static final String AWS_ACCESSKEYID = "xxx";
    private static final String AWS_SECRETKEY = "xxx";

    private AmazonS3Client myClient;

    private static final Logger ourLog = Logger.getLogger();


    @Before
    public void setUp() throws Exception {

        ourLog.info("Remove old files in S3 and on disk");

        myClient = new AmazonS3Client(new BasicAWSCredentials(AWS_ACCESSKEYID, AWS_SECRETKEY));

        // Clean up destination
        List<String> aS3Keys = listS3Objects(DST_BUCKET, "avro/" + DST_PREFIX + "/");

        deleteS3Objects(DST_BUCKET, aS3Keys);

        // Clean up source
        FileUtils.deleteDirectory(new File(LOCAL_DIR));
    }

    @Test
    public void testWriteAndPublishToS3() throws Exception {

        int aRowsPerFile = 10;
        int aRows = 1000;

        DataLakePublisher aPublisher = new DataLakePublisher(DST_PREFIX, "srcname", LOCAL_DIR, DST_BUCKET, AWS_ACCESSKEYID, AWS_SECRETKEY);

        AvroWriter<WifiInfo> aWriter = aPublisher.createGenericRecordWriter(WifiInfo.SCHEMA$, aRowsPerFile, 100);

        ourLog.info("Writing lots of logs...");

        for (int i = 0; i < aRows; i++) {
            WifiInfo anInfo = new WifiInfo("" + i, "" + i, i, true);

            aWriter.write(System.currentTimeMillis(), anInfo);
        }

        ourLog.info("Closing writers");

        aPublisher.close();

        ourLog.info("List files published...");

        List<String> aS3Keys = listS3Objects(DST_BUCKET, "avro/" + DST_PREFIX + "/");

        assertTrue(aS3Keys.size() >= aRows / aRowsPerFile);
        assertTrue(aS3Keys.size() < aRows / aRowsPerFile + 5);

        Set<Integer> aIds = new HashSet<>();

        int aWrittenRows = 0;

        for (String aKey : aS3Keys) {

            ourLog.info("Fetch: %s", aKey);

            List<WifiInfo> anInfos = getWifiInfos(DST_BUCKET, aKey);

            ourLog.info("Found %s rows in file", anInfos.size());

            for (WifiInfo anInfo : anInfos) {
                aWrittenRows++;

                assertFalse(aIds.contains(anInfo.getLvl()));
                assertTrue(anInfo.getActive());
                assertTrue(anInfo.getLvl() < 1000);
                assertEquals(anInfo.getLvl().toString(), anInfo.getBssid());
                assertEquals(anInfo.getLvl().toString(), anInfo.getSsid());

                aIds.add(anInfo.getLvl());
            }
        }

        assertEquals(1000, aWrittenRows);
    }

    @Test
    public void testWriteAndDoNotPublishToS3() throws Exception {

        int aRowsPerFile = 10;
        int aRows = 1000;

        DataLakePublisher aPublisher = new DataLakePublisher(DST_PREFIX, "srcname", LOCAL_DIR, DST_BUCKET, AWS_ACCESSKEYID, AWS_SECRETKEY, true);

        AvroWriter<WifiInfo> aWriter = aPublisher.createGenericRecordWriter(WifiInfo.SCHEMA$, aRowsPerFile, 100);

        ourLog.info("Writting lots of logs...");

        for (int i = 0; i < aRows; i++) {
            WifiInfo anInfo = new WifiInfo("" + i, "" + i, i, true);

            aWriter.write(System.currentTimeMillis(), anInfo);
        }

        ourLog.info("Closing writers");

        aPublisher.close();

        ourLog.info("List files published...");

        List<String> aS3Keys = listS3Objects(DST_BUCKET, "avro/" + DST_PREFIX + "/");
        Assert.assertEquals(0, aS3Keys.size());
    }

    @Test
    public void testWriteAndPublishToS3WithFileNamePrefix() throws Exception {
        DataLakePublisher aPublisher = new DataLakePublisher(DST_PREFIX, "srcname", LOCAL_DIR, DST_BUCKET, AWS_ACCESSKEYID, AWS_SECRETKEY);
        aPublisher.setFileNamePrefix("AnAvroPrefix-");

        AvroWriter<WifiInfo> aWriter = aPublisher.createGenericRecordWriter(WifiInfo.SCHEMA$, 10, 100);

        WifiInfo anInfo = new WifiInfo("1", "1", 1, true);
        aWriter.write(System.currentTimeMillis(), anInfo);

        aPublisher.close();

        List<String> aS3Keys = listS3Objects(DST_BUCKET, "avro/" + DST_PREFIX + "/");
        Assert.assertEquals(1, aS3Keys.size());
        Assert.assertTrue(aS3Keys.get(0).matches("^.*/AnAvroPrefix-.*\\.avro$"));
    }

    private List<WifiInfo> getWifiInfos(String theBucket, String theKey) throws IOException {

        List<WifiInfo> anInfos = new ArrayList<>();

        byte[] aData = getRawObject(theBucket, theKey);

        DatumReader<WifiInfo> aDatumReader = new SpecificDatumReader<>(WifiInfo.class);
        DataFileReader<WifiInfo> aDataReader = new DataFileReader<>(new SeekableByteArrayInput(aData), aDatumReader);

        while (aDataReader.hasNext()) {
            WifiInfo anObject = aDataReader.next();

            anInfos.add(anObject);
        }

        return anInfos;
    }

    private List<String> listS3Objects(String theBucket, String thePrefix) {
        try {

            ListObjectsRequest aRequest = new ListObjectsRequest()
                    .withBucketName(theBucket)
                    .withPrefix(thePrefix);

            ObjectListing aListing;

            List<String> anUrls = new ArrayList<>();

            do {
                aListing = myClient.listObjects(aRequest);
                for (S3ObjectSummary aSummary : aListing.getObjectSummaries()) {
                    anUrls.add(aSummary.getKey());
                }
                aRequest.setMarker(aListing.getNextMarker());

            } while (aListing.isTruncated());

            return anUrls;

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getRawObject(String theBucket, String theKey) {
        try {

            S3Object anObject = myClient.getObject(new GetObjectRequest(theBucket, theKey));

            return IOUtils.toByteArray(anObject.getObjectContent());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteS3Objects(String theBucket, List<String> theKeys) {
        try {

            List<String> aKeys = new ArrayList<>(theKeys);

            while (aKeys.size() > 0) {
                List<String> aToDelete = new ArrayList<>();

                int aCount = 0;
                while (aCount++ < 1000 && aKeys.size() > 0) {
                    aToDelete.add(aKeys.remove(0));
                }

                DeleteObjectsRequest aRequest =
                        new DeleteObjectsRequest(theBucket)
                                .withKeys(aToDelete.toArray(new String[0]));

                myClient.deleteObjects(aRequest);
            }


        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
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