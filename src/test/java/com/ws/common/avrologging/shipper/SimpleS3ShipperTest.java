package com.ws.common.avrologging.shipper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class SimpleS3ShipperTest {

    private static final String SRC_DIR = "/tmp/SimpleS3ShipperTest/";
    private static final String DST_BUCKET = "engine-unittests";
    private static final String DST_PREFIX = "SimpleS3ShipperTest/";

    private static final String AWS_ACCESSKEYID = "xxx";
    private static final String AWS_SECRETKEY   = "xxx";

    private AmazonS3Client myClient;

    @Before
    public void setUp() throws Exception {

        myClient = new AmazonS3Client(new BasicAWSCredentials(AWS_ACCESSKEYID, AWS_SECRETKEY));

        // Clean up destination
        List<String> aS3Keys = listS3Objects(DST_BUCKET, DST_PREFIX);

        deleteS3Objects(DST_BUCKET, aS3Keys);


        // Clean up source
        FileUtils.deleteDirectory(new File(SRC_DIR));
    }

    @Test
    public void test() throws Exception {

        File a1 = createFile(SRC_DIR + "some/dirs", "1.txt", "Hej");
        File a2 = createFile(SRC_DIR + "some/dirs", "2.txt", "Hå");

        File a3 = createFile(SRC_DIR + "other/dirs", "3.txt", "Hi");
        File a4 = createFile(SRC_DIR + "other/dirs", "4.txt", "Ho");

        File a5 = createFile(SRC_DIR + "another/dirs", "5.apa", "Ciao");
        File a6 = createFile(SRC_DIR + "another/dirs", "6.apa", "Mjau");

        SimpleS3Shipper aShipper = new SimpleS3Shipper(SRC_DIR, DST_BUCKET, DST_PREFIX, AWS_ACCESSKEYID, AWS_SECRETKEY, "txt");

        aShipper.pushAndRemove();

        Thread.sleep(200);

        // Check that uploaded files been removed locally
        assertFalse(a1.exists());
        assertFalse(a2.exists());
        assertFalse(a3.exists());
        assertFalse(a4.exists());

        // Check that non-uploaded files not been removed locally (of another extension)
        assertTrue(a5.exists());
        assertTrue(a6.exists());

        List<String> aS3Files = listS3Objects(DST_BUCKET, DST_PREFIX);

        assertEquals(4, aS3Files.size());

        Set<String> aFileKeys = new HashSet<>(aS3Files);

        assertTrue(aFileKeys.contains(DST_PREFIX + "some/dirs/1.txt"));
        assertTrue(aFileKeys.contains(DST_PREFIX + "some/dirs/2.txt"));
        assertTrue(aFileKeys.contains(DST_PREFIX + "other/dirs/3.txt"));
        assertTrue(aFileKeys.contains(DST_PREFIX + "other/dirs/4.txt"));

        // Check that the empty folders been pruned
        assertEquals(1, new File(SRC_DIR).listFiles().length);
    }

    private File createFile(String theDir, String theFilename, String theContent) throws Exception {

        File aDir = new File(theDir);
        aDir.mkdirs();

        File aFile = new File(aDir, theFilename);

        FileUtils.writeStringToFile(aFile, theContent);

        return aFile;
    }


    public List<String> listS3Objects(String theBucket, String thePrefix) {
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

    public void deleteS3Objects(String theBucket, List<String> theKeys) {
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

}
