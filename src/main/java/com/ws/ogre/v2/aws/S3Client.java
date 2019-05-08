package com.ws.ogre.v2.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.StringInputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class S3Client {

    private AWSCredentials myCredentials;

    public S3Client(String theAccessKeyId, String theSecretKey) {
        myCredentials = new BasicAWSCredentials(theAccessKeyId, theSecretKey);
    }

    public AWSCredentials getCredentials() {
        return myCredentials;
    }

    public List<S3Url> listObjects(S3Url theRoot, int theLimit) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            ListObjectsRequest aRequest = new ListObjectsRequest()
                    .withBucketName(theRoot.bucket)
                    .withPrefix(theRoot.key)
                    .withMaxKeys(theLimit);


            List<S3Url> anUrls = new ArrayList<>();

            ObjectListing aListing = aClient.listObjects(aRequest);

            for (S3ObjectSummary aSummary : aListing.getObjectSummaries()) {
                anUrls.add(new S3Url(aSummary.getBucketName(), aSummary.getKey()));
            }

            return anUrls;

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public List<S3Url> listObjects(S3Url theRoot) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            ListObjectsRequest aRequest = new ListObjectsRequest()
                    .withBucketName(theRoot.bucket)
                    .withPrefix(theRoot.key);

            ObjectListing aListing;

            List<S3Url> anUrls = new ArrayList<>();

            do {
                aListing = aClient.listObjects(aRequest);
                for (S3ObjectSummary aSummary : aListing.getObjectSummaries()) {

                    // Remove files ending with / since those are there to represents a "folder" for some tools
                    if (aSummary.getKey().endsWith("/")) {
                        continue;
                    }

                    anUrls.add(new S3Url(aSummary.getBucketName(), aSummary.getKey()));
                }
                aRequest.setMarker(aListing.getNextMarker());

            } while (aListing.isTruncated());

            return anUrls;

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public List<S3Url> listFolders(S3Url theRoot) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            String aFolderPrefix = theRoot.key;

            if (!aFolderPrefix.endsWith("/")) {
                aFolderPrefix += "/";
            }

            ListObjectsRequest aRequest = new ListObjectsRequest()
                    .withBucketName(theRoot.bucket)
                    .withPrefix(aFolderPrefix)
                    .withDelimiter("/");

            ObjectListing aListing;

            List<S3Url> anUrls = new ArrayList<>();

            do {
                aListing = aClient.listObjects(aRequest);
                for (String aPrefix : aListing.getCommonPrefixes()) {
                    anUrls.add(new S3Url(theRoot.bucket, aPrefix));
                }
                aRequest.setMarker(aListing.getNextMarker());

            } while (aListing.isTruncated());

            return anUrls;

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public String getObjectAsString(String theBucket, String theKey) {
        S3Object anObject = null;

        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            anObject = aClient.getObject(new GetObjectRequest(theBucket, theKey));
            return IOUtils.toString(anObject.getObjectContent());

        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            IOUtils.closeQuietly(anObject);
        }
    }

    public String getObjectAsStringAfterUnzip(String theBucket, String theKey) {
        GZIPInputStream aGzipStream = null;

        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            S3Object anObject = aClient.getObject(new GetObjectRequest(theBucket, theKey));
            aGzipStream = new GZIPInputStream(anObject.getObjectContent());
            return IOUtils.toString(aGzipStream);

        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            IOUtils.closeQuietly(aGzipStream);
        }
    }

    public byte[] getRawObject(String theBucket, String theKey) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            S3Object anObject = aClient.getObject(new GetObjectRequest(theBucket, theKey));

            return IOUtils.toByteArray(anObject.getObjectContent());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream getObjectStream(String theBucket, String theKey) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            S3Object anObject = aClient.getObject(new GetObjectRequest(theBucket, theKey));
            return anObject.getObjectContent();

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void getObjectToFile(String theBucket, String theKey, File theDestination) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            GetObjectRequest aRequest = new GetObjectRequest(theBucket, theKey);

            aClient.getObject(aRequest, theDestination);

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void putObject(String theBucket, String theKey, String theData) {
        try {
            putObject(theBucket, theKey, new StringInputStream(theData));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void putObject(S3Url theUrl, File theData) {
        putObject(theUrl.bucket, theUrl.key, theData);
    }

    public void putObject(String theBucket, String theKey, File theData) {
        try {

            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            aClient.putObject(theBucket, theKey, theData);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TransferManager getTransferManager() {
        return new TransferManager(myCredentials);
    }

    public void putObjectGzipped(String theBucket, String theKey, String theData) {
        try {
            ByteArrayInputStream anIn = new ByteArrayInputStream(gzip(theData.getBytes("UTF8")));

            putObject(theBucket, theKey, anIn);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void putObject(String theBucket, String theKey, InputStream theIn) {
        try {
            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            aClient.putObject(theBucket, theKey, theIn, null);

        } catch (AmazonClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteObjects(S3Url theRoot) {

        List<String> aKeys = new ArrayList<>();

        for (S3Url anUrl : listObjects(theRoot)) {
            aKeys.add(anUrl.key);
        }

        deleteObjects(theRoot.bucket, aKeys);
    }

    public void deleteObjects(String theBucket, List<String> theKeys) {

        List<String> aKeys = new ArrayList<>(theKeys);

        while (aKeys.size() > 0) {
            List<String> aToDelete = new ArrayList<>();

            int aCount = 0;
            while (aCount++ < 1000 && aKeys.size() > 0) {
                aToDelete.add(aKeys.remove(0));
            }

            AmazonS3Client aClient = new AmazonS3Client(myCredentials);

            DeleteObjectsRequest aRequest =
                    new DeleteObjectsRequest(theBucket)
                            .withKeys(aToDelete.toArray(new String[0]));

            aClient.deleteObjects(aRequest);
        }
    }

    public void copy(S3Url theFrom, S3Url theTo) {

        AmazonS3Client aClient = new AmazonS3Client(myCredentials);

        aClient.copyObject(theFrom.bucket, theFrom.key, theTo.bucket, theTo.key);
    }

    private String gunzip(InputStream theIn) throws Exception {

        byte[] buffer = new byte[1024];

        GZIPInputStream gzis = new GZIPInputStream(theIn);

        ByteArrayOutputStream anOut = new ByteArrayOutputStream();

        int len;
        while ((len = gzis.read(buffer)) > 0) {
            anOut.write(buffer, 0, len);
        }

        gzis.close();

        return new String(anOut.toByteArray(), "UTF-8");
    }

    private byte[] gzip(byte[] theData) throws IOException {

        ByteArrayOutputStream anOut = new ByteArrayOutputStream();

        GZIPOutputStream aGzOut = new GZIPOutputStream(anOut);

        aGzOut.write(theData);

        aGzOut.close();

        return anOut.toByteArray();
    }

}
