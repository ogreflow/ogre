package com.ws.common.avrologging.shipper;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.ws.common.logging.Logger;
import org.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Simple class that pushes up files to s3. This class is initiated with the local directory to search recursively for
 * files of a certain extension. Matching files are uploaded to S3 preserving the same folder structure above the
 * source dir.
 *
 * Example:
 *
 * Source dir: /apa/banan/
 * Destination bucket: blabla
 * Destination prefix: /a/b/c/
 *
 * Local files:
 *
 *   /apa/banan/date=2015-05-19/hour=11/1.apa
 *   /apa/banan/date=2015-05-19/hour=12/2.apa
 *   /apa/banan/date=2015-05-19/hour=13/3.apa
 *
 * Will be pushed to:
 *
 *   s3://blabla/a/b/c/date=2015-05-19/hour=11/1.apa
 *   s3://blabla/a/b/c/date=2015-05-19/hour=12/2.apa
 *   s3://blabla/a/b/c/date=2015-05-19/hour=13/3.apa
 *
 * This class uses the AWS SDK to upload files using multipart put and 10 threads.
 *
 * Uploaded files will be removed and empty dirs will be pruned.
 */
public class SimpleS3Shipper {

    private String mySourceDir;
    private String myDestBucket;
    private String myDestPrefix;

    private String myAwsKeyId;
    private String myAwsSecretKey;

    private StorageClass myStorageClass = StorageClass.Standard;

    private String myExtension;

    private final static Logger ourLogger = Logger.getLogger();

    /**
     * Creates a S3 shipper.
     *
     * @param theSourceDir      The source dir to search recursively for files to ship
     * @param theDestBucket     The S3 bucket to upload files
     * @param theDestPrefix     The S3 key prefix where to store files under
     * @param theAwsKeyId       The AWS access key to access S3
     * @param theAwsSecretKey   The AWS secret key to access S3
     * @param theExtension      The file extension of files to upload
     */
    public SimpleS3Shipper(String theSourceDir, String theDestBucket, String theDestPrefix, String theAwsKeyId, String theAwsSecretKey, String theExtension) {

        if (theSourceDir == null) {
            throw new IllegalArgumentException("Source dir not defined");
        }

        if (theDestBucket == null || theDestPrefix == null) {
            throw new IllegalArgumentException("Destination bucket and prefix not defined");
        }

        // Prefix should not start with a '/'
        if (theDestPrefix.startsWith("/") && theDestPrefix.length() > 1) {
            theDestPrefix = theDestPrefix.substring(1);
        }

        // Prefix should end with '/'
        if (!theDestPrefix.endsWith("/")) {
            theDestPrefix = theDestPrefix + "/";
        }

        if (theExtension == null) {
            throw new IllegalArgumentException("No extension defined");
        }

        if (theExtension.startsWith(".")) {
            theExtension = theExtension.substring(1);
        }

        mySourceDir = new File(theSourceDir).getAbsolutePath() + "/";   // Normalize path
        myDestBucket = theDestBucket;
        myDestPrefix = theDestPrefix;
        myAwsKeyId = theAwsKeyId;
        myAwsSecretKey = theAwsSecretKey;
        myExtension = theExtension;
    }

    @SuppressWarnings("all")
    public SimpleS3Shipper(String theSourceDir, String theDestBucket, String theDestPrefix, String theExtension) {
        this(theSourceDir, theDestBucket, theDestPrefix,theExtension, null, null);
    }

    public void setStorageClass(StorageClass theStorageClass) {
        myStorageClass = theStorageClass;
    }

    /**
     * Scan source dir recursively for files to ship. Matching files are uploaded and then removed.
     *
     * This method is synchronized because in practice, we have timer to push to s3 and on service shutdown
     * time, we can push again to confirm all data to be uploaded in s3.
     *
     * @return The shipped (and removed) files
     */
    public synchronized Collection<File> pushAndRemove() {

        try {

            TransferManager aMgr;

            if (myAwsKeyId == null) {
                aMgr = new TransferManager();
            } else {
                aMgr = new TransferManager(new BasicAWSCredentials(myAwsKeyId, myAwsSecretKey));
            }

            File aSrcDir = new File(mySourceDir);

            if (!aSrcDir.exists() || !aSrcDir.isDirectory()) {
                return Collections.emptyList();
            }

            List<Upload> anUploads = new ArrayList<>();

            // Find all files to upload
            Collection<File> aFiles = FileUtils.listFiles(aSrcDir, new String[]{myExtension}, true);

            for (final File aFile : aFiles) {

                // Calc destination prefix preserving file structure above the source dir
                String aRelativePath = aFile.getAbsolutePath().substring(mySourceDir.length());
                String aDestKey = myDestPrefix+aRelativePath;


                ourLogger.debug("Upload %s to: s3://%s/%s", aFile.getAbsolutePath(), myDestBucket, aDestKey);

                PutObjectRequest aPutRequest = new PutObjectRequest(myDestBucket, aDestKey, aFile)
                        .withStorageClass(myStorageClass);

                Upload anUpload = aMgr.upload(aPutRequest);

                // Add a progress listener that deletes the file on successful upload and
                // also prunes empty folders if any.
                anUpload.addProgressListener(new ProgressListener() {
                    public void progressChanged(ProgressEvent theProgressEvent) {

                        switch (theProgressEvent.getEventType()) {
                            case TRANSFER_COMPLETED_EVENT:
                                ourLogger.debug("File uploaded, delete it: %s", aFile.getAbsolutePath());
                                pruneFileAndEmptyDirs(aFile);
                                break;

                            case TRANSFER_FAILED_EVENT:
                            case TRANSFER_CANCELED_EVENT:
                                ourLogger.warn("Failed to upload file: %s", aFile.getAbsolutePath());
                        }
                    }
                });

                anUploads.add(anUpload);
            }

            // Wait for uploads to finnish
            for (Upload anUpload : anUploads) {
                anUpload.waitForCompletion();
            }

            ourLogger.debug("%s files uploaded", aFiles.size());

            aMgr.shutdownNow();

            return aFiles;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private void pruneFileAndEmptyDirs(File theFile) {
        try {

            if (!theFile.delete()) {
                throw new IOException("Failed to delete file");
            }

            File aDir = theFile.getParentFile();

            while (aDir != null && aDir.getAbsolutePath().startsWith(mySourceDir)) {

                // Get files and dirs in folder
                File[] aFiles = aDir.listFiles();

                // Empty?
                if (aFiles == null || aFiles.length > 0)
                    return;

                ourLogger.debug("Pruning empty dir: %s", aDir.getAbsolutePath());

                // Delete empty folder
                if (!aDir.delete()) {
                    return;
                }

                // Go up the tree
                aDir = aDir.getParentFile();
            }

        } catch (Exception e) {
            ourLogger.warn("Failed to delete file and purge dirs: " + theFile.getAbsolutePath(), e);
        }
    }

}
