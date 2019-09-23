package com.ws.common.avrologging.datalake;

import com.amazonaws.services.s3.model.StorageClass;
import com.ws.common.avrologging.shipper.SimpleS3Shipper;
import com.ws.common.avrologging.writer.v2.AvroRollingFileWriter;
import com.ws.common.avrologging.writer.v2.AvroWriter;
import com.ws.common.avrologging.writer.v2.ReflectionRollingFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Creates a Data Lake Publisher instance for helping out to write and publish AVRO records to the central
 * Widespace Data Lake.
 *
 * Written avro records will be published in the Data Lake S3 bucket under following path:
 *
 * - s3://<theBucket>/avro/<theComponentName>/<theSourceName>/
 *
 * So be sure to chose theComponentName and theSourceName wisely.
 *
 * To use this class you first create an instance of it and then:
 *
 * - Create one AvroWriter per avro record type to publish, use one of the methods below for this:
 *   - createGenericRecordWriter(..)
 *   - createReflectionRecordWriter(..)
 *
 * - Write records by calling the AvroWriters write(..) method
 *
 * - You manually publish/uploads all finished files to Data Lace by calling the publish() methods at even intervals.
 *
 * - When done you need to call close() on the DataLakePublisher instance. That will close all active writers and
 *   to upload all the remaining files not yet uploaded.
 *
 * Example:
 *
 * // Create a Data Lake publisher for helping out writing and publishing avro records.
 * DataLakePublisher aPublisher = new DataLakePublisher("comp", "source", "/tmp/datalake/", "<bucket-name>", "xxx", "yyy");
 *
 * // Create a writer for the generated AVRO record class Event. The writer will package the written records into an AVRO
 * // container file when either the max number of 10000 records reached or the records exceeded a two minute age.
 * AvroWriter aWriter = aPublisher.createGenericRecordWriter(Event.SCHEMA, 10000, 120);
 *
 * // Write some AVRO events
 * aWriter.write(System.currentTimeMillis(), new Event(...));
 * aWriter.write(System.currentTimeMillis(), new Event(...));
 * aWriter.write(System.currentTimeMillis(), new Event(...));
 *
 * // Upload and publish the generated files if any is packed and ready.
 * aPublisher.publish();
 *
 * // Close publisher, all writers will flush their records and package them for upload, finally is the files published
 * // to S3.
 * aPublisher.close();
 */
public class DataLakePublisher implements Closeable {

    private File myDir;

    private String myFileNamePrefix;

    private SimpleS3Shipper myShipper;

    private boolean isS3PublishDisabled;

    private List<AvroWriter> myWriters = new ArrayList<>();

    /*
     * Creates a Data Lake Publisher instance to help out with data publishing.
     *
     * @param theComponentName  The name of system component producing the data in lower case. E.g. 'engine', 'recommender', 'inscreen' etc.
     * @param theSourceName     The source/log in component producing the data. E.g. 'rl2', 'accesslogs', 'uidmergelogs' etc
     * @param theLocalDir       Path to a temporary folder on local disk where to write AVRO files and package them before publishing.
     * @param theBucket         The bucket in Data Lake where to upload and publish data.
     * @param theAwsKeyId       The AWS access key id with permissions to upload files to Data Lake bucket.
     * @param theAwsSecretKeyId The AWS secret key
     */
    public DataLakePublisher(String theComponentName, String theSourceName, String theLocalDir, String theBucket, String theAwsKeyId, String theAwsSecretKeyId) {
       this(theComponentName, theSourceName, theLocalDir, theBucket, theAwsKeyId, theAwsSecretKeyId, false /* S3 publishing enabled by default */);
    }

    /*
     * Creates a Data Lake Publisher instance to help out with data publishing.
     *
     * @param theComponentName  The name of system component producing the data in lower case. E.g. 'engine', 'recommender', 'inscreen' etc.
     * @param theSourceName     The source/log in component producing the data. E.g. 'rl2', 'accesslogs', 'uidmergelogs' etc
     * @param theLocalDir       Path to a temporary folder on local disk where to write AVRO files and package them before publishing.
     * @param theBucket         The bucket in Data Lake where to upload and publish data.
     * @param theAwsKeyId       The AWS access key id with permissions to upload files to Data Lake bucket.
     * @param theAwsSecretKeyId The AWS secret key
     * @param theIsS3PublishDisabled true means data will not be uploaded to S3. Only files will be written.
     */
    public DataLakePublisher(String theComponentName, String theSourceName, String theLocalDir, String theBucket, String theAwsKeyId, String theAwsSecretKeyId, boolean theIsS3PublishDisabled) {
        this(theBucket, "avro/" + theComponentName + "/" + theSourceName, theLocalDir, theAwsKeyId, theAwsSecretKeyId, theIsS3PublishDisabled);
    }

    /*
     * Creates a Data Lake Publisher instance to help out with data publishing.
     *
     * @param theS3Bucket       The bucket in Data Lake where to upload and publish data.
     * @param theS3Prefix       The S3 prefix / root where to upload files.
     * @param theLocalDir       Path to a temporary folder on local disk where to write AVRO files and package them before publishing.
     * @param theAwsKeyId       The AWS access key id with permissions to upload files to Data Lake bucket.
     * @param theAwsSecretKeyId The AWS secret key
     * @param theIsS3PublishDisabled true means data will not be uploaded to S3. Only files will be written.
     */
    protected DataLakePublisher(String theS3Bucket, String theS3Prefix, String theLocalDir, String theAwsKeyId, String theAwsSecretKeyId, boolean theIsS3PublishDisabled) {
        File aDir = new File(theLocalDir);

        if (!aDir.exists()) {
            aDir.mkdirs();
        }

        if (!aDir.isDirectory() || !aDir.canWrite()) {
            throw new IllegalArgumentException("Not a valid dir: " + theLocalDir);
        }

        myDir = aDir;
        isS3PublishDisabled = theIsS3PublishDisabled;
        myShipper = new SimpleS3Shipper(myDir.getAbsolutePath(), theS3Bucket, theS3Prefix, theAwsKeyId, theAwsSecretKeyId, "avro");

        setStorageClass(StorageClass.StandardInfrequentAccess);
    }



    /**
     * Sets storage class of the uploaded files.
     *
     * @link https://aws.amazon.com/s3/storage-classes/
     *
     * Default is the Infrequent Access class.
     *
     * @param theStorageClass The storage class to set, e.g. STANDARD, REDUCED_REDUNDANCY, GLACIER, STANDARD_IA...
     */
    public void setStorageClass(StorageClass theStorageClass) {
        myShipper.setStorageClass(theStorageClass);
    }

    public void setFileNamePrefix(String theFileNamePrefix) {
        myFileNamePrefix = theFileNamePrefix;
    }

    private String getPathPattern() {
        return myDir.getAbsolutePath() + "/%t/d=%d{yyyy-MM-dd}/h=%d{HH}/" + StringUtils.defaultString(myFileNamePrefix, "") + "%c{yyyyMMddHHmm}-%i.avro";
    }

    /**
     * Creates an AVRO writer for a specific AVRO record type. Use this method if you have pregenerated AVRO record
     * classes or if you are working with GenericRecord types.
     *
     * @param theSchema     The AVRO schema. If auto generated AVRO java classes use the <Class>.SCHEMA$ static member.
     * @param theMaxRecords The max number of records for a single AVRO container file before making it ready for upload
     * @param theMaxAgeS    The max age in seconds for AVRO container file before making it ready for upload
     * @param <T>           The class generic type.
     * @return              A writer for AVRO records
     * @throws IOException  If problems
     */
    public <T extends GenericRecord> AvroWriter<T> createGenericRecordWriter(Schema theSchema, int theMaxRecords, int theMaxAgeS) throws IOException {

        String aPattern = getPathPattern();

        AvroWriter<T> aWriter = new AvroRollingFileWriter<T>(theSchema, aPattern, ".progress", theMaxRecords, theMaxAgeS);

        myWriters.add(aWriter);

        return aWriter;
    }

    /**
     * Creates an AVRO writer for writing java POJOs using reflection. AVRO schema is dynamically generated.
     *
     * NOTE: This writer is not preferred, use the createGenericRecordWriter(...) if possible.
     *
     * @param theClass      The java POJO class
     * @param theMaxRecords The max number of records for a single AVRO container file before making it ready for upload
     * @param theMaxAgeS    The max age in seconds for AVRO container file before making it ready for upload
     * @param <T>           The class generic type.
     * @return              A writer for AVRO records
     * @throws IOException  If problems
     */
    public <T> AvroWriter<T> createReflectionRecordWriter(Class<T> theClass, int theMaxRecords, int theMaxAgeS) throws IOException {

        String aPattern = getPathPattern();

        AvroWriter<T> aWriter = new ReflectionRollingFileWriter<T>(theClass, aPattern, ".progress", theMaxRecords, theMaxAgeS);

        myWriters.add(aWriter);

        return aWriter;
    }

    /**
     * Call this to publish already packaged AVRO container files.
     */
    public Collection<File> publish() {
        if (isS3PublishDisabled) {
            return Collections.emptyList();
        }

        return myShipper.pushAndRemove();
    }

    /**
     * Closes all created AVRO writers and makes a final publish/upload of the remainders.
     */
    @Override
    public void close() throws IOException {
        for (AvroWriter aWriter : myWriters) {
            try {
                aWriter.close();
            } catch (Exception e) {
            }
        }

        publish();
    }
}
