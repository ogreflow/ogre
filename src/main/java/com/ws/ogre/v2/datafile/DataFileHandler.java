package com.ws.ogre.v2.datafile;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.datetime.DateHour.DateHours;
import com.ws.ogre.v2.datetime.DateUtil;
import com.ws.ogre.v2.utils.JobExecutorService;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Handler working with data files on S3.
 */
public class DataFileHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private S3Client myS3Client;
    private S3BetterUrl myRoot;

    public DataFileHandler(S3Client theClient, S3BetterUrl theRootUrl) {
        myS3Client = theClient;
        myRoot = theRootUrl;
    }

    public Set<String> getAllTypes() {
        List<S3BetterUrl> someS3Urls = myS3Client.listFolders(myRoot);

        Set<String> someUrls = new HashSet<>();
        for (S3BetterUrl aS3Url : someS3Urls) {
            someUrls.add(aS3Url.getBaseName());
        }

        return someUrls;
    }

    public DataFiles findFilesByTimeRange(DateHour theFrom, DateHour theTo, final Set<String> theTypes) {
        return findFilesByTimeRange(theFrom, theTo, theTypes, null);
    }

    public DataFiles findFilesByTimeRange(DateHour theFrom, DateHour theTo, final Set<String> theTypes, final String theRegExpFilter) {

        ourLogger.trace("Find S3 files for period: %s - %s, type: %s, source: %s", theFrom, theTo, theTypes, myRoot);

        // Get all hours in range
        DateHours aTimeRange = theFrom.getHoursTo(theTo);

        // Create set of full days within range, key is yyyyMMdd
        Set<String> aFullDates = new HashSet<>(aTimeRange.getFullDates());

        final DataFiles aFiles = new DataFiles();

        // Spawn up a number of threads to share the load...

        JobExecutorService<String> anExecutor = new JobExecutorService<>(Math.min(20, aFullDates.size()));

        anExecutor.addTasks(aFullDates);

        // Get files for full dates
        anExecutor.execute(new JobExecutorService.JobExecutor<String>() {
            public void execute(String theDate) throws Exception {
                for (String aType : theTypes) {
                    DataFiles aDateFiles = getFilesForDate(DateUtil.parse(theDate, "yyyyMMdd"), aType, theRegExpFilter);
                    synchronized (aFiles) {
                        aFiles.addAll(aDateFiles);
                    }
                }
            }
        });

        // Get files for hours of the partial dates
        for (DateHour anHour : aTimeRange) {
            if (aFullDates.contains(anHour.format("yyyyMMdd"))) {
                continue;
            }

            for (String aType : theTypes) {
                DataFiles anHourFiles = getFilesForHour(anHour, aType, theRegExpFilter);
                aFiles.addAll(anHourFiles);
            }
        }

        ourLogger.trace("Found %s S3 files for period: %s - %s, type: %s, source: %s", aFiles.size(), theFrom, theTo, theTypes, myRoot);

        return aFiles;
    }

    private DataFiles getFilesForDate(Date theDate, String theType, String theRegExpFilter) {

        String aKeyPrefix = DataFile.getDateKeyPrefix(myRoot, theDate, theType);

        // Fetch listing of files for prefix
        ourLogger.trace("Listing objects at s3://%s/%s", myRoot.bucket, aKeyPrefix);
        List<S3BetterUrl> someUrls = myS3Client.listObjects(new S3BetterUrl(myRoot.bucket, aKeyPrefix));

        DataFiles aFiles = new DataFiles();

        for (S3BetterUrl anUrl : someUrls) {
            if (theRegExpFilter != null && !anUrl.toString().matches(theRegExpFilter)) {
                ourLogger.debug("Skip %s since it does not match filter: %s", anUrl, theRegExpFilter);
                continue;
            }
            aFiles.add(new DataFile(anUrl));
        }

        return aFiles;
    }

    private DataFiles getFilesForHour(DateHour theDateHour, String theType, String theRegExpFilter) {

        String aKeyPrefix = DataFile.getDateHourKeyPrefix(myRoot, theDateHour, theType);

        // Fetch listing of files for prefix
        ourLogger.trace("Listing objects at s3://%s/%s", myRoot.bucket, aKeyPrefix);
        List<S3BetterUrl> someUrls = myS3Client.listObjects(new S3BetterUrl(myRoot.bucket, aKeyPrefix));

        DataFiles aFiles = new DataFiles();

        for (S3BetterUrl anUrl : someUrls) {
            if (theRegExpFilter != null && !anUrl.toString().matches(theRegExpFilter)) {
                ourLogger.debug("Skip %s since it does not match filter: %s", anUrl, theRegExpFilter);
                continue;
            }
            aFiles.add(new DataFile(anUrl));
        }

        return aFiles;
    }

    public void deleteFilesByTimeRange(DateHour theFrom, DateHour theTo, Set<String> theTypes) {
        deleteFilesByTimeRange(theFrom, theTo, theTypes, null);
    }

    public void deleteFilesByTimeRange(DateHour theFrom, DateHour theTo, Set<String> theTypes, String theRegExpFilter) {

        ourLogger.trace("Going to delete S3 files for period: %s - %s, type: %s, source: %s", theFrom, theTo, theTypes, myRoot);

        DataFiles aFiles = findFilesByTimeRange(theFrom, theTo, theTypes, theRegExpFilter);

        List<String> aKeys = new ArrayList<>();
        for (DataFile aFile : aFiles) {
            aKeys.add(aFile.url.key);
        }

        ourLogger.info("Found %s files to delete: %s", aKeys.size(), aKeys);

        myS3Client.deleteObjects(myRoot.bucket, aKeys);
    }

    /**
     * The invoker should close the stream.
     */
    public InputStream getInputStream(DataFile theFile) {
        return myS3Client.getObjectStream(theFile.url.bucket, theFile.url.key);
    }

    public File downloadDataFile(DataFile theFile) {
        try {
            File aDestinationFile = File.createTempFile("data-" + theFile.type + "-" + theFile.name + "-", "." + theFile.ext);
            aDestinationFile.deleteOnExit(); // Safe guard to delete even though we will manually delete it later.

            myS3Client.getObjectToFile(theFile.url.bucket, theFile.url.key, aDestinationFile);

            return aDestinationFile;

        } catch (IOException e) {
            throw new RuntimeException("Unable to download file: " + theFile, e);
        }
    }

    public File downloadDataFileAndUnzip(DataFile theFile) {
        File aDownloadedFile = downloadDataFile(theFile);

        // No gzip, nothing to do more.
        if (!GzipUtils.isCompressedFilename(aDownloadedFile.getAbsolutePath())) {
            return aDownloadedFile;
        }

        try {
            File anUnzippedFile = new File(GzipUtils.getUncompressedFilename(aDownloadedFile.getAbsolutePath()));
            anUnzippedFile.deleteOnExit(); // Safe guard to delete even though we will manually delete it later.
            return unzip(aDownloadedFile, anUnzippedFile);

        } finally {
            ourLogger.info("Deleting temporary file: %s", aDownloadedFile.getAbsolutePath());

            if (!FileUtils.deleteQuietly(aDownloadedFile)) {
                Alert.getAlert().alert("Unable to delete temporary file: %s", aDownloadedFile.getAbsolutePath());
            }
        }
    }

    private File unzip(File theGzipFile, File theUnzippedFile) {
        try (
                GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(theGzipFile));
                FileOutputStream anUnzippedFileWriter = new FileOutputStream(theUnzippedFile)
        ) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                anUnzippedFileWriter.write(buffer, 0, len);
            }

            return theUnzippedFile;
        } catch (IOException e) {
            throw new RuntimeException("Unable to unzip file: " + theGzipFile.getAbsolutePath(), e);
        }
    }

    public void uploadDataFile(File theFileToUpload, DataFile theDstFile, StorageClass theDstStorageClass) {

        TransferManager aMgr = myS3Client.getTransferManager();
        InputStream anUploadingStream = null;

        try {
            anUploadingStream = new BufferedInputStream(new FileInputStream(theFileToUpload));

            PutObjectRequest aRequest = new PutObjectRequest(theDstFile.url.bucket, theDstFile.url.key, anUploadingStream, null)
                    .withStorageClass(theDstStorageClass);

            Upload anUpload = aMgr.upload(aRequest);
            anUpload.waitForCompletion();

            ourLogger.info("%s", anUpload.getDescription());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } catch (IOException e) {
            throw new RuntimeException(e);

        } finally {
            aMgr.shutdownNow();
            IOUtils.closeQuietly(anUploadingStream);
        }
    }

    public DataFiles findLatest(String theType, int theLookbackHours, int theMaxDaysToLookback) {

        DateHour aStart = new DateHour(new Date());
        DateHour aMinStart = aStart.getPrevDateHour(theLookbackHours);

        DataFiles aFiles = new DataFiles();

        for (int i = 0; i < theMaxDaysToLookback && aFiles.isEmpty(); i++) {

            DateHour anEnd = aStart;
            aStart = anEnd.getPrevDateHour(24);

            if (anEnd.isBefore(aMinStart)) {
                break;
            }

            if (aStart.isBefore(aMinStart)) {
                aStart = aMinStart;
            }

            aFiles = findFilesByTimeRange(aStart, anEnd, Collections.singleton(theType));
        }

        if (aFiles.isEmpty()) {
            return aFiles;
        }

        aFiles.sortDesc();

        DateHour aHour = aFiles.get(0).dateHour;

        DataFiles aLatest = new DataFiles();

        for (DataFile aFile : aFiles) {
            if (aFile.dateHour.equals(aHour)) {
                aLatest.add(aFile);
            }
        }

        return aLatest;
    }

    public void copy(DataFile theFile, S3BetterUrl theToRoot) {
        S3BetterUrl aTo = DataFile.createUrl(theToRoot, theFile.date, theFile.hour, theFile.type, theFile.name, theFile.ext);

        ourLogger.info("Copying %s to %s", theFile.url, aTo);

        myS3Client.copy(theFile.url, aTo);
    }

    public static class DataFilesByType extends HashMap<String, DataFiles> {
        public DataFilesByType() {
        }

        public DataFilesByType(DataFiles theFiles) {
            for (DataFile aFile : theFiles) {
                put(aFile);
            }
        }

        public void put(DataFile theFile) {
            DataFiles aFiles = get(theFile.type);

            if (aFiles == null) {
                aFiles = new DataFiles();
                put(theFile.type, aFiles);
            }

            aFiles.add(theFile);
        }

        public DataFiles getForType(String theType) {
            return get(theType);
        }

        public Set<String> getTypes() {
            return keySet();
        }

        public DataFiles getAllFiles() {
            DataFiles allFiles = new DataFiles();

            for (DataFiles aFiles : values()) {
                allFiles.addAll(aFiles);
            }

            return allFiles;
        }

        public Set<DateHour> getHours(String theType) {
            if (!containsKey(theType)) {
                return Collections.emptySet();
            }

            Set<DateHour> anHours = new HashSet<>();
            for (DataFile aFile : get(theType)) {
                anHours.add(aFile.dateHour);
            }

            return anHours;
        }

        public int getSize(String theType) {
            if (!containsKey(theType)) {
                return 0;
            }

            return get(theType).size();
        }
    }


    public static class DataFilesById extends HashMap<String, DataFile> {
        public DataFilesById(DataFiles theFiles) {
            for (DataFile aFile : theFiles) {
                put(aFile.id, aFile);
            }
        }

        public boolean contains(DataFile theFile) {
            return containsKey(theFile.id);
        }
    }

    public static class DataFiles extends ArrayList<DataFile> {
        public DataFiles() {
            super();
        }

        public DataFiles(Collection<? extends DataFile> c) {
            super(c);

            for (DataFile aFile : c) {
                if (aFile == null) {
                    ourLogger.warn("Inserting null into DataFiles", new Exception("Inserting null into DataFiles"));
                }
            }
        }

        @Override
        public boolean add(DataFile e) {
            if (e == null) {
                ourLogger.warn("Inserting null into DataFiles", new Exception("Inserting null into DataFiles"));
            }
            return super.add(e);
        }

        @Override
        public boolean addAll(Collection<? extends DataFile> c) {

            for (DataFile aFile : c) {
                if (aFile == null) {
                    ourLogger.warn("Inserting null into DataFiles", new Exception("Inserting null into DataFiles"));
                }
            }

            return super.addAll(c);
        }

        public void sortAsc() {
            Collections.sort(this, new Comparator<DataFile>() {
                public int compare(DataFile the1, DataFile the2) {
                    if (the1.timestamp.getTime() == the2.timestamp.getTime()) {
                        return 0;
                    }
                    return the1.timestamp.getTime() > the2.timestamp.getTime() ? 1 : -1;
                }
            });
        }

        public void sortDesc() {
            sortAsc();
            Collections.reverse(this);
        }

        public Set<DateHour> getHours() {
            Set<DateHour> anHours = new HashSet<>();
            for (DataFile aFile : this) {
                anHours.add(aFile.dateHour);
            }

            return anHours;
        }

        public boolean contains(DataFile theFile) {
            for (DataFile aFile : this) {
                if (aFile.id.equals(theFile.id)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     *
     */
    public static class DataFile {
        public S3BetterUrl url;

        public String id;

        public String name;
        public String ext;
        public String type;
        public String date;
        public String hour;
        public DateHour dateHour;

        public Date timestamp;

        /**
         * theUrl ==> s3://<bucket>/<avro/json/tsv>/<source>/<component>/<type>/d=2015-02-07/h=16/delivery.yyyymmddhh.random.avro             [ avro ]
         * theUrl ==> s3://<bucket>/<avro/json/tsv>/<source>/<component>/<type>/d=2015-02-07/h=16/delivery.yyyymmddhh.random.json.gz          [ json + gzipped ]
         * theUrl ==> s3://<bucket>/<avro/json/tsv>/<source>/<component>/<type>/d=2015-02-07/h=16/delivery.yyyymmddhh.random.json             [ json ]
         * theUrl ==> s3://<bucket>/<avro/json/tsv>/<source>/<component>/<type>/d=2015-02-07/h=16/delivery.yyyymmddhh.random.gz               [ gz ]
         * theUrl ==> s3://<bucket>/<avro/json/tsv>/<source>/<component>/<type>/d=2015-02-07/h=16/delivery.yyyymmddhh.random.0001_part_00.gz  [ tsv ]
         */
        public DataFile(S3BetterUrl theUrl) {

            Pattern aPattern = Pattern.compile(".*/([^/]+)/d=([0-9]{4}-[0-9]{2}-[0-9]{2})/h=([0-9]{2})/(.*)\\.(.*)");

            Matcher aMatcher = aPattern.matcher(theUrl.key);

            if (!aMatcher.find()) {
                throw new IllegalArgumentException("Not a Data file: " + theUrl);
            }

            type = aMatcher.group(1);
            date = aMatcher.group(2);
            hour = aMatcher.group(3);
            name = aMatcher.group(4);
            ext = aMatcher.group(5);

            // Fix double extensions like: json.gz / tsv.gz etc. We could do that in the above regex pattern but that becomes too complicated.
            if (name.endsWith(".json") || name.endsWith(".tsv") || name.endsWith(".csv")) {
                String anExtInName = name.replaceAll("(.*)\\.(.*)", "$2");

                ext = anExtInName + "." + ext;
                name = name.replaceAll("(.*)\\.(.*)", "$1");
            }

            // Derived data from the raw data.
            id = date + "/" + hour + "/" + type + "/" + name;

            url = theUrl;
            timestamp = DateUtil.parse(date + hour, "yyyy-MM-ddHH");
            dateHour = new DateHour(timestamp);
        }

        public boolean isGzipFile() {
            return "gz".equals(ext) || ext.endsWith(".gz");
        }

        public boolean isAvroFile() {
            return "avro".equals(ext);
        }

        public boolean isJson() {
            return "json".equals(ext);
        }

        public boolean isTsv() {
            return "tsv".equals(ext);
        }

        public boolean isGzipedJson() {
            return "json.gz".equals(ext);
        }

        public boolean isGzipedTsv() {
            return "tsv.gz".equals(ext);
        }

        protected static String getDateHourKeyPrefix(S3BetterUrl theRoot, DateHour theDateHour, String theType) {

            String aYear = theDateHour.format("yyyy");
            String aMonth = theDateHour.format("MM");
            String aDay = theDateHour.format("dd");
            String anHour = theDateHour.format("HH");

            return String.format("%s/%s/d=%s-%s-%s/h=%s/", theRoot.key, theType, aYear, aMonth, aDay, anHour);
        }

        protected static String getDateKeyPrefix(S3BetterUrl theRoot, Date theDate, String theType) {

            String aYear = DateUtil.format(theDate, "yyyy");
            String aMonth = DateUtil.format(theDate, "MM");
            String aDay = DateUtil.format(theDate, "dd");

            return String.format("%s/%s/d=%s-%s-%s/", theRoot.key, theType, aYear, aMonth, aDay);
        }

        public static S3BetterUrl createUrl(S3BetterUrl theRoot, String theDate, String theHour, String theType, String theName, String theExt) {

            String aYear = theDate.substring(0, 4);
            String aMonth = theDate.substring(5, 7);
            String aDay = theDate.substring(8, 10);

            return new S3BetterUrl(String.format("%s/%s/d=%s-%s-%s/h=%s/%s.%s", theRoot, theType, aYear, aMonth, aDay, theHour, theName, theExt));
        }

        @Override
        public String toString() {
            return url.toString();
        }
    }
}
