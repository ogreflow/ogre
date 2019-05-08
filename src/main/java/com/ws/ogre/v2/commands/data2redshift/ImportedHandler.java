package com.ws.ogre.v2.commands.data2redshift;

import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2redshift.db.ImportLogDao;
import com.ws.ogre.v2.datafile.DataFileHandler.*;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.Set;

/**
 */
public class ImportedHandler {

    // Remove this when ImportLog.filename includes the"s3://" prefix every where in all DBs.
    private String myBucket;

    public ImportedHandler(@Deprecated String theDataFileBucket) {
        myBucket = theDataFileBucket;
    }

    public DataFiles getImportedFiles(DateHour theFrom, DateHour theTo, String theType) {

        DataFiles aFiles = new DataFiles();

        Set<String> someImported = ImportLogDao.getInstance().findFilesByTypeAndTimeRange(theFrom, theTo, theType);

        for (String anImported : someImported) {

            if (!anImported.startsWith("s3://")) {
                anImported = "s3://" + myBucket + "/" + anImported;
            }

            aFiles.add(new DataFile(new S3Url(anImported)));
        }

        return aFiles;
    }

}
