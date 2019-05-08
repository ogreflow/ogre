package com.ws.ogre.v2.commands.data2rds.insertionhandlers.managed;

import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2rds.db.ImportLogDao;
import com.ws.ogre.v2.commands.data2rds.db.RdsDao;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.DataImportHandler;
import com.ws.ogre.v2.data2dbcommon.db.ImportLog;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFile;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 */
public class ManagedDataImportHandler implements DataImportHandler {

    public ManagedDataImportHandler() {
    }

    @Override
    public void markAsImported(DataFiles theMarkImported) {
        // Log imported rows
        List<ImportLog> someLogs = new ArrayList<>();

        for (DataFile aFile : theMarkImported) {
            ImportLog aLog = new ImportLog();
            aLog.filename = aFile.url.toString();
            aLog.tablename = aFile.type;
            aLog.timestamp = aFile.timestamp;
            someLogs.add(aLog);
        }

        ImportLogDao.getInstance().persist(someLogs, RdsDao.RDS_DATE_FORMAT);
    }

    @Override
    public DataFiles getImportedFiles(DateHour theFrom, DateHour theTo, String theType) {

        DataFiles aFiles = new DataFiles();

        Set<String> someImported = ImportLogDao.getInstance().findFilesByTypeAndTimeRange(theFrom, theTo, theType);

        for (String anImported : someImported) {
            aFiles.add(new DataFile(new S3Url(anImported)));
        }

        return aFiles;
    }

    @Override
    public int deleteByTimeRange(String theType, DateHour theFrom, DateHour theTo) {
        return ImportLogDao.getInstance().deleteByTimeRange(theType, theFrom, theTo);
    }

    @Override
    public void deleteAllByType(String theType) {
        ImportLogDao.getInstance().deleteAllByType(theType);
    }
}
