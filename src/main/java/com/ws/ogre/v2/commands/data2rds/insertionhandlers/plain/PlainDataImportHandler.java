package com.ws.ogre.v2.commands.data2rds.insertionhandlers.plain;

import com.ws.ogre.v2.commands.data2rds.insertionhandlers.DataImportHandler;
import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datetime.DateHour;

public class PlainDataImportHandler implements DataImportHandler {

    public PlainDataImportHandler() {
    }

    @Override
    public void markAsImported(DataFiles theMarkImported) {
    }

    @Override
    public DataFiles getImportedFiles(DateHour theFrom, DateHour theTo, String theType) {
        return new DataFiles();
    }

    @Override
    public int deleteByTimeRange(String theType, DateHour theFrom, DateHour theTo) {
        return 0;
    }

    @Override
    public void deleteAllByType(String theType) {
    }
}
