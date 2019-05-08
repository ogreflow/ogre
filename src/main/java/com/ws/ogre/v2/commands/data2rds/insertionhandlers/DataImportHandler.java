package com.ws.ogre.v2.commands.data2rds.insertionhandlers;

import com.ws.ogre.v2.datafile.DataFileHandler.DataFiles;
import com.ws.ogre.v2.datetime.DateHour;

public interface DataImportHandler {

    void markAsImported(DataFiles theMarkImported);

    DataFiles getImportedFiles(DateHour theFrom, DateHour theTo, String theType);

    int deleteByTimeRange(String theType, DateHour theFrom, DateHour theTo);

    void deleteAllByType(String theType);
}
