package com.ws.ogre.v2.commands.data2redshift.db;

import com.ws.ogre.v2.data2dbcommon.db.DdlLog;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;

public class DdlLogDao {

    private static DdlLogDao ourInstance = new DdlLogDao();

    public static DdlLogDao getInstance() {
        return ourInstance;
    }


    public void log(String theFilename, String theSql) {

        DdlLog aLog = new DdlLog();

        aLog.filename = theFilename;
        aLog.sql = theSql;

        EntityManager aManager = DbHandler.getInstance().getEntityManager();

        aManager.persist(aLog);
    }

    private List<DdlLog> getAll() {
        try {
            EntityManager aManager = DbHandler.getInstance().getEntityManager();

            return aManager.createNamedQuery("DdlLog.findAll", DdlLog.class)
                    .getResultList();

        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public List<String> getImportedFiles() {
        try {

            List<String> aFiles = new ArrayList<>();

            for (DdlLog aMapping : getAll()) {
                aFiles.add(aMapping.filename);
            }

            return aFiles;

        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

}
