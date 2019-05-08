package com.ws.ogre.v2.commands.data2rds.insertionhandlers.managed;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2rds.db.ColumnMappingDao;
import com.ws.ogre.v2.commands.data2rds.db.DdlLogDao;
import com.ws.ogre.v2.commands.data2rds.db.RdsDao;
import com.ws.ogre.v2.commands.data2rds.insertionhandlers.SchemaHandler;
import com.ws.ogre.v2.data2dbcommon.db.ColumnMapping;
import com.ws.ogre.v2.db.SqlUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;

import java.util.*;

public class ManagedSchemaHandler implements SchemaHandler {
    private static final Logger ourLogger = Logger.getLogger();

    private S3Client myS3Client;

    private S3Url myDdlDir;

    public ManagedSchemaHandler(S3Url theDdlDir, String theAccessKeyId, String theSecretKey) {
        myDdlDir = theDdlDir;

        myS3Client = new S3Client(theAccessKeyId, theSecretKey);
    }

    @Override
    public boolean syncDdls() {

        // Detect new DDL files not imported yet

        DdlFiles aS3Files = getMasterDdlFiles();
        DdlFiles aDbFiles = getImportedDdlFiles();

        aS3Files.removeAll(aDbFiles);

        // Nothing new to import ?
        if (aS3Files.isEmpty()) {
            return false;
        }

        ourLogger.info("Syncing DB DDL file(s)");
        ourLogger.info("Found %s new DDLs to import", aS3Files.size());

        // Import all new DDLs
        for (String aFile : aS3Files) {
            importDdl(aFile);
        }

        ourLogger.info("Done syncing DB DDL file(s)");

        return true;
    }

    @Override
    public List<String> getJsonMappingsSequence(String theTableName) {
        List<String> someJsonKeys = new ArrayList<>();

        List<ColumnMapping> aDbMappings = ColumnMappingDao.getInstance().getMappings(theTableName);

        Collections.sort(aDbMappings, new Comparator<ColumnMapping>() {
            public int compare(ColumnMapping the1, ColumnMapping the2) {
                return the1.id - the2.id;
            }
        });

        for (ColumnMapping aDbMapping : aDbMappings) {
            someJsonKeys.add(aDbMapping.jsonpath);
        }

        return someJsonKeys;
    }

    @Override
    public Set<String> getExistingAllTypes() {
        Set<String> someTables = RdsDao.getInstance().getAllTablesInSchema();

        // Remove ogre's tables.
        CollectionUtils.filter(someTables, new Predicate() {
            @Override
            public boolean evaluate(Object theTableName) {
                if (theTableName == null) {
                    return false;
                }

                return !theTableName.toString().startsWith("ogre");
            }
        });

        return someTables;
    }

    private DdlFiles getMasterDdlFiles() {

        List<S3Url> aFiles = myS3Client.listObjects(myDdlDir);

        DdlFiles aDdlFiles = new DdlFiles();

        for (S3Url aFile : aFiles) {
            String aKey = aFile.key;

            if (!aKey.endsWith(".ddl")) {
                ourLogger.info("WARNING: Found file without .ddl extension, skipping it: %s", aKey);
                continue;
            }

            aDdlFiles.add(aKey);
        }

        return aDdlFiles;
    }

    private DdlFiles getImportedDdlFiles() {

        List<String> aDdlFiles = DdlLogDao.getInstance().getImportedFiles();

        return new DdlFiles(aDdlFiles);
    }

    private void importDdl(String theFile) {

        ourLogger.info("Import: %s", theFile);

        String aDdl = myS3Client.getObjectAsString(myDdlDir.bucket, theFile);

        aDdl = SqlUtils.removeComments(aDdl);

        String[] aSqls = aDdl.substring(0, aDdl.lastIndexOf(";")).split(";");
        for (String aSql : aSqls) {
            ourLogger.info("Execute: %s", aSql);
            RdsDao.getInstance().executeUpdate(aSql);
        }

        ourLogger.info("Log ddl change: filename=%s, sql=%s", theFile, aDdl);

        DdlLogDao.getInstance().log(theFile, aDdl);
    }

    private class DdlFiles extends ArrayList<String> {
        public DdlFiles() {
        }

        public DdlFiles(Collection<? extends String> theFiles) {
            super(theFiles);
        }
    }
}