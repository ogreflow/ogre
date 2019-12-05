package com.ws.ogre.v2.commands.data2redshift;

import com.google.gson.Gson;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.commands.data2redshift.db.ColumnMappingDao;
import com.ws.ogre.v2.commands.data2redshift.db.DdlLogDao;
import com.ws.ogre.v2.commands.data2redshift.db.RedShiftDao;
import com.ws.ogre.v2.data2dbcommon.db.ColumnMapping;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaHandler {
    private static final Logger ourLogger = Logger.getLogger();

    private S3Client myS3Client;

    private S3BetterUrl myMappingsDir;
    private S3BetterUrl myDdlDir;

    private PartitionHandler myPartitionHandler;


    public SchemaHandler(S3BetterUrl theDdlDir, S3BetterUrl theMappingDir, String theAccessKeyId, String theSecretKey, PartitionHandler thePartitionHandler) {
        myDdlDir = theDdlDir;
        myMappingsDir = theMappingDir;

        myS3Client = new S3Client(theAccessKeyId, theSecretKey);

        myPartitionHandler = thePartitionHandler;
    }

    public boolean syncDdls(boolean theForceCreateMappingFiles) {

        // Detect new DDL files not imported yet

        DdlFiles aS3Files = getMasterDdlFiles();
        DdlFiles aDbFiles = getImportedDdlFiles();

        aS3Files.removeAll(aDbFiles);

        // Nothing new to import ?
        if (aS3Files.isEmpty()) {

            if (theForceCreateMappingFiles) {
                generateMappings();
            }

            return false;
        }

        ourLogger.info("Syncing DB DDL file(s)");
        ourLogger.info("Found %s new DDLs to import", aS3Files.size());

        // Import all new DDLs
        for (String aFile : aS3Files) {
            importDdl(aFile);
        }

        // Update all column mappings to reflect new changes
        generateMappings();

        ourLogger.info("Done syncing DB DDL file(s)");

        return true;
    }

    private DdlFiles getMasterDdlFiles() {

        List<S3BetterUrl> aFiles = myS3Client.listObjects(myDdlDir);

        DdlFiles aDdlFiles = new DdlFiles();

        for (S3BetterUrl aFile : aFiles) {
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

        // Remove all comments
        aDdl = aDdl + "\n";
        aDdl = aDdl.replaceAll("//.*\n", "");

        aDdl = aDdl.substring(0, aDdl.lastIndexOf(";"));

        Set<String> aRecreateViewsFor = new HashSet<>();

        String[] aSqls = aDdl.split(";");

        for (String aSql : aSqls) {

            // We need to alter all partitioned tables if this sql alters a partitioned type.
            Set<String> aPartitionAwareSqls = createSqlsForAllPartitions(aSql);

            for (String aPartitionAwareSql : aPartitionAwareSqls) {
                ourLogger.info("Execute: %s", aPartitionAwareSql);
                RedShiftDao.getInstance().executeUpdate(aPartitionAwareSql);
            }

            // If a partitioned type is altered then its view needs to be recreated
            String aType = getAlteredPartitionedType(aSql);

            if (aType != null) {
                aRecreateViewsFor.add(aType);
            }
        }

        for (String aType : aRecreateViewsFor) {
            ourLogger.info("Altered type '%s' is partitioned, recreate view to expose new changes", aType);
            myPartitionHandler.recreatePartitionView(aType);
        }

        ourLogger.info("Log ddl change");

        DdlLogDao.getInstance().log(theFile, aDdl);
    }

    private String getAlteredPartitionedType(String theSql) {

        // Create pattern for capture table name from expressions like:
        // ALTER TABLE apirequest ADD COLUMN serverId VARCHAR(40) encode lzo;
        Pattern aP = Pattern.compile("\\s*alter\\s+table\\s+(\\\"?[a-zA-Z0-9_$]*\\\"?\\.)?\\\"?([a-zA-Z0-9_$]*)\\\"?\\s+.*", Pattern.CASE_INSENSITIVE);
        Matcher aM = aP.matcher(theSql);

        // Does sql contain alter table ?
        if (!aM.matches() || aM.groupCount() != 2) {
            return null;
        }

        // Ok we have table ddl change...

        // Extract table name in sql
        String aType = aM.group(2);

        // Is type partitioned?
        if (!myPartitionHandler.isPartitioned(aType)) {
            return null;
        }

        return aType;
    }

    private Set<String> createSqlsForAllPartitions(String theSql) {

        String aType = getAlteredPartitionedType(theSql);

        // Does sql alter any partitioned type ?
        if (aType == null) {
            return Collections.singleton(theSql);
        }

        // Get all existing partitions for table
        Set<String> aPartitions = myPartitionHandler.getPartitionTables(aType);

        Set<String> someSqls = new HashSet<>();

        // Add ddl change for blue print table
        someSqls.add(theSql);

        int aStartPos = theSql.toLowerCase().indexOf(aType.toLowerCase());

        // Create sqls applying ddl change for all partitions
        for (String aPartition : aPartitions) {
            String aSql = theSql.substring(0, aStartPos);
            aSql += " " + aPartition;
            aSql += theSql.substring(aStartPos + aType.length());
            someSqls.add(aSql);
        }

        return someSqls;
    }

    private void generateMappings() {

        // Get all mappings to compare with old ones...
        Set<String> aTables = ColumnMappingDao.getInstance().getTables();

        // Resolve the updated tables to recreate mapping files for...
        for (String aTable : aTables) {

            List<ColumnMapping> aDbMappings = ColumnMappingDao.getInstance().getMappings(aTable);

            Collections.sort(aDbMappings, new Comparator<ColumnMapping>() {
                public int compare(ColumnMapping the1, ColumnMapping the2) {
                    return the1.id - the2.id;
                }
            });

            Mappings aMappings = new Mappings();

            for (ColumnMapping aDbMapping : aDbMappings) {
                aMappings.jsonpaths.add(aDbMapping.jsonpath);
            }

            String aJson = new Gson().toJson(aMappings);

            ourLogger.info("Write mappings to %s/%s.json: %s", myMappingsDir, aTable, aJson);

            myS3Client.putObject(myMappingsDir.bucket, myMappingsDir.key + "/" + aTable + ".json", aJson);
        }
    }


    private class DdlFiles extends ArrayList<String> {
        public DdlFiles() {
        }

        public DdlFiles(Collection<? extends String> theFiles) {
            super(theFiles);
        }
    }

    private class Mappings {
        List<String> jsonpaths = new ArrayList<>();
    }
}