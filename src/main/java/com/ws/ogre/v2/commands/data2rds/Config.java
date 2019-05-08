package com.ws.ogre.v2.commands.data2rds;

import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2rds.db.PartitionHandler;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/*
 log4j.configuration   = log4j.xml

 src.s3.accessKeyId    =
 src.s3.secretKey      =
 src.s3.rootdir        =
 src.s3.tmpdir         =
 src.s3.ddldir         =

 dst.rds.host     =
 dst.rds.database =
 dst.rds.user     =
 dst.rds.password =

 partition.<type>     = <view-name>:<scheme>:<count> (schemas: yearly, monthly, weekly, daily ,hourly)

Future, replace partition with a more generic:
 type.<type>           = view:<viewname>, partition:<scheme>:<count>, <prop name>:<value>

 */
public class Config {

    private static final String PROP_LOG4J = "log4j.configuration";

    // Type of load. Example,
    // - PLAIN: No column mapping, avro import log etc.
    // - TRACKED (default): With column mapping, import log and ddl log.
    //
    private static final String PROP_LOAD_TYPE = "load.type";

    private static final String PROP_LOAD_FORMAT_TSV_FIELD_SEPARATOR = "load.format.tsv.fieldSeparator";
    private static final String PROP_LOAD_FORMAT_TSV_VALUE_ENCLOSURE = "load.format.tsv.valueEnclosure";

    private static final String PROP_SRC_S3_KEYID = "src.s3.accessKeyId";
    private static final String PROP_SRC_S3_SECRET = "src.s3.secretKey";
    private static final String PROP_SRC_S3_ROOTDIR = "src.s3.rootdir";
    private static final String PROP_SRC_S3_DDLDIR = "src.s3.ddldir";

    // Optional. This column is mainly used to map avro / json property into RDS column.
    // By default ogre expects the avro / json property name 'timestamp' to mean the timestamp column.
    // If we have different name (say, 'created'), then use this property to specify it.
    private static final String PROP_SRC_S3_TIMESTAMP_COLUMN_NAME = "src.s3.timestampColumnName";

    private static final String PROP_DST_RDS_HOST = "dst.rds.host";
    private static final String PROP_DST_RDS_PORT = "dst.rds.port"; // Optional
    private static final String PROP_DST_RDS_DB = "dst.rds.database";
    private static final String PROP_DST_RDS_USER = "dst.rds.user";
    private static final String PROP_DST_RDS_PWD = "dst.rds.password";

    // Optional. This column is mainly used to delete data based on date range.
    // By default ogre expects an RDS column named 'timestamp' to mean the date column.
    // If we have different name (say, 'created'), then use this property to specify it.
    private static final String PROP_DST_RDS_TABLE_TIMESTAMP_COLUMN_NAME = "dst.rds.table.timestampColumnName";

    // Optional.
    private static final String PROP_DST_RDS_TABLE_CONVERT_NULL_VALUE = "dst.rds.table.convertNullValue";

    // Optional. With this, ogre will try to created partition when the date arrives. Example,
    // - partitioning.report_hour_adtraffic = monthly:7
    // - partitioning.report_hour_adevent = monthly:7
    //
    private static final String PROP_TYPE_PARTITION_PREFIX = "partitioning";

    // Optional. Mainly when the type name and destination table name defers. Example,
    // - table.report_hour_adtraffic = RLN2_Report_Hour_AdTraffic
    // - table.report_hour_adevent = RLN2_Report_Hour_AdEvent
    //
    private static final String PROP_TYPE_TABLE_PREFIX = "table";

    public enum LoadType {PLAIN, TRACKED}

    public enum LoadFormat {AVRO, JSON, TSV}

    public String log4jConf;

    public LoadType loadType;

    public String loadFormatTsvFieldSeparator; /* Only application for TSV load format */
    public String loadFormatTsvValueEnclosure; /* Only application for TSV load format */

    public String srcAccessKey;
    public String srcSecret;
    public S3Url srcRootDir;
    public S3Url srcDdlDir;
    public String srcTimestampColumnName;

    public String dstHost;
    public int dstPort;
    public String dstDb;
    public String dstUser;

    public String dstPwd;
    public String dstTimestampColumnName;
    public boolean dstConvertNullValue;

    public PartitionHandler.PartitionTableSpec tableSpec = null;
    public Collection<PartitionHandler.Partitioning> partitionings = new ArrayList<>();

    public static Config load(String theFile) {
        try {

            return new Config(theFile);

        } catch (ConfigurationException e) {
            throw new ConfigException(e);
        }
    }

    public Config(String theFile) throws ConfigurationException {

        PropertiesConfiguration aConf = new PropertiesConfiguration(theFile);

        log4jConf = aConf.getString(PROP_LOG4J, ".");
        loadType = LoadType.valueOf(aConf.getString(PROP_LOAD_TYPE, "TRACKED"));

        loadFormatTsvFieldSeparator = aConf.getString(PROP_LOAD_FORMAT_TSV_FIELD_SEPARATOR);
        loadFormatTsvValueEnclosure = aConf.getString(PROP_LOAD_FORMAT_TSV_VALUE_ENCLOSURE);

        srcAccessKey = aConf.getString(PROP_SRC_S3_KEYID);
        srcSecret = aConf.getString(PROP_SRC_S3_SECRET);
        srcRootDir = getS3Url(aConf, PROP_SRC_S3_ROOTDIR);
        srcDdlDir = getS3Url(aConf, PROP_SRC_S3_DDLDIR);
        srcTimestampColumnName = aConf.getString(PROP_SRC_S3_TIMESTAMP_COLUMN_NAME, "timestamp");

        dstHost = aConf.getString(PROP_DST_RDS_HOST);
        dstPort = aConf.getInt(PROP_DST_RDS_PORT, 3306);
        dstDb = aConf.getString(PROP_DST_RDS_DB);
        dstUser = aConf.getString(PROP_DST_RDS_USER);
        dstPwd = aConf.getString(PROP_DST_RDS_PWD);

        dstTimestampColumnName = aConf.getString(PROP_DST_RDS_TABLE_TIMESTAMP_COLUMN_NAME, "timestamp");
        dstConvertNullValue = aConf.getBoolean(PROP_DST_RDS_TABLE_CONVERT_NULL_VALUE, false);

        partitionings = getPartitioningConf(aConf);
        tableSpec = getTableSpec(aConf);
    }

    private S3Url getS3Url(PropertiesConfiguration theConf, String theKey) {
        if (!theConf.containsKey(theKey)) {
            return null;
        }

        return new S3Url(theConf.getString(theKey));
    }

    private PartitionHandler.PartitionTableSpec getTableSpec(PropertiesConfiguration theConf) {
        PartitionHandler.PartitionTableSpec tableSpec = new PartitionHandler.PartitionTableSpec();

        Iterator<String> aKeys = theConf.getKeys(PROP_TYPE_TABLE_PREFIX);
        while (aKeys.hasNext()) {

            String aKey = aKeys.next();
            String aType = aKey.substring(aKey.indexOf('.') + 1);
            String aTableName = theConf.getString(aKey);

            tableSpec.addSpec(aType, aTableName);
        }

        return tableSpec;
    }

    private Collection<PartitionHandler.Partitioning> getPartitioningConf(PropertiesConfiguration theConf) {
        Collection<PartitionHandler.Partitioning> somePartitionings = new ArrayList<>();

        Iterator<String> aKeys = theConf.getKeys(PROP_TYPE_PARTITION_PREFIX);
        while (aKeys.hasNext()) {

            String aKey = aKeys.next();
            String aVal = theConf.getString(aKey);

            somePartitionings.add(createPartitioning(aKey, aVal));
        }

        return somePartitionings;
    }

    private PartitionHandler.Partitioning createPartitioning(String theKey, String theValue) {

        String aType = theKey.substring(theKey.indexOf('.') + 1);

        String[] aValParts = theValue.split(":");
        String aScheme = aValParts[0];
        String aCount = aValParts[1];

        return new PartitionHandler.Partitioning(aType, aScheme, Integer.valueOf(aCount));
    }

    public static class ConfigException extends RuntimeException {
        public ConfigException(Throwable theCause) {
            super(theCause);
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "log4jConf=" + log4jConf +
                ", loadType=" + loadType +
                ", srcAccessKey=" + srcAccessKey +
                ", srcSecret=" + "***" +
                ", srcRootDir=" + srcRootDir +
                ", srcDdlDir=" + srcDdlDir +
                ", srcTimestampColumnName=" + srcTimestampColumnName +
                ", dstHost=" + dstHost +
                ", dstPort=" + dstPort +
                ", dstDb=" + dstDb +
                ", dstUser=" + dstUser +
                ", dstPwd=" + "***" +
                ", dstTimestampColumnName=" + dstTimestampColumnName +
                ", dstConvertNullValue=" + dstConvertNullValue +
                ", tableSpec=" + tableSpec +
                ", partitioning=" + partitionings +
                '}';
    }
}