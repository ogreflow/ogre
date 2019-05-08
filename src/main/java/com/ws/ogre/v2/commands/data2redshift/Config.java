package com.ws.ogre.v2.commands.data2redshift;

import com.ws.ogre.v2.aws.S3Url;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.*;

/*
 log4j.configuration   = log4j.xml

 src.s3.accessKeyId    =
 src.s3.secretKey      =
 src.s3.rootdir        =
 src.s3.tmpdir         =
 src.s3.ddldir         =

 dst.redshift.host     =
 dst.redshift.database =
 dst.redshift.schema   =
 dst.redshift.user     =
 dst.redshift.password =


 partition.<type>     = <view-name>:<scheme>:<count> (schemas: yearly, monthly, weekly, daily ,hourly)

Future, replace partition with a more generic:
 type.<type>           = view:<viewname>, partition:<scheme>:<count>, <prop name>:<value>

 */
public class Config {

    private static final String PROP_LOG4J               = "log4j.configuration";

    private static final String PROP_SRC_S3_KEYID        = "src.s3.accessKeyId";
    private static final String PROP_SRC_S3_SECRET       = "src.s3.secretKey";
    private static final String PROP_SRC_S3_ROOTDIR      = "src.s3.rootdir";
    private static final String PROP_SRC_S3_TMPDIR       = "src.s3.tmpdir";
    private static final String PROP_SRC_S3_DDLDIR       = "src.s3.ddldir";

    private static final String PROP_DST_REDSHIFT_HOST   = "dst.redshift.host";
    private static final String PROP_DST_REDSHIFT_PORT   = "dst.redshift.port"; // Optional
    private static final String PROP_DST_REDSHIFT_DB     = "dst.redshift.database";
    private static final String PROP_DST_REDSHIFT_SCHEMA = "dst.redshift.schema";
    private static final String PROP_DST_REDSHIFT_USER   = "dst.redshift.user";
    private static final String PROP_DST_REDSHIFT_PWD    = "dst.redshift.password";

    private static final String PROP_TYPE_PARTITION_PREFIX = "partitioning";


    public String log4jConf;

    public String srcAccessKey;
    public String srcSecret;
    public S3Url  srcRootDir;
    public S3Url  srcTmpDir;
    public S3Url  srcDdlDir;

    public String dstHost;
    public int dstPort;
    public String dstDb;
    public String dstSchema;
    public String dstUser;
    public String dstPwd;

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

        log4jConf     = aConf.getString(PROP_LOG4J, ".");

        srcAccessKey  = aConf.getString(PROP_SRC_S3_KEYID);
        srcSecret     = aConf.getString(PROP_SRC_S3_SECRET);
        srcRootDir    = new S3Url(aConf.getString(PROP_SRC_S3_ROOTDIR));
        srcTmpDir     = new S3Url(aConf.getString(PROP_SRC_S3_TMPDIR));
        srcDdlDir     = new S3Url(aConf.getString(PROP_SRC_S3_DDLDIR));

        dstHost       = aConf.getString(PROP_DST_REDSHIFT_HOST);
        dstPort       = aConf.getInt(PROP_DST_REDSHIFT_PORT, 5439);
        dstDb         = aConf.getString(PROP_DST_REDSHIFT_DB);
        dstSchema     = aConf.getString(PROP_DST_REDSHIFT_SCHEMA);
        dstUser       = aConf.getString(PROP_DST_REDSHIFT_USER);
        dstPwd        = aConf.getString(PROP_DST_REDSHIFT_PWD);

        Iterator<String> aKeys = aConf.getKeys(PROP_TYPE_PARTITION_PREFIX);

        while (aKeys.hasNext()) {

            String aKey = aKeys.next();
            String aVal = aConf.getString(aKey);

            PartitionHandler.Partitioning aPartitioning = createPartitioning(aKey, aVal);

            partitionings.add(aPartitioning);
        }
    }

    private PartitionHandler.Partitioning createPartitioning(String theKey, String theValue) {

        String[] aValParts = theValue.split(":");

        String aType = theKey.substring(theKey.indexOf('.') + 1);
        String aView = aValParts[0];
        String aSchema = aValParts[1];
        String aCount = aValParts[2];

        return new PartitionHandler.Partitioning(aType, aView, aSchema, Integer.valueOf(aCount));
    }

    public static class ConfigException extends RuntimeException {
        public ConfigException(Throwable theCause) {
            super(theCause);
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "log4jConf='" + log4jConf + '\'' +
                ", srcAccessKey='" + srcAccessKey + '\'' +
                ", srcRootDir=" + srcRootDir +
                ", srcTmpDir=" + srcTmpDir +
                ", srcDdlDir=" + srcDdlDir +
                ", dstHost='" + dstHost + '\'' +
                ", dstDb='" + dstDb + '\'' +
                ", dstSchema='" + dstSchema + '\'' +
                ", dstUser='" + dstUser + '\'' +
                ", partitioning=" + partitionings +
                '}';
    }


}