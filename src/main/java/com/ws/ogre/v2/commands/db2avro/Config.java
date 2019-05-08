package com.ws.ogre.v2.commands.db2avro;

import com.amazonaws.services.s3.model.StorageClass;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.*;

/*
 log4j.configuration = log4j.xml

 types               = apa, banan

 <type>.sql          =

 src.db.type         = mysql (optional, default mysql)
 src.db.host         = apa.mysql.com
 src.db.port         = 3306 (optional, default mysql - 3306, redshift - 5439)
 src.db.database     = apa
 src.db.user         = apa
 src.db.password     = banan

 dst.s3.accessKeyId  = ...
 dst.s3.secretKey    = ...
 dst.s3.rootPath     = s3://...
 dst.s3.storageClass = (STANDARD, STANDARD_IA, REDUCED_REDUNDANCY, GLACIER - default is STANDARD_IA)

 */
public class Config {

    public String log4jConf;

    public String[] types;

    public JdbcDbHandler.DbType srcDbType;
    public String srcDbHost;
    public Integer srcDbPort;
    public String srcDbDatabase;
    public String srcDbUser;
    public String srcDbPassword;
    public String srcDbStagingDir;

    public String dstAccessKey;
    public String dstSecret;
    public S3Url  dstRoot;
    public String dstComponent;
    public String dstSource;
    public StorageClass dstStorageClass;
    public StorageType dstStorageType;

    public String localAvroRoot;

    public Map<String, Sql> sqls = new HashMap<>();


    public static Config load(String theFile) {
        try {

            return new Config(theFile);

        } catch (ConfigurationException e) {
            throw new ConfigException(e);
        }
    }

    public Config(String theFile) throws ConfigurationException {

        PropertiesConfiguration aConf = new PropertiesConfiguration(theFile);

        log4jConf      = aConf.getString("log4j.configuration");

        srcDbType      = JdbcDbHandler.DbType.valueOf(aConf.getString("src.db.type", "mysql").toUpperCase());
        srcDbHost      = aConf.getString("src.db.host");
        srcDbPort      = aConf.getInteger("src.db.port", null);
        srcDbDatabase  = aConf.getString("src.db.database");
        srcDbUser      = aConf.getString("src.db.user");
        srcDbPassword  = aConf.getString("src.db.password");
        srcDbStagingDir = aConf.getString("src.db.stagingDir");

        dstAccessKey    = aConf.getString("dst.s3.accessKeyId");
        dstSecret       = aConf.getString("dst.s3.secretKey");
        dstRoot         = new S3Url(aConf.getString("dst.s3.rootPath"));
        dstComponent    = aConf.getString("dst.s3.component");
        dstSource       = aConf.getString("dst.s3.source");
        dstStorageClass = StorageClass.fromValue(aConf.getString("dst.s3.storageClass", "STANDARD_IA"));
        dstStorageType  = StorageType.fromValue(aConf.getString("dst.s3.storageType", "avro"));

        localAvroRoot  = aConf.getString("local.avro.root");

        types          = aConf.getStringArray("types");

        aConf.setDelimiterParsingDisabled(true);
        aConf.refresh();

        for (String aType : types) {

            String aName = aType + ".sql";

            Sql aSql = new Sql();

            aSql.type = aType;
            aSql.sql = aConf.getString(aName);

            sqls.put(aType, aSql);
        }

        aConf.setDelimiterParsingDisabled(false);
        aConf.refresh();
    }

    public Sql getSql(String theType) {
        return sqls.get(theType);
    }

    public String getDstS3FullPath() {
        return "s3://" + dstRoot.bucket + "/" + dstStorageType.getTypeId() + "/" + dstComponent + "/" + dstSource;
    }

    public JdbcDbHandlerBuilder.JdbcDbHandlerConfig getSrcDbConfig() {
        return new JdbcDbHandlerBuilder.JdbcDbHandlerConfig() {
            @Override
            public JdbcDbHandler.DbType getDbType() {
                return srcDbType;
            }

            @Override
            public String getDbHost() {
                return srcDbHost;
            }

            @Override
            public Integer getDbPort() {
                return srcDbPort;
            }

            @Override
            public String getDbDatabaseName() {
                return srcDbDatabase;
            }

            @Override
            public String getDbUser() {
                return srcDbUser;
            }

            @Override
            public String getDbPassword() {
                return srcDbPassword;
            }

            @Override
            public String getDbStagingDir() {
                return srcDbStagingDir;
            }
        };
    }

    public static class Sql {
        String sql;
        String type;
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
                ", types=" + Arrays.toString(types) +
                ", srcDbType=" + srcDbType +
                ", srcDbHost=" + srcDbHost +
                ", srcDbPort=" + srcDbPort +
                ", srcDbDatabase=" + srcDbDatabase +
                ", srcDbUser=" + srcDbUser +
                ", srcDbPassword=" + "***" +
                ", srcDbStagingDir=" + srcDbStagingDir +
                ", dstAccessKey=" + dstAccessKey +
                ", dstSecret=" + "***" +
                ", dstRoot=" + dstRoot +
                ", dstComponent=" + dstComponent +
                ", dstSource=" + dstSource +
                ", dstStorageClass=" + dstStorageClass +
                ", dstStorageType=" + dstStorageType +
                ", localAvroRoot=" + localAvroRoot +
                ", sqls=" + sqls +
                '}';
    }
}
