package com.ws.ogre.v2.commands.db2file;

import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

        log4jConf = aConf.getString("log4j.configuration");

        srcDbType = JdbcDbHandler.DbType.valueOf(aConf.getString("src.db.type", "mysql").toUpperCase());
        srcDbHost = aConf.getString("src.db.host");
        srcDbPort = aConf.getInteger("src.db.port", null);
        srcDbDatabase = aConf.getString("src.db.database");
        srcDbUser = aConf.getString("src.db.user");
        srcDbPassword = aConf.getString("src.db.password");
        srcDbStagingDir = aConf.getString("src.db.stagingDir");

        types = aConf.getStringArray("types");

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
                ", sqls=" + sqls +
                '}';
    }
}
