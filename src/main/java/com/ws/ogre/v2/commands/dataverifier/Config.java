package com.ws.ogre.v2.commands.dataverifier;

import com.google.gson.GsonBuilder;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Config {

    public String log4jConf;

    public JdbcDbHandler.DbType refDbType;
    public String refDbHost;
    public int refDbPort;
    public String refDbDbName;
    public String refDbUser;
    public String refDbPassword;
    public String refDbStagingDir;

    public JdbcDbHandler.DbType testDbType;
    public String testDbHost;
    public int testDbPort;
    public String testDbDbName;
    public String testDbUser;
    public String testDbPassword;
    public String testDbStagingDir;

    public List<SingleVerificationDetail> verificationDetails = new ArrayList<>();

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

        refDbType = JdbcDbHandler.DbType.valueOf(aConf.getString("ref.db.type", "redshift").toUpperCase());
        refDbHost = aConf.getString("ref.db.host");
        refDbPort = aConf.getInt("ref.db.port", 5439);
        refDbDbName = aConf.getString("ref.db.database");
        refDbUser = aConf.getString("ref.db.user");
        refDbPassword = aConf.getString("ref.db.password");
        refDbStagingDir = aConf.getString("ref.db.stagingDir");

        testDbType = JdbcDbHandler.DbType.valueOf(aConf.getString("test.db.type", "mysql").toUpperCase());
        testDbHost = aConf.getString("test.db.host");
        testDbPort = aConf.getInt("test.db.port", 3306);
        testDbDbName = aConf.getString("test.db.database");
        testDbUser = aConf.getString("test.db.user");
        testDbPassword = aConf.getString("test.db.password");
        testDbStagingDir = aConf.getString("test.db.stagingDir");

        initVerificationDetails(aConf);
    }

    private void initVerificationDetails(PropertiesConfiguration aConf) throws ConfigurationException {
        aConf.setDelimiterParsingDisabled(true);
        aConf.refresh();

        Iterator<String> aKeys = aConf.getKeys("compare.ref.sql");
        while (aKeys.hasNext()) {
            String aKey = aKeys.next();
            String aSingleVerificationName = aKey.substring("compare.ref.sql".length() + 1);

            verificationDetails.add(new SingleVerificationDetail(
                    aSingleVerificationName,
                    aConf.getString("compare.ref.sql" + "." + aSingleVerificationName),
                    aConf.getString("compare.test.sql" + "." + aSingleVerificationName),
                    getErrorTolerances(aConf.getString("compare.tolerance" + "." + aSingleVerificationName))
            ));
        }

        aConf.setDelimiterParsingDisabled(false);
        aConf.refresh();
    }

    private List<MismatchTolerance> getErrorTolerances(String theToleranceSpec) {
        List<MismatchTolerance> someTolerances = new ArrayList<>();

        for (String aToleranceSpec : theToleranceSpec.split(",\\s*")) {
            someTolerances.add(MismatchTolerance.getToleranceFromSpec(aToleranceSpec));
        }

        return someTolerances;
    }

    public JdbcDbHandlerBuilder.JdbcDbHandlerConfig getRefDbConfig() {
        return new JdbcDbHandlerBuilder.JdbcDbHandlerConfig() {
            @Override
            public JdbcDbHandler.DbType getDbType() {
                return refDbType;
            }

            @Override
            public String getDbHost() {
                return refDbHost;
            }

            @Override
            public Integer getDbPort() {
                return refDbPort;
            }

            @Override
            public String getDbDatabaseName() {
                return refDbDbName;
            }

            @Override
            public String getDbUser() {
                return refDbUser;
            }

            @Override
            public String getDbPassword() {
                return refDbPassword;
            }

            @Override
            public String getDbStagingDir() {
                return refDbStagingDir;
            }
        };
    }

    public JdbcDbHandlerBuilder.JdbcDbHandlerConfig getTestDbConfig() {
        return new JdbcDbHandlerBuilder.JdbcDbHandlerConfig() {
            @Override
            public JdbcDbHandler.DbType getDbType() {
                return testDbType;
            }

            @Override
            public String getDbHost() {
                return testDbHost;
            }

            @Override
            public Integer getDbPort() {
                return testDbPort;
            }

            @Override
            public String getDbDatabaseName() {
                return testDbDbName;
            }

            @Override
            public String getDbUser() {
                return testDbUser;
            }

            @Override
            public String getDbPassword() {
                return testDbPassword;
            }

            @Override
            public String getDbStagingDir() {
                return testDbStagingDir;
            }
        };
    }

    public static class ConfigException extends RuntimeException {
        public ConfigException(Throwable theCause) {
            super(theCause);
        }
    }

    @Override
    public String toString() {
        return new GsonBuilder().setPrettyPrinting().create().toJson(this);
    }
}
