package com.ws.ogre.v2.db;

import com.ws.ogre.v2.aws.AthenaClient;
import com.ws.ogre.v2.aws.MysqlClient;
import com.ws.ogre.v2.aws.RedshiftClient;

public class JdbcDbHandlerBuilder {
    private static JdbcDbHandlerBuilder ourInstance = new JdbcDbHandlerBuilder();

    private JdbcDbHandlerBuilder() {
    }

    public static JdbcDbHandlerBuilder getInstance() {
        return ourInstance;
    }

    public JdbcDbHandler buildJdbcDbHandler(JdbcDbHandlerConfig theConfig) {
        if (theConfig.getDbType() == JdbcDbHandler.DbType.MYSQL) {
            return new MysqlClient(
                    theConfig.getDbHost(),
                    theConfig.getDbPort(),
                    theConfig.getDbDatabaseName(),
                    theConfig.getDbUser(),
                    theConfig.getDbPassword()
            ).getDbHandler();
        }

        if (theConfig.getDbType() == JdbcDbHandler.DbType.REDSHIFT) {
            return new RedshiftClient(
                    theConfig.getDbHost(),
                    theConfig.getDbPort(),
                    theConfig.getDbDatabaseName(),
                    theConfig.getDbUser(),
                    theConfig.getDbPassword()
            ).getDbHandler();
        }

        if (theConfig.getDbType() == JdbcDbHandler.DbType.ATHENA) {
            return new AthenaClient(
                    theConfig.getDbHost(),
                    theConfig.getDbPort(),
                    theConfig.getDbUser(),
                    theConfig.getDbPassword(),
                    theConfig.getDbStagingDir()
            ).getDbHandler();
        }

        throw new IllegalArgumentException("Unknown source db type: " + theConfig.getDbType());
    }

    public interface JdbcDbHandlerConfig {

        JdbcDbHandler.DbType getDbType();

        String getDbHost();

        Integer getDbPort();

        String getDbDatabaseName();

        String getDbUser();

        String getDbPassword();

        String getDbStagingDir();
    }
}
