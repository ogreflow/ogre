package com.ws.ogre.v2.aws;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.db.JdbcDbHandler;

public class MysqlClient {

    private static final Logger ourLogger = Logger.getLogger();

    private JdbcDbHandler myDbHandler;

    public MysqlClient(String theHost, Integer thePort, String theDatabase, String theUser, String thePassword) {
        ourLogger.info("Initiate the mysql driver: %s, %s, %s, %s", theHost, thePort, theDatabase, theUser);

        myDbHandler = new JdbcDbHandler(JdbcDbHandler.DbType.MYSQL, theHost, thePort, theDatabase, theUser, thePassword);
    }

    public JdbcDbHandler getDbHandler() {
        return myDbHandler;
    }
}
