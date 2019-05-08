package com.ws.ogre.v2.commands.db2file;

import com.ws.common.logging.Logger;
import com.ws.ogre.v2.db.JdbcDbHandler;
import com.ws.ogre.v2.db.JdbcDbHandlerBuilder;
import com.ws.ogre.v2.db.SqlScript;
import com.ws.ogre.v2.datetime.DateHour;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Class querying DB and exporting result to S3 in AVRO format.
 */
public class DbToFileHandler {

    private static final Logger ourLogger = Logger.getLogger();

    private Config myConfig;

    private JdbcDbHandler myDbHandler;

    private Map<String, String> myVars = new HashMap<>();

    public DbToFileHandler(Config theConfig, Map<String, String> theVariables) {
        myConfig = theConfig;

        myDbHandler = JdbcDbHandlerBuilder.getInstance().buildJdbcDbHandler(theConfig.getSrcDbConfig());

        initForDoingQueryForGlobalData(theVariables);
    }

    private void initForDoingQueryForGlobalData(Map<String, String> theVariables) {
        myVars.putAll(System.getenv());
        myVars.putAll(theVariables);
    }

    protected void initForDoingQuery() {
        myVars.put("now", new DateHour(new Date()).format("yyyy-MM-dd HH:mm:ss"));
        myVars.put("nowDate", new DateHour(new Date()).format("yyyy-MM-dd"));
        myVars.put("nowYear", new DateHour(new Date()).format("yyyy"));
        myVars.put("nowMonth", new DateHour(new Date()).format("MM"));
        myVars.put("nowDay", new DateHour(new Date()).format("dd"));
        myVars.put("nowHour", new DateHour(new Date()).format("HH"));
    }

    private void initForDoingQueryForType(String theType) {
        myVars.put("type", theType);
    }

    public void load(String theType, String theOutputPath) {
        ourLogger.info("Execute SQL(s) for type: %s", theType);

        try {
            // Get SQL(s) for type
            Config.Sql aSqlOrScript = myConfig.getSql(theType);
            if (aSqlOrScript == null) {
                throw new IllegalArgumentException("No SQL found for type '" + theType + "' in config");
            }

            // Parse and extracts SQLs from script
            initForDoingQuery();
            initForDoingQueryForType(theType);
            ResultResponse aResponse = loadAsJson(aSqlOrScript);

            FileUtils.writeStringToFile(new File(theOutputPath), aResponse.toJson(), "UTF-8");

        } catch (Exception e) {
            ourLogger.warn("Unable to run sql for type '%s'", theType, e);

            try {
                FileUtils.writeStringToFile(new File(theOutputPath), new ErrorResponse(e.getMessage()).toJson(), "UTF-8");
            } catch (IOException e1) {
                ourLogger.warn("Unable to write error response for type '%s'", theType, e);
                e1.printStackTrace();
            }
        }
    }

    private ResultResponse loadAsJson(Config.Sql aSqlOrScript) throws Exception {
        SqlScript aScript = new SqlScript(aSqlOrScript.sql, myVars);

        // All SQL(s) except the last one in script are for preparing end result, execute them here.
        for (String aSql : aScript.getExecSqls()) {
            ourLogger.debug("Execute: %s", aSql.replace('\n', ' '));
            myDbHandler.execute(aSql);
        }

        // The last SQL is the query returning the records to dump to file.
        ourLogger.debug("Query: %s", aScript.getQuerySql().replace('\n', ' '));
        return executeSql(aScript.getQuerySql());
    }

    private ResultResponse executeSql(String theSql) throws Exception {
        ResultResponse aResponse = new ResultResponse();

        myDbHandler.query(theSql, (ResultSet theRowSet) -> {

            // Create a converter that converts from DB records to avro records.
            RowSetToJson aConverter = new RowSetToJson(theRowSet);

            while (true) {
                // Get DB record and convert it to avro
                ResultResponse.ResultData aRecord = aConverter.next();

                // No more records?
                if (aRecord == null) {
                    break;
                }

                aResponse.addData(aRecord);
            }

            ourLogger.info("Query executed and %s records returned.", aResponse.getData().size());
        });

        return aResponse;
    }
}
