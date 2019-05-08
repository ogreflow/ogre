package com.ws.ogre.v2.commands.data2rds;

import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.aws.S3Url;
import com.ws.ogre.v2.commands.data2rds.Config;
import com.ws.ogre.v2.db.JdbcDbHandler;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;

/**
 * Botstraps the Ogre schema and tables in an empty Rds database.
 */
public class InitHandler {

    private Config myConfig;

    private S3Client myS3Client;

    private JdbcDbHandler myDbHandler;

    public InitHandler(Config theConfig) {
        myConfig = theConfig;

        myS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);

        myDbHandler = new JdbcDbHandler(JdbcDbHandler.DbType.MYSQL, myConfig.dstHost, myConfig.dstPort, myConfig.dstDb, myConfig.dstUser, myConfig.dstPwd);
    }

    public void bootstrap() throws Exception {
        try {

            System.out.println("Ogre pipeline Bootstraping");
            System.out.println("--------------------------");

            ping();
            testS3(myConfig.srcRootDir);

            // For tracked load we need to setup ogre's tracking tables.
            if (myConfig.loadType == Config.LoadType.TRACKED) {
                setupSchema();
                testS3(myConfig.srcDdlDir);
            }

            System.out.println("Ogre successfully bootstrapped!");

        } catch (Exception e) {
            throw e;
        }
    }

    private void testS3(S3Url theUrl) {
        try {

            System.out.println("Testing access to: " + theUrl);
            myS3Client.listObjects(theUrl, 1);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read from: " + theUrl, e);
        }
    }

    private void ping() throws Exception {
        System.out.println("Testing RDS connection...");
        myDbHandler.execute("SELECT NOW()");
    }

    private void setupSchema() throws Exception {
        System.out.println("Setting up Ogre schema...");

        InputStream anIn = ClassLoader.getSystemResourceAsStream("schema/ogre-schema-rds.sql");

        if (anIn == null) {
            throw new RuntimeException("Failed to locate ogre-schema-rds.sql");
        }

        String aSchema = IOUtils.toString(anIn);

        int aPos = aSchema.lastIndexOf(";");

        if (aPos < 0) {
            throw new RuntimeException("No rows to execute ?");
        }

        aSchema = aSchema.substring(0, aPos);

        String[] aSqls = aSchema.split(";");

        for (String aSql : aSqls) {
            myDbHandler.executeUpdate(aSql);
        }
    }
}
