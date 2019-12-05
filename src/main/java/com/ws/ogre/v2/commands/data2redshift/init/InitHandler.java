package com.ws.ogre.v2.commands.data2redshift.init;

import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.aws.S3Client;
import com.ws.ogre.v2.commands.data2redshift.Config;

/**
 * Botstraps the Ogre schema and tables in an empty Redshift database.
 */
public class InitHandler {

    private Config myConfig;

    private S3Client myS3Client;


    public InitHandler(Config theConfig) {
        myConfig = theConfig;

        myS3Client = new S3Client(theConfig.srcAccessKey, theConfig.srcSecret);
    }

    private void testS3(S3BetterUrl theUrl) {
        try {

            System.out.println("Testing access to: " + theUrl);
            myS3Client.listObjects(theUrl, 1);

        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read from: " + theUrl, e);
        }
    }

    public void bootstrap() throws Exception {
        try {

            System.out.println("Ogre pipeline Bootstraping");
            System.out.println("--------------------------");

            DbHandler aDbHandler = new DbHandler(myConfig.dstHost, myConfig.dstPort, myConfig.dstDb, myConfig.dstSchema, myConfig.dstUser, myConfig.dstPwd);

            aDbHandler.ping();
            aDbHandler.setupSchema();

            testS3(myConfig.srcRootDir);
            testS3(myConfig.srcDdlDir);
            testS3(myConfig.srcTmpDir);

            System.out.println("Ogre successfully bootstrapped!");

        } catch (Exception e) {
            throw e;
        }
    }

}
