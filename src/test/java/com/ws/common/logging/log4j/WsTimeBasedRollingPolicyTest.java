package com.ws.common.logging.log4j;

import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;

public class WsTimeBasedRollingPolicyTest {

    static {
        //Remove any log file
        File aLogDir = new File(".");

        String[] aList = aLogDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("server.log");
            }
        });

        for( int i = 0; i < aList.length; i++ ) {
            System.out.println("Removing: "+ aList[i]);
            new File(aList[i]).delete();
        }

        System.out.println("Removed all existing log files");
    }

    private org.apache.log4j.Logger ourLogger = org.apache.log4j.Logger.getLogger("ws.test");

//    @Before
    public void prepare() {

    }


    @Test
//    @Ignore
    public void testRoll() throws InterruptedException {
        ourLogger.info("Start");
        Thread.sleep(1100);
        ourLogger.info("1");
        for( int i = 0; i < 40; i++ ) {
            ourLogger.info(i);
            Thread.sleep(500);
        }
        ourLogger.info("Done");
//        synchronized(  )

        Thread.sleep(5000);
    }
}
