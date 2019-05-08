package com.ws.ogre.v2.utils;

import com.amazonaws.services.sns.model.PublishRequest;
import com.ws.ogre.v2.aws.SnsClient;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/*
    Log4j appender that can push log messages to AWS SNS.

    With inspiration from com.twitsprout.appender.sns.SnsAsyncAppender.

    <appender name="sns" class="com.ws.ogre.v2.utils.SnsAsyncAppender">
       <param name="threshold" value="WARN"/>
       <param name="topicArn" value="arn:aws:sns:eu-west-1:xxx:xxx"/>
       <param name="subject" value="My Alarm"/>
       <param name="awsKeyId" value="xxx"/>
       <param name="awsSecretKey" value="xxx"/>

        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="%p %t %c - %m%n"/>
        </layout>
     </appender>
*/
public class SnsAsyncAppender extends AppenderSkeleton {

    private String myTopicArn;
    private String myAwsKeyId;
    private String myAwsSecretKey;
    private String mySubject;

    private SnsClient mySnsClient;

    private boolean iAmInitialized = false;
    private boolean mySnsClosed = true;


    public SnsAsyncAppender() {
    }

    public void setTopicArn(String theArn) {
        myTopicArn = theArn;
    }

    public void setAwsKeyId(String theAwsKeyId) {
        myAwsKeyId = theAwsKeyId;
    }

    public void setAwsSecretKey(String theAwsSecretKey) {
        myAwsSecretKey = theAwsSecretKey;
    }

    public void setSubject(String theSubject) {
        mySubject = theSubject;
    }

    public String getSubject() {
        return mySubject;
    }

    synchronized private void lazyInit() {
        try {

            if (iAmInitialized) {
                return;
            }

            iAmInitialized = true;
            mySnsClient = new SnsClient(myAwsKeyId, myAwsSecretKey);
            mySnsClosed = false;

        } catch (Exception e) {
            mySnsClosed = true;
            throw new RuntimeException("Could not instantiate SnsAsyncAppender", e);
        }
    }

    @Override
    protected void append(LoggingEvent theEvent) {

        if (!iAmInitialized) {
            lazyInit();
        }

        if (mySnsClosed) {
            return;
        }


        String aMsg;
        if (layout != null) {
            aMsg = layout.format(theEvent);
        } else {
            aMsg = theEvent.getRenderedMessage();
        }

        String[] aStacktrace = theEvent.getThrowableStrRep();

        if (aStacktrace != null && aStacktrace.length > 0) {
            for (String aStr : aStacktrace) {
                aMsg += "\n" + aStr;
            }
        }

        if (aMsg.getBytes().length > 64 * 1024) {
            // SNS has a 64K limit on each published message.
            aMsg = new String(Arrays.copyOf(aMsg.getBytes(), 64 * 1024));
        }

        String aSubject = mySubject;

        if (aSubject == null) {
            aSubject = theEvent.getLoggerName() + " log: " + theEvent.getLevel().toString();
        }

        try {

            mySnsClient.publishAsyncAndGet(
                    new PublishRequest(myTopicArn, aMsg, aSubject),
                    10, TimeUnit.SECONDS
            );

        } catch (Exception e) {
            LogLog.error("Could not log to SNS", e);
        }
    }

    public void close() {
        if (mySnsClosed) {
            return;
        }

        // No logging here. System out.
        System.out.println("Closing SNS appender....");

        mySnsClosed = true;
        mySnsClient.close();
    }

    public boolean requiresLayout() {
        return false;
    }

}