package com.ws.ogre.v2.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClient;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SnsClient {

    private final AmazonSNSAsync myClient;

    public SnsClient(String theAccessKeyId, String theSecretKey) {
        AmazonSNSAsyncClientBuilder aBuilder = AmazonSNSAsyncClient.asyncBuilder();
        aBuilder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(theAccessKeyId, theSecretKey)));
        aBuilder.setRegion(Regions.EU_WEST_1.getName());

        myClient = aBuilder.build();
    }

    public void publish(String theTopicArn, String theSubject, String theMessage) {
        // Truncate message. SNS has a 64K limit on each published message.
        if (theMessage.getBytes().length > 64 * 1024) {
            theMessage = new String(Arrays.copyOf(theMessage.getBytes(), 64 * 1024));
        }

        // Post message
        try {
            publishAsyncAndGet(new PublishRequest(theTopicArn, theMessage, theSubject), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publishAsyncAndGet(PublishRequest theRequest, long theTimeout, TimeUnit theTimeUnit) throws Exception {
        Future<PublishResult> aFuture = myClient.publishAsync(theRequest);
        aFuture.get(theTimeout, theTimeUnit);
    }

    public void close() {
        myClient.shutdown();
    }
}
