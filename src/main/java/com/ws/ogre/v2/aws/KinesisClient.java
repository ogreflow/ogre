package com.ws.ogre.v2.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

import java.nio.ByteBuffer;

public class KinesisClient {

    private AWSCredentials myCredentials;

    public KinesisClient(String theAccessKeyId, String theSecretKey) {
        myCredentials = new BasicAWSCredentials(theAccessKeyId, theSecretKey);
    }

    public void put(String theStream, String thePartitionKey, byte[] theData) {
        AmazonKinesisClient aClient = new AmazonKinesisClient(myCredentials);
        aClient.setRegion(Region.getRegion(Regions.EU_WEST_1));

        PutRecordRequest aRequest = new PutRecordRequest();
        aRequest.setStreamName(theStream);
        aRequest.setData(ByteBuffer.wrap(theData));
        aRequest.setPartitionKey(thePartitionKey);
        PutRecordResult aResult = aClient.putRecord(aRequest);
    }
}
