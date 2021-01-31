package com.ws.ogre.v2.aws;

import org.junit.Assert;
import org.junit.Test;

public class S3UrlTest {
    @Test
    public void testS3Url_BucketWithSpecialCharacter() {
        try {
            String anS3Url = "s3://ogre.unittests-test/smiskme/DataFileHandlerTest/click/d=2015-02-07/h=08/delivery-2015020708-0001_part_00.gz";
            S3BetterUrl anUrl = new S3BetterUrl(anS3Url);
            Assert.assertEquals("ogre.unittests-test", anUrl.bucket);
            Assert.assertEquals("smiskme/DataFileHandlerTest/click/d=2015-02-07/h=08/delivery-2015020708-0001_part_00.gz", anUrl.key);

        } catch (Exception e) {
            Assert.fail("Should not got any exception for s3 url having bucket with special character (e.g., dot, hyphen) but got: " + e.getMessage());
        }
    }
}
