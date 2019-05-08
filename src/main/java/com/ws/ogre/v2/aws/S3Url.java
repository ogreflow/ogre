package com.ws.ogre.v2.aws;

import org.apache.commons.lang3.StringUtils;

/**
 * S3 URL.
 */
public class S3Url {

    public String bucket;
    public String key;

    public S3Url(S3Url theParent, String theSubKey) {
        String aParentUrl = theParent.toString();

        if (!aParentUrl.endsWith("/") && !theSubKey.startsWith("/")) {
            aParentUrl += "/";
        }

        resolve(aParentUrl + theSubKey);
    }

    public S3Url(String theBucket, String theKey) {
        resolve("s3://" + theBucket + "/" + theKey);
    }

    public S3Url(String theUrl) {
        resolve(theUrl);
    }

    public String getBaseName() {
        // Input: s3://ogre-unittests/smiskme/data2rds/avro/report_hour_adtraffic/
        // Output: report_hour_adtraffic
        String[] someParts = StringUtils.strip(this.key, "/").split("/");
        return someParts.length > 0 ? someParts[someParts.length - 1] : "";
    }

    private void resolve(String theUrl) {
        if (theUrl == null || !theUrl.matches("s3://[a-zA-Z0-9-\\.]+/([a-zA-Z0-9-=./_]*)")) {
            throw new IllegalArgumentException("Malformed URL: " + theUrl);
        }

        int aPos = theUrl.indexOf("/", 5);

        bucket = theUrl.substring(5, aPos);
        key = theUrl.substring(aPos+1);
    }

    @Override
    public String toString() {
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof S3Url)) {
            return false;
        }

        S3Url that = (S3Url)obj;

        return StringUtils.equals(this.bucket, that.bucket) && StringUtils.equals(this.key, that.key);
    }
}
