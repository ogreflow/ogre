package com.ws.ogre.v2.commands.avroconcat;

import com.amazonaws.services.s3.model.StorageClass;
import com.ws.ogre.v2.aws.S3BetterUrl;
import com.ws.ogre.v2.utils.GlobToRegexp;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.*;

/*
 log4j.configuration   = log4j.xml

 types                 = delivery, click, impression, charge, event

 alert.sns.accessKeyId =
 alert.sns.secretKey   =
 alert.sns.topic       = arn:aws:sns:eu-west-1:xxx:yyy

 src.s3.accessKeyId    =
 src.s3.secretKey      =
 src.s3.rootPath       =

 dst.s3.accessKeyId    =
 dst.s3.secretKey      =
 dst.s3.rootPath       =
 dst.s3.storageClass   = STANDARD_IA

 */
public class Config {

    private static final String PROP_LOG4J               = "log4j.configuration";

    private static final String PROP_ALERT_SNS_KEYID     = "alert.sns.accessKeyId";
    private static final String PROP_ALERT_SNS_SECRET    = "alert.sns.secretKey";
    private static final String PROP_ALERT_SNS_TOPIC     = "alert.sns.topic";

    private static final String PROP_TYPES               = "types";

    private static final String PROP_SRC_S3_KEYID        = "src.s3.accessKeyId";
    private static final String PROP_SRC_S3_SECRET       = "src.s3.secretKey";
    private static final String PROP_SRC_S3_ROOT         = "src.s3.rootPath";

    private static final String PROP_DST_S3_KEYID        = "dst.s3.accessKeyId";
    private static final String PROP_DST_S3_SECRET       = "dst.s3.secretKey";
    private static final String PROP_DST_S3_ROOT         = "dst.s3.rootPath";
    private static final String PROP_DST_S3_STORAGECLASS = "dst.s3.storageClass";


    public String log4jConf;

    public String alertAccessKey;
    public String alertSecret;
    public String alertTopic;

    public String[] types;

    public String srcAccessKey;
    public String srcSecret;
    public S3BetterUrl srcRoot;

    public String dstAccessKey;
    public String dstSecret;
    public S3BetterUrl dstRoot;
    public StorageClass dstClass;


    public static Config load(String theFile) {
        try {

            return new Config(theFile);

        } catch (ConfigurationException e) {
            throw new ConfigException(e);
        }
    }

    public Config(String theFile) throws ConfigurationException {

        PropertiesConfiguration aConf = new PropertiesConfiguration(theFile);

        log4jConf      = aConf.getString(PROP_LOG4J);

        alertAccessKey = aConf.getString(PROP_ALERT_SNS_KEYID);
        alertSecret    = aConf.getString(PROP_ALERT_SNS_SECRET);
        alertTopic     = aConf.getString(PROP_ALERT_SNS_TOPIC);

        types          = aConf.getStringArray(PROP_TYPES);

        srcAccessKey   = aConf.getString(PROP_SRC_S3_KEYID);
        srcSecret      = aConf.getString(PROP_SRC_S3_SECRET);
        srcRoot        = new S3BetterUrl(aConf.getString(PROP_SRC_S3_ROOT));

        dstAccessKey   = aConf.getString(PROP_DST_S3_KEYID);
        dstSecret      = aConf.getString(PROP_DST_S3_SECRET);
        dstRoot        = new S3BetterUrl(aConf.getString(PROP_DST_S3_ROOT));
        dstClass       = StorageClass.fromValue(aConf.getString(PROP_DST_S3_STORAGECLASS, "STANDARD_IA"));
    }

    public static class ConfigException extends RuntimeException {
        public ConfigException(Throwable theCause) {
            super(theCause);
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "log4jConf='" + log4jConf + '\'' +
                ", alertAccessKey='" + alertAccessKey + '\'' +
                ", alertSecret='xxx'" +
                ", alertTopic='" + alertTopic + '\'' +
                ", srcAccessKey='" + srcAccessKey + '\'' +
                ", srcSecret='xxx'" +
                ", srcRoot=" + srcRoot +
                ", dstAccessKey='" + dstAccessKey + '\'' +
                ", dstSecret='xxx'" +
                ", dstRoot=" + dstRoot +
                ", dstClass=" + dstClass +
                '}';
    }
}
