package com.ws.ogre.v2.commands.avro2kinesis;

import com.ws.ogre.v2.aws.S3BetterUrl;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/*
 log4j.configuration     = log4j.xml

 types                   = delivery, click, impression, charge, event

 src.s3.accessKeyId      =
 src.s3.secretKey        =
 src.s3.rootPath         =

 dst.kinesis.accessKeyId =
 dst.kinesis.secretKey   =
 dst.kinesis.stream      =
 */
public class Config {

    private static final String PROP_LOG4J               = "log4j.configuration";

    private static final String PROP_TYPES               = "types";

    private static final String PROP_SRC_S3_KEYID        = "src.s3.accessKeyId";
    private static final String PROP_SRC_S3_SECRET       = "src.s3.secretKey";
    private static final String PROP_SRC_S3_ROOT         = "src.s3.rootPath";

    private static final String PROP_DST_KINESIS_KEYID   = "dst.kinesis.accessKeyId";
    private static final String PROP_DST_KINESIS_SECRET  = "dst.kinesis.secretKey";
    private static final String PROP_DST_KINESIS_STREAM  = "dst.kinesis.stream";


    public String log4jConf;

    public String[] types;

    public String srcAccessKey;
    public String srcSecret;
    public S3BetterUrl srcRoot;

    public String dstAccessKey;
    public String dstSecret;
    public String dstStream;


    public static Config load(String theFile) {
        try {

            return new Config(theFile);

        } catch (ConfigurationException e) {
            throw new ConfigException(e);
        }
    }

    public Config(String theFile) throws ConfigurationException {

        PropertiesConfiguration aConf = new PropertiesConfiguration(theFile);

        log4jConf    = aConf.getString(PROP_LOG4J, ".");

        types        = aConf.getStringArray(PROP_TYPES);

        srcAccessKey = aConf.getString(PROP_SRC_S3_KEYID);
        srcSecret    = aConf.getString(PROP_SRC_S3_SECRET);
        srcRoot      = new S3BetterUrl(aConf.getString(PROP_SRC_S3_ROOT));

        dstAccessKey = aConf.getString(PROP_DST_KINESIS_KEYID);
        dstSecret    = aConf.getString(PROP_DST_KINESIS_SECRET);
        dstStream    = aConf.getString(PROP_DST_KINESIS_STREAM);
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
                ", types='" + types + '\'' +
                ", srcAccessKey='" + srcAccessKey + '\'' +
                ", srcRoot=" + srcRoot +
                ", dstAccessKey='" + dstAccessKey + '\'' +
                ", dstStream=" + dstStream +
                '}';
    }
}
