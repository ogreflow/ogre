package com.ws.ogre.v2.commands.sparkrunner;

import com.ws.ogre.v2.aws.EmrClient;
import com.ws.ogre.v2.aws.S3BetterUrl;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {
    public String log4jConf;
    public S3BetterUrl sqlRunnerJarPath;
    public String[] steps;

    public String emrAccessKeyId;
    public String emrSecretKey;
    public int emrMaxRunTimeMin;

    public EmrClient.EmrOldClusterConfig emrOldClusterConfig;
    public EmrClient.EmrNewClusterConfig emrNewClusterConfig;
    public EmrClient.EmrSparkConfig emrSparkConfig;

    private Map<String, String> stepConfigs = new HashMap<>();
    private Map<String, List<String>> stepArgs = new HashMap<>();

    public static Config load(String theFile) {
        try {
            return new Config(theFile);

        } catch (ConfigurationException e) {
            throw new ConfigException(e);
        }
    }

    public Config(String theFile) throws ConfigurationException {

        PropertiesConfiguration aConf = new PropertiesConfiguration(theFile);

        log4jConf = aConf.getString("log4j.configuration");

        sqlRunnerJarPath = new S3BetterUrl(aConf.getString("spark_sql_runner.jar"));

        emrAccessKeyId = aConf.getString("emr.accessKeyId");
        emrSecretKey = aConf.getString("emr.secretKey");
        emrMaxRunTimeMin = aConf.getInt("emr.maxRunTimeMin", 120);

        emrOldClusterConfig = new EmrClient.EmrOldClusterConfig();
        emrOldClusterConfig.clusterId = aConf.getString("emr.old.clusterId");

        emrNewClusterConfig = new EmrClient.EmrNewClusterConfig();
        emrNewClusterConfig.clusterNamePrefix = aConf.getString("emr.new.clusterNamePrefix");
        emrNewClusterConfig.releaseLabel = aConf.getString("emr.new.releaseLabel");
        emrNewClusterConfig.logUri = aConf.getString("emr.new.logUri");
        emrNewClusterConfig.jobFlowRole = aConf.getString("emr.new.jobFlowRole");
        emrNewClusterConfig.serviceRole = aConf.getString("emr.new.serviceRole");
        emrNewClusterConfig.instanceCount = aConf.getInt("emr.new.instanceCount", 1);
        emrNewClusterConfig.instanceEbsVolumeSizeInGB = aConf.getInt("emr.new.instanceEbsVolumeSizeInGB", 100);
        emrNewClusterConfig.instanceType = aConf.getString("emr.new.instanceType");
        emrNewClusterConfig.instancePrice = aConf.getString("emr.new.instancePrice");
        emrNewClusterConfig.instanceKeyPair = aConf.getString("emr.new.instanceKeyPair");
        emrNewClusterConfig.instanceVpcSubnetId = aConf.getString("emr.new.instanceVpcSubnetId");

        emrSparkConfig = new EmrClient.EmrSparkConfig();
        emrSparkConfig.sparkMaster = aConf.getString("emr.spark.master");
        emrSparkConfig.sparkClass = aConf.getString("emr.spark.class");
        emrSparkConfig.sparkExecutorCore = aConf.getString("emr.spark.executor.core");
        emrSparkConfig.sparkExecutorNum = aConf.getString("emr.spark.executor.num");
        emrSparkConfig.sparkExecutorMemory = aConf.getString("emr.spark.executor.memory");
        emrSparkConfig.sparkDriverCore = aConf.getString("emr.spark.driver.core");
        emrSparkConfig.sparkDriverMemory = aConf.getString("emr.spark.driver.memory");
        emrSparkConfig.sparkExecutorShuffleServiceEnabled = aConf.getBoolean("emr.spark.executor.shuffle.service.enabled", false);
        emrSparkConfig.sparkExecutorDynamicAllocationEnabled = aConf.getBoolean("emr.spark.executor.dynamic.allocation.enabled", false);

        steps = aConf.getStringArray("steps");

        readStepConfigs(aConf);
        readStepArgs(aConf);
    }

    private void readStepConfigs(PropertiesConfiguration aConf) throws ConfigurationException {
        aConf.setDelimiterParsingDisabled(true);
        aConf.refresh();

        for (String aStep : steps) {
            stepConfigs.put(aStep, aConf.getString("step." + aStep + ".conf"));
        }

        aConf.setDelimiterParsingDisabled(false);
        aConf.refresh();
    }

    private void readStepArgs(PropertiesConfiguration aConf) throws ConfigurationException {
        aConf.setDelimiterParsingDisabled(true);
        aConf.refresh();

        for (String aStep : steps) {
            stepArgs.put(aStep, Arrays.asList(aConf.getString("step." + aStep + ".args", "").trim().split("\\s+")));
        }

        aConf.setDelimiterParsingDisabled(false);
        aConf.refresh();
    }

    public String getStepConfigPath(String theType) {
        return stepConfigs.get(theType);
    }

    public List<String> getStepArgs(String theType) {
        return stepArgs.get(theType);
    }

    public static class ConfigException extends RuntimeException {
        public ConfigException(Throwable theCause) {
            super(theCause);
        }
    }

    @Override
    public String toString() {
        return "Config{" +
                "log4jConf=" + log4jConf +
                ", sqlRunnerJarPath=" + sqlRunnerJarPath +
                ", steps=" + Arrays.toString(steps) +
                ", emrAccessKeyId=" + emrAccessKeyId +
                ", emrSecretKey=" + "***" +
                ", emrOldClusterConfig=" + emrOldClusterConfig +
                ", emrNewClusterConfig=" + emrNewClusterConfig +
                ", emrSparkConfig=" + emrSparkConfig +
                ", stepConfigs=" + stepConfigs +
                ", stepArgs=" + stepArgs +
                "}";
    }
}
