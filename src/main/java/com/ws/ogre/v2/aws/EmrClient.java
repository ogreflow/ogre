package com.ws.ogre.v2.aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EmrClient {
    private AmazonElasticMapReduce myMapReduce;

    public EmrClient(String theAccessKeyId, String theSecretKey) {
        this(new BasicAWSCredentials(theAccessKeyId, theSecretKey));
    }

    public EmrClient(BasicAWSCredentials theCredentials) {
        myMapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion("eu-west-1")
                .withCredentials(new AWSStaticCredentialsProvider(theCredentials))
                .build();
    }

    public EmrCluster getCluster(String theClusterId) {
        DescribeClusterResult aResult = myMapReduce.describeCluster(
                new DescribeClusterRequest().withClusterId(theClusterId)
        );

        if (aResult == null || aResult.getCluster() == null) {
            return null;
        }

        return new EmrCluster(aResult.getCluster());
    }

    public List<StepSummary> getClusterSteps(String theClusterId) {
        return myMapReduce.listSteps(new ListStepsRequest().withClusterId(theClusterId)).getSteps();
    }

    public void addStep(String theClusterId, List<StepConfig> theStepConfigs) {
        myMapReduce.addJobFlowSteps(
                new AddJobFlowStepsRequest()
                        .withJobFlowId(theClusterId)
                        .withSteps(theStepConfigs)
        );
    }

    public RunJobFlowResult fireCluster(String theClusterNameSuffix, EmrNewClusterConfig theClusterConfig, List<StepConfig> theStepConfigs) {
        if (theClusterConfig.instanceCount > 1) {
            throw new RuntimeException("Instance count more than 1 is not supported now.");
        }

        return myMapReduce.runJobFlow(new RunJobFlowRequest()
                .withName(theClusterConfig.clusterNamePrefix + " (" + theClusterNameSuffix + ")")
                .withSteps(theStepConfigs)
                .withApplications(Arrays.asList(
                        new Application().withName("Spark")
                ))
                .withReleaseLabel(theClusterConfig.releaseLabel)
                .withTags(new Tag("Name", theClusterConfig.clusterNamePrefix + "-EMR"))
                .withLogUri(theClusterConfig.logUri)
                .withVisibleToAllUsers(true)
                .withInstances(
                        new JobFlowInstancesConfig()
                                .withKeepJobFlowAliveWhenNoSteps(false)
                                .withEc2SubnetId(theClusterConfig.instanceVpcSubnetId)
                                .withEc2KeyName(theClusterConfig.instanceKeyPair)
                                .withInstanceFleets(
                                        new InstanceFleetConfig()
                                                .withInstanceFleetType(InstanceFleetType.MASTER)
                                                .withInstanceTypeConfigs(new InstanceTypeConfig()
                                                                .withInstanceType(theClusterConfig.instanceType)
                                                                .withBidPrice(theClusterConfig.instancePrice)
                                                                .withEbsConfiguration(
                                                                        new EbsConfiguration()
                                                                                .withEbsBlockDeviceConfigs(
                                                                                        new EbsBlockDeviceConfig()
                                                                                                .withVolumeSpecification(
                                                                                                        new VolumeSpecification()
                                                                                                                .withVolumeType("gp2")
                                                                                                                .withSizeInGB(theClusterConfig.instanceEbsVolumeSizeInGB)
                                                                                                )
                                                                                                .withVolumesPerInstance(1)
                                                                                )
                                                                )
                                                                .withWeightedCapacity(1)
                                                )
                                                .withName(theClusterConfig.clusterNamePrefix + "-master")
                                                .withTargetSpotCapacity(theClusterConfig.instanceCount)
                                )
                )
                .withJobFlowRole(theClusterConfig.jobFlowRole)
                .withServiceRole(theClusterConfig.serviceRole));
    }

    public StepConfig getSingleStepConfig(String theStepName, EmrSparkConfig theSparkConfig, String theJarPath, List<String> theArgs, ActionOnFailure theActionOnFailure) {
        List<String> someStepArgs = new ArrayList<>();
        someStepArgs.add("spark-submit");
        someStepArgs.add("--class");
        someStepArgs.add(theSparkConfig.sparkClass);
        someStepArgs.add("--master");
        someStepArgs.add(theSparkConfig.sparkMaster);
        someStepArgs.add("--driver-cores");
        someStepArgs.add(theSparkConfig.sparkDriverCore);
        someStepArgs.add("--driver-memory");
        someStepArgs.add(theSparkConfig.sparkDriverMemory);

        if (theSparkConfig.sparkExecutorDynamicAllocationEnabled) {
            someStepArgs.add("--conf");
            someStepArgs.add("spark.shuffle.service.enabled=" + theSparkConfig.sparkExecutorShuffleServiceEnabled);
            someStepArgs.add("--conf");
            someStepArgs.add("spark.dynamicAllocation.enabled=" + theSparkConfig.sparkExecutorDynamicAllocationEnabled);
        } else {
            someStepArgs.add("--num-executors");
            someStepArgs.add(theSparkConfig.sparkExecutorNum);
            someStepArgs.add("--conf");
            someStepArgs.add("spark.executor.memory=" + theSparkConfig.sparkExecutorMemory);
            someStepArgs.add("--executor-cores");
            someStepArgs.add(theSparkConfig.sparkExecutorCore);
        }

        someStepArgs.add(theJarPath);
        someStepArgs.addAll(theArgs);

        return new StepConfig()
                .withName("Step : " + theStepName)
                .withActionOnFailure(theActionOnFailure)
                .withHadoopJarStep(
                        new HadoopJarStepConfig()
                                .withJar("command-runner.jar")
                                .withArgs(someStepArgs)
                );
    }

    public static class EmrCluster {
        private Cluster myCluster;

        public EmrCluster(Cluster theCluster) {
            myCluster = theCluster;
        }

        public boolean isClusterInOkState() {
            return Arrays.asList(ClusterState.WAITING, ClusterState.RUNNING, ClusterState.STARTING, ClusterState.BOOTSTRAPPING).contains(
                    getState()
            );
        }

        public boolean isClusterTerminated() {
            return Arrays.asList(ClusterState.TERMINATED).contains(
                    getState()
            );
        }

        public boolean isClusterTerminatedWithError() {
            return Arrays.asList(ClusterState.TERMINATED_WITH_ERRORS).contains(
                    getState()
            );
        }

        public ClusterState getState() {
            return ClusterState.fromValue(myCluster.getStatus().getState());
        }

        public String getId() {
            return myCluster.getId();
        }

        public String getName() {
            return myCluster.getName();
        }
    }

    public static class EmrOldClusterConfig {
        public String clusterId;

        @Override
        public String toString() {
            return "EmrOldClusterConfig{" +
                    "clusterId=" + clusterId +
                    "}";
        }
    }

    public static class EmrNewClusterConfig {
        public String clusterNamePrefix;
        public String releaseLabel;
        public String logUri;
        public String jobFlowRole;
        public String serviceRole;

        public Integer instanceCount;
        public Integer instanceEbsVolumeSizeInGB;
        public String instanceType;
        public String instancePrice;
        public String instanceKeyPair;
        public String instanceVpcSubnetId;

        @Override
        public String toString() {
            return "EmrNewClusterConfig{" +
                    "clusterNamePrefix=" + clusterNamePrefix +
                    ", releaseLabel=" + releaseLabel +
                    ", logUri=" + logUri +
                    ", jobFlowRole=" + jobFlowRole +
                    ", serviceRole=" + serviceRole +
                    ", instanceCount=" + instanceCount +
                    ", instanceEbsVolumeSizeInGB=" + instanceEbsVolumeSizeInGB +
                    ", instanceType=" + instanceType +
                    ", instancePrice=" + instancePrice +
                    ", instanceKeyPair=" + instanceKeyPair +
                    ", instanceVpcSubnetId=" + instanceVpcSubnetId +
                    "}";
        }
    }

    public static class EmrSparkConfig {
        public String sparkMaster;
        public String sparkClass;
        public String sparkExecutorCore;
        public String sparkExecutorNum;
        public String sparkExecutorMemory;
        public String sparkDriverCore;
        public String sparkDriverMemory;

        public Boolean sparkExecutorDynamicAllocationEnabled;
        public Boolean sparkExecutorShuffleServiceEnabled;

        @Override
        public String toString() {
            return "EmrSparkConfig{" +
                    "sparkMaster=" + sparkMaster +
                    ", sparkClass=" + sparkClass +
                    ", sparkExecutorCore=" + sparkExecutorCore +
                    ", sparkExecutorNum=" + sparkExecutorNum +
                    ", sparkExecutorMemory=" + sparkExecutorMemory +
                    ", sparkDriverCore=" + sparkDriverCore +
                    ", sparkDriverMemory=" + sparkDriverMemory +
                    ", sparkExecutorDynamicAllocationEnabled=" + sparkExecutorDynamicAllocationEnabled +
                    ", sparkExecutorShuffleServiceEnabled=" + sparkExecutorShuffleServiceEnabled +
                    "}";
        }
    }
}
