package com.ws.ogre.v2.commands.sparkrunner;

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.EmrClient;
import com.ws.ogre.v2.datetime.DateHour;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class SparkRunnerHandler {
    private static final Logger ourLogger = Logger.getLogger();

    private Config myConfig;
    private EmrClient myEmrClient;

    public SparkRunnerHandler(Config theConfig) {
        myConfig = theConfig;
        myEmrClient = new EmrClient(myConfig.emrAccessKeyId, myConfig.emrSecretKey);
    }

    public void load(Set<String> theSteps, DateHour.Range theTimeRange) throws Exception {
        Set<String> someSteps = getStepsToWork(theSteps, myConfig.steps);

        String aJobFlowId = null;
        if (StringUtils.isNotEmpty(myConfig.emrOldClusterConfig.clusterId)) {
            aJobFlowId = runJobInExistingCluster(someSteps, theTimeRange);
        } else {
            aJobFlowId = runJobInNewCluster(someSteps, theTimeRange);
        }

        ourLogger.info("Adding watcher to monitor the state of the job flow %s", aJobFlowId);
        new EmrClusterStatusWatcher(aJobFlowId, myConfig.emrAccessKeyId, myConfig.emrSecretKey).watch(myConfig.emrMaxRunTimeMin);
    }

    private String runJobInExistingCluster(Set<String> theSteps, DateHour.Range theTimeRange) throws Exception {
        String aClusterId = myConfig.emrOldClusterConfig.clusterId;
        ourLogger.info("Will try to run in the cluster id=%s", aClusterId);

        EmrClient.EmrCluster aCluster = myEmrClient.getCluster(aClusterId);
        if (aCluster == null) {
            throw new Exception("No cluster found with id: " + aClusterId);
        }

        ourLogger.info("Found cluster '%s - %s' in state=%s", aCluster.getName(), aCluster.getId(), aCluster.getState());
        if (!aCluster.isClusterInOkState()) {
            throw new Exception("Cluster '" + aCluster.getName() + " - " + aCluster.getId() + "' is not in OK state. It is in " + aCluster.getState() + " state");
        }

        ourLogger.info("Adding steps in the cluster %s", aClusterId);
        myEmrClient.addStep(
                aClusterId,
                getSteps(theSteps, theTimeRange, ActionOnFailure.CONTINUE)
        );

        ourLogger.info("Successfully added steps in the cluster %s", aClusterId);
        return aClusterId;
    }

    private String runJobInNewCluster(Set<String> theSteps, DateHour.Range theTimeRange) {
        ourLogger.info("Will run in a new cluster");

        RunJobFlowResult aJobFlowResult = myEmrClient.fireCluster(
                theTimeRange.toString(),
                myConfig.emrNewClusterConfig,
                getSteps(theSteps, theTimeRange, ActionOnFailure.TERMINATE_CLUSTER)
        );

        ourLogger.info("Successfully started new EMR cluster %s", aJobFlowResult.getJobFlowId());
        return aJobFlowResult.getJobFlowId();
    }

    private Set<String> getStepsToWork(Set<String> theCliSteps, String[] theConfigSteps) {
        if (theCliSteps != null && !theCliSteps.isEmpty()) {
            return theCliSteps; /* Steps that we send in CLI is of highest priority. */
        }

        if (theConfigSteps == null || theConfigSteps.length == 0) {
            throw new IllegalArgumentException("No 'types' found in CLI and not event configured");
        }

        return new HashSet<>(Arrays.asList(theConfigSteps));
    }

    private List<StepConfig> getSteps(Set<String> theSteps, DateHour.Range theTimeRange, ActionOnFailure theActionOnFailure) {
        List<StepConfig> steps = new ArrayList<>();

        for (String aStep : theSteps) {
            List<String> someStepArgs = new ArrayList<>();
            someStepArgs.add("load");
            someStepArgs.add(theTimeRange.getFrom().toString());
            someStepArgs.add(theTimeRange.getTo().toString());
            someStepArgs.add("-config");
            someStepArgs.add(myConfig.getStepConfigPath(aStep));
            List<String> sparkRunnerArgs = myConfig.getStepArgs(aStep);
            if (!sparkRunnerArgs.isEmpty()) {
                someStepArgs.addAll(myConfig.getStepArgs(aStep));
            }

            steps.add(myEmrClient.getSingleStepConfig(
                    aStep, myConfig.emrSparkConfig, myConfig.sqlRunnerJarPath.toString(), someStepArgs, theActionOnFailure
            ));
        }
        return steps;
    }
}
