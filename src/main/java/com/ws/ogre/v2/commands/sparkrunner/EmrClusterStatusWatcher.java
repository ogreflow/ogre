package com.ws.ogre.v2.commands.sparkrunner;

import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.StepState;
import com.google.gson.Gson;
import com.ws.common.logging.Alert;
import com.ws.common.logging.Logger;
import com.ws.ogre.v2.aws.EmrClient;
import com.ws.ogre.v2.utils.SleepUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EmrClusterStatusWatcher {
    private static final int LOOKUP_SLEEP_TIME_MIN = 5; /* 5min */

    private EmrClient myEmrClient;
    private String myClusterId;
    private ClusterStatusMonitorData myCurrentMonitorData;

    private static final Logger ourLogger = Logger.getLogger();

    public EmrClusterStatusWatcher(String theClusterId, String theAccessKey, String theSecretKey) {
        myClusterId = theClusterId;
        myEmrClient = new EmrClient(theAccessKey, theSecretKey);
        myCurrentMonitorData = new ClusterStatusMonitorData();
    }

    public void watch(int theMaxRunningTimeMin) {
        int aLoopTimes = theMaxRunningTimeMin / LOOKUP_SLEEP_TIME_MIN;

        for (int i = 0; i < aLoopTimes; i++) {
            EmrClient.EmrCluster aCluster = myEmrClient.getCluster(myClusterId);

            ClusterStatusMonitorData aNewMonitorData = findClusterStatusMonitorData(aCluster);

            if (!StringUtils.equals(aNewMonitorData.toString(), myCurrentMonitorData.toString())) {
                ourLogger.info("Emr cluster '%s - %s' has changed status from %s => %s.", aCluster.getName(), aCluster.getId(), myCurrentMonitorData, aNewMonitorData);
                myCurrentMonitorData = aNewMonitorData;
            }

            if (aCluster.isClusterTerminated()) {
                ourLogger.info("Emr cluster '%s - %s' terminated successfully. Stopping watching.", aCluster.getName(), aCluster.getId());
                return;
            }

            if (aCluster.isClusterTerminatedWithError()) {
                Alert.getAlert().alert("Emr cluster '%s - %s' terminated with error. Please check.", aCluster.getName(), aCluster.getId());
                return;
            }

            // Wait for next checkup.
            SleepUtil.sleep(LOOKUP_SLEEP_TIME_MIN * 60 * 1000);

            if (i == aLoopTimes - 1) {
                Alert.getAlert().alert("Emr cluster '%s - %s' is running for unexpectedly long time. Please check.", aCluster.getName(), aCluster.getId());
            }
        }
    }

    private ClusterStatusMonitorData findClusterStatusMonitorData(EmrClient.EmrCluster theCluster) {
        ClusterStatusMonitorData aNewMonitorData = new ClusterStatusMonitorData();
        aNewMonitorData.setState(theCluster.getState());
        aNewMonitorData.setStepsData(
                myEmrClient.getClusterSteps(theCluster.getId()).stream()
                        .map(aStep -> StepState.fromValue(aStep.getStatus().getState()))
                        .collect(Collectors.toList())
        );
        return aNewMonitorData;
    }

    private class ClusterStatusMonitorData {
        private List<StepState> myStepStates = new ArrayList<>();
        private ClusterState myState;

        public ClusterStatusMonitorData() {
        }

        public void setState(ClusterState theState) {
            myState = theState;
        }

        public void setStepsData(List<StepState> theStepStates) {
            myStepStates = theStepStates;
        }

        @Override
        public String toString() {
            return new Gson().toJson(this);
        }
    }
}
