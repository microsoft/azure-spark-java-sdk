// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
package com.microsoft.azure.spark.tools.restapi.yarn.rm.apps.appid;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.microsoft.azure.spark.tools.restapi.Convertible;

import java.util.Collections;
import java.util.List;

/**
 * An application resource contains information about a particular application that was submitted to a cluster.
 *
 * Based on Hadoop 3.0.0, refer to
 * https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_API
 *
 * Use the following URI to obtain an app object, from a application identified by the appid value.
 *   http://$rmHttpAddress:port/ws/v1/cluster/apps/$appid
 *
 * HTTP Operations Supported
 *   GET
 *
 * Query Parameters Supported
 *   None
 */

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("nullness")
public class App implements Convertible {
    private String id;                  // The application id
    private String user;                // The user who started the application
    private String name;                // The application name
    private String applicationType;     // The application type
    private String queue;               // The queue the application was submitted to
    private String state;               // The application state according to the ResourceManager -
                                        // valid values are members of the YarnApplicationState enum:
                                        // NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
    private String finalStatus;         // The final status of the application if finished -
                                        // reported by the application itself - valid values are:
                                        // UNDEFINED, SUCCEEDED, FAILED, KILLED
    private float progress;             // The progress of the application as a percent
    private String trackingUI;          // Where the tracking url is currently pointing -
                                        // History (for history server) or ApplicationMaster
    private String trackingUrl;         // The web URL that can be used to track the application
    private String diagnostics;         // Detailed diagnostics information
    private long clusterId;             // The cluster id
    private long startedTime;           // The time in which application started (in ms since epoch)
    private long finishedTime;          // The time in which the application finished (in ms since epoch)
    private long elapsedTime;           // The elapsed time since the application started (in ms)
    private String amContainerLogs;     // The URL of the application master container logs
    private String amHostHttpAddress;   // The nodes http address of the application master
    private int allocatedMB;            // The sum of memory in MB allocated to the application’s running containers
    private int allocatedVCores;        // The sum of virtual cores allocated to the application’s running containers
    private int runningContainers;      // The number of containers currently running for the application
    private long memorySeconds;         // The amount of memory the application has allocated (megabyte-seconds)
    private long vcoreSeconds;          // The amount of CPU resources the application has allocated
                                        // (virtual core-seconds)
    private String applicationTags;     // Comma separated tags of an application
    private float clusterUsagePercentage; // The percentage of resources of the cluster that the app is using.
    private long preemptedResourceMB;   // Memory used by preempted container
    private int preemptedResourceVCores; // Number of virtual cores used by preempted container
    private boolean unmanagedApplication; // Is the application unmanaged.
    private String priority;            //Priority of the submitted application
    private String logAggregationStatus; // Status of log aggregation - valid values are the members of
                                         // the LogAggregationStatus enum:
                                         // DISABLED, NOT_START, RUNNING,
                                         // RUNNING_WITH_FAILURE, SUCCEEDED, FAILED, TIME_OUT
    private int numNonAMContainerPreempted; // Number of standard containers preempted;
    private String amNodeLabelExpression; // Node Label expression which is used to identify the node on which
                                          // application’s AM container is expected to run.
    private int numAMContainerPreempted; // Number of application master containers preempted
    private float queueUsagePercentage;  // The percentage of resources of the queue that the app is using
    private List<ResourceRequest> resourceRequests; // additional for HDInsight cluster


    public List<ResourceRequest> getResourceRequests() {
        return resourceRequests != null ? resourceRequests : Collections.emptyList();
    }

    public float getProgress() {
        return progress;
    }

    public String getQueue() {
        return queue;
    }

    public float getClusterUsagePercentage() {
        return clusterUsagePercentage;
    }

    public String getTrackingUI() {
        return trackingUI;
    }

    public String getState() {
        return state;
    }

    public String getAmContainerLogs() {
        return amContainerLogs;
    }

    public String getApplicationType() {
        return applicationType;
    }

    public int getPreemptedResourceVCores() {
        return preemptedResourceVCores;
    }

    public int getRunningContainers() {
        return runningContainers;
    }

    public int getAllocatedMB() {
        return allocatedMB;
    }

    public long getPreemptedResourceMB() {
        return preemptedResourceMB;
    }

    public String getId() {
        return id;
    }

    public boolean isUnmanagedApplication() {
        return unmanagedApplication;
    }

    public String getPriority() {
        return priority;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public int getAllocatedVCores() {
        return allocatedVCores;
    }

    public String getName() {
        return name;
    }

    public String getLogAggregationStatus() {
        return logAggregationStatus;
    }

    public long getVcoreSeconds() {
        return vcoreSeconds;
    }

    public int getNumNonAMContainerPreempted() {
        return numNonAMContainerPreempted;
    }

    public long getMemorySeconds() {
        return memorySeconds;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public String getAmNodeLabelExpression() {
        return amNodeLabelExpression;
    }

    public String getAmHostHttpAddress() {
        return amHostHttpAddress;
    }

    public String getFinalStatus() {
        return finalStatus;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public int getNumAMContainerPreempted() {
        return numAMContainerPreempted;
    }

    public String getApplicationTags() {
        return applicationTags;
    }

    public long getClusterId() {
        return clusterId;
    }

    public String getUser() {
        return user;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public long getStartedTime() {
        return startedTime;
    }

    public float getQueueUsagePercentage() {
        return queueUsagePercentage;
    }

    /**
     * Check if the Yarn job finish or not.
     *
     * @return true for finished.
     */
    public boolean isFinished() {
        String stateUpper = this.getState().toUpperCase();

        return stateUpper.equals("FINISHED") || stateUpper.equals("FAILED") || stateUpper.equals("KILLED");
    }
}

