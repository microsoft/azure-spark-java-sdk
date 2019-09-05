// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm.apps.appid.appattempts;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("nullness")
public class AppAttempt {
    private int id;                  // The app attempt id
    private String nodeId;           // The node id of the node the attempt ran on
    private String nodeHttpAddress;  // The node http address of the node the attempt ran on
    private String logsLink;         // The http link to the app attempt logs
    private String containerId;      // The id of the container for the app attempt
    private long startTime;          // The start time of the attempt (in ms since epoch)
    private long finishedTime;       // The end time of the attempt (in ms since epoch), 0 for not end
    private String blacklistedNodes; // Nodes blacklist
    private String appAttemptId;     // App Attempt Id

    public int getId() {
        return id;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeHttpAddress() {
        return nodeHttpAddress;
    }

    public String getLogsLink() {
        return logsLink;
    }

    public String getContainerId() {
        return containerId;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public String getBlacklistedNodes() {
        return blacklistedNodes;
    }

    public String getAppAttemptId() {
        return appAttemptId;
    }
}
