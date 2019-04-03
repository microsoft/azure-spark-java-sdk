// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi;

import java.util.List;
import java.util.Map;

@SuppressWarnings("nullness")
public class SparkSubmitResponse {
    private int id;
    private String state;

    private String appId;                   // The application ID
    private Map<String, Object> appInfo;    // The detailed application info
    private List<String> log;               // The log lines

    public String getAppId() {
        return appId;
    }

    public Map<String, Object> getAppInfo() {
        return appInfo;
    }

    public List<String> getLog() {
        return log;
    }

    public int getId() {
        return id;
    }

    public String getState() {
        return state;
    }

    public boolean isAlive() {
        return !this.getState().equals("error")
                && !this.getState().equals("success")
                && !this.getState().equals("dead");
    }
}
