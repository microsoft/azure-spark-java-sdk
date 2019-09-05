// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.livy.batches;

import com.microsoft.azure.spark.tools.restapi.Convertible;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("nullness")
public class Batch implements Convertible {
    private int id;
    private String state;

    private String appId;                   // The application ID
    private Map<String, String> appInfo;    // The detailed application info
    private List<String> log;               // The log lines

    public String getAppId() {
        return appId;
    }

    public Map<String, String> getAppInfo() {
        return appInfo != null ? appInfo : Collections.emptyMap();
    }

    public List<String> getLog() {
        return log != null ? log : Collections.emptyList();
    }

    public int getId() {
        return id;
    }

    public String getState() {
        return state;
    }
}
