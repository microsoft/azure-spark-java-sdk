// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.microsoft.azure.spark.tools.restapi.Convertible;

import java.util.List;

/**
 * An application resource contains information about a particular application that was submitted to a cluster.
 *
 * Based on Hadoop 3.0.0, refer to https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_API
 *
 * Use the following URI to obtain an apps list,
 *   http://$rmHttpAddress:port>/ws/v1/cluster/$apps
 *
 * HTTP Operations Supported
 *   GET
 *
 * Query Parameters Supported
 *   None
 */
@SuppressWarnings("nullness")
public class YarnApplications implements Convertible {
    @JsonProperty(value = "app")
    private List<App> apps;

    public List<App> getApps() {
        return apps;
    }

    public void setApps(List<App> apps) {
        this.apps = apps;
    }
}
