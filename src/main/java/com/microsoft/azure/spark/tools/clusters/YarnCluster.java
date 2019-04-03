// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

public interface YarnCluster {
    String getYarnNMConnectionUrl();

    String getYarnUIBaseUrl();
}
