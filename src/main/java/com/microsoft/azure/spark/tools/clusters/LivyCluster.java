// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

import java.net.URI;

public interface LivyCluster {
    String getLivyConnectionUrl();

    default String getLivyBatchUrl() {
        return URI.create(getLivyConnectionUrl()).resolve("batches").toString();
    }

    default String getLivySessionUrl() {
        return URI.create(getLivyConnectionUrl()).resolve("sessions").toString();
    }

}
