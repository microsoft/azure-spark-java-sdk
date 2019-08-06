// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

import com.microsoft.azure.spark.tools.job.Deployable;

public interface DefaultStorage {
    Deployable createDefaultDeployable();
}
