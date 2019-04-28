// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.utils.Pair;
import rx.Observable;
import java.net.URI;

interface SparkDriverLog {
    Observable<URI> getYarnNMConnectUri();

    Observable<String> getDriverHost();

    Observable<Pair<String, Long>> getDriverLog(String type, long logOffset, int size);
}