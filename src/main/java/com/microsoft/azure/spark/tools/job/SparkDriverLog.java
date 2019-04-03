// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import rx.Observable;
import java.net.URI;
import java.util.AbstractMap;

interface SparkDriverLog {
    URI getYarnNMConnectUri();

    Observable<String> getDriverHost();

    Observable<AbstractMap.SimpleImmutableEntry<String, Long>> getDriverLog(String type, long logOffset, int size);
}