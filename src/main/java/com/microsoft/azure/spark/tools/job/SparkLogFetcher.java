// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import rx.Observable;

public interface SparkLogFetcher {
    Observable<String> fetch(String type, long logOffset, int size);

    Observable<String> awaitLogAggregationDone();
}