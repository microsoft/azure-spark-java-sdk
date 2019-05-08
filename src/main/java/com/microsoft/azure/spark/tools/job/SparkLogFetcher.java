// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.utils.Pair;
import rx.Observable;

public interface SparkLogFetcher {
    Observable<Pair<String, Long>> fetch(String type, long logOffset, int size);
}