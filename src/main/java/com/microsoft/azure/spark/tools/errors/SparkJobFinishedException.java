// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

/**
 * The Exception for probing the running Spark Batch job status, but the Spark Batch Job is finished.
 */
public class SparkJobFinishedException extends SparkJobException {
    public SparkJobFinishedException(String message) {
        super(message);
    }

    public SparkJobFinishedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
