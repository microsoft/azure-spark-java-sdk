// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

/**
 * The Exception for probing the running Spark Batch job status, but the Spark Batch Job is finished.
 */
public class SparkJobFinishedException extends HDIException {
    public SparkJobFinishedException(String message) {
        super(message);
    }

    public SparkJobFinishedException(String message, int errorCode) {
        super(message, errorCode);
    }

    public SparkJobFinishedException(String message, String errorLog) {
        super(message, errorLog);
    }

    public SparkJobFinishedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
