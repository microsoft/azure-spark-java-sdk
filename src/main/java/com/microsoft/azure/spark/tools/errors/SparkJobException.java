// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

/**
 * The Base Exception for all Spark Job related exceptions.
 */
public class SparkJobException extends HDIException {
    public SparkJobException(String message) {
        super(message);
    }

    public SparkJobException(String message, int errorCode) {
        super(message, errorCode);
    }

    public SparkJobException(String message, String errorLog) {
        super(message, errorLog);
    }

    public SparkJobException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
