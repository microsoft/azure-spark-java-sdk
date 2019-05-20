// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

import java.io.IOException;

/**
 * The Base Exception for all Spark Job related exceptions.
 */
public class SparkJobException extends IOException {
    public SparkJobException(String message) {
        super(message);
    }

    public SparkJobException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
