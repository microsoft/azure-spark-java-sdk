// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

import java.io.IOException;

/**
 * The base exception class for HDInsight cluster.
 */
public class HDIException extends IOException {
    public HDIException(String message) {
        super(message);
    }

    public HDIException(String message, Throwable cause) {
        super(message, cause);
    }

    public HDIException(Throwable cause) {
        super(cause);
    }
}