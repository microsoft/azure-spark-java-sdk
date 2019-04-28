// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

public class NotInitializedException extends RuntimeException {
    public NotInitializedException(final String message) {
        super(message);
    }
}
