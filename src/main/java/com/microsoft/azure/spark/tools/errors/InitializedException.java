// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

public class InitializedException extends RuntimeException {
    public InitializedException(final String message) {
        super(message);
    }
}
