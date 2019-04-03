// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

import java.net.UnknownServiceException;

public class SparkJobNotConfiguredException extends UnknownServiceException {
    public SparkJobNotConfiguredException(String message) {
        super(message);
    }
}
