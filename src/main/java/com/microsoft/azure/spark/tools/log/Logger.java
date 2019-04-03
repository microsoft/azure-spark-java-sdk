// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.log;


import org.slf4j.LoggerFactory;

/**
 * Base logger class.
 */
public interface Logger {
    default org.slf4j.Logger log() {
        return LoggerFactory.getLogger(getClass());
    }
}
