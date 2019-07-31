// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

public class Configurations {
    public static final String BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY =
            "spark.tools.client.bypassLivySslCertValidation";
    public static final String TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY =
            "spark.tools.client.trustLivySslAllHostStrategy";
}
