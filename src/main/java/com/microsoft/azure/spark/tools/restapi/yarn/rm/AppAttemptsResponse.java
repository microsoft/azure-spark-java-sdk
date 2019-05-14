// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;

import java.util.List;

@SuppressWarnings("nullness")
public class AppAttemptsResponse {
    public class AppAttempts {
        public List<AppAttempt> appAttempt;
    }

    private AppAttempts appAttempts;

    public AppAttempts getAppAttempts() {
        return appAttempts;
    }
}
