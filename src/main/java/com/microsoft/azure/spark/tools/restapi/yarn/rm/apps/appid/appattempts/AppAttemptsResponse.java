// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm.apps.appid.appattempts;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("nullness")
public class AppAttemptsResponse {
    public static class AppAttempts {
        public List<AppAttempt> appAttempt = Collections.emptyList();
    }

    private AppAttempts appAttempts;

    public AppAttempts getAppAttempts() {
        return appAttempts;
    }
}
