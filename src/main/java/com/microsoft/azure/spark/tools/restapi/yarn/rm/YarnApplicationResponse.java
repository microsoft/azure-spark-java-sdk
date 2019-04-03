// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;


import com.microsoft.azure.spark.tools.restapi.Convertible;

import java.util.List;
import java.util.Optional;

@SuppressWarnings("nullness")
public class YarnApplicationResponse implements Convertible {
    private YarnApplications apps;

    public YarnApplications getApps() {
        return apps;
    }

    public void setApps(YarnApplications apps) {
        this.apps = apps;
    }

    public static YarnApplicationResponse EMPTY = new YarnApplicationResponse();

    public Optional<List<App>> getAllApplication() {
        if (apps != null) {
            return Optional.ofNullable(apps.getApps());
        }
        return Optional.empty();
    }
}
