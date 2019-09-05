// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm.apps.appid;


import com.microsoft.azure.spark.tools.restapi.Convertible;

@SuppressWarnings("nullness")
public class AppResponse implements Convertible {
    private App app;

    public App getApp() {
        return app;
    }
}
