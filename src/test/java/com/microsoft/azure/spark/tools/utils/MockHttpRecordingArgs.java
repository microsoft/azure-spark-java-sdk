// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URL;
import java.util.concurrent.Callable;

@CommandLine.Command(
        description = "Record all Mock HTTP service requests and response for testing",
        version = "1.0")
public class MockHttpRecordingArgs implements Callable<Void> {
    @Parameters(index = "0", description = "The target prefix URL for recording")
    private URL targetUrl;

    @Option(names = "--username", description = "Basic Auth Username")
    private String username;

    @Option(names = "--password", description = "Basic Auth Password")
    private String password;

    public URL getTargetUrl() {
        return targetUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public Void call() throws Exception {
        return null;
    }
}
