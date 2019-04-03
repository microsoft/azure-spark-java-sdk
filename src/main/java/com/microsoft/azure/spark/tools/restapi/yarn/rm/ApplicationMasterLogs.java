// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;


import org.checkerframework.checker.nullness.qual.Nullable;

public class ApplicationMasterLogs {
    @Nullable
    private String stderr;
    @Nullable
    private String stdout;
    @Nullable
    private String directoryInfo;

    public ApplicationMasterLogs(@Nullable String standout, @Nullable String standerr, @Nullable String directoryInfo) {
        this.stdout = standout;
        this.stderr = standerr;
        this.directoryInfo = directoryInfo;
    }

    public ApplicationMasterLogs() {

    }

    @Nullable
    public String getStderr() {
        return stderr;
    }

    public void setStderr(@Nullable String stderr) {
        this.stderr = stderr;
    }

    @Nullable
    public String getStdout() {
        return stdout;
    }

    public void setStdout(@Nullable String stdout) {
        this.stdout = stdout;
    }

    @Nullable
    public String getDirectoryInfo() {
        return directoryInfo;
    }

    public void setDirectoryInfo(@Nullable String directoryInfo) {
        this.directoryInfo = directoryInfo;
    }
}
