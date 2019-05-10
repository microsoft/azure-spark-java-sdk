// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;


import org.checkerframework.checker.nullness.qual.Nullable;

public class ApplicationMasterLogs {
    private @Nullable String stderr;
    private @Nullable String stdout;
    private @Nullable String directoryInfo;

    public ApplicationMasterLogs(@Nullable String standout, @Nullable String standerr, @Nullable String directoryInfo) {
        this.stdout = standout;
        this.stderr = standerr;
        this.directoryInfo = directoryInfo;
    }

    public ApplicationMasterLogs() {

    }

    public @Nullable String getStderr() {
        return stderr;
    }

    public void setStderr(@Nullable String stderr) {
        this.stderr = stderr;
    }

    public @Nullable String getStdout() {
        return stdout;
    }

    public void setStdout(@Nullable String stdout) {
        this.stdout = stdout;
    }

    public @Nullable String getDirectoryInfo() {
        return directoryInfo;
    }

    public void setDirectoryInfo(@Nullable String directoryInfo) {
        this.directoryInfo = directoryInfo;
    }
}
