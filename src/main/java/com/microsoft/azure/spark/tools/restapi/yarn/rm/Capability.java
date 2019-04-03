// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm;


import com.microsoft.azure.spark.tools.restapi.Convertible;

@SuppressWarnings("nullness")
public class Capability implements Convertible {
    private String virtualCores;

    private String memorySize;

    private String memory;

    public String getVirtualCores() {
        return virtualCores;
    }

    public void setVirtualCores(String virtualCores) {
        this.virtualCores = virtualCores;
    }

    public String getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(String memorySize) {
        this.memorySize = memorySize;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }
}
