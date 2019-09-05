// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.yarn.rm.apps.appid;


import com.microsoft.azure.spark.tools.restapi.Convertible;

@SuppressWarnings("nullness")
public class ResourceRequest implements Convertible {
    private String nodeLabelExpression;

    private Priority priority;

    private String relaxLocality;

    private String numContainers;

    private Capability capability;

    private String resourceName;

    public String getNodeLabelExpression() {
        return nodeLabelExpression;
    }

    public Priority getPriority() {
        return priority;
    }

    public String getRelaxLocality() {
        return relaxLocality;
    }

    public String getNumContainers() {
        return numContainers;
    }

    public Capability getCapability() {
        return capability;
    }

    public String getResourceName() {
        return resourceName;
    }

    public static class Capability implements Convertible {
        private String virtualCores;

        private String memorySize;

        private String memory;

        public String getVirtualCores() {
            return virtualCores;
        }

        public String getMemorySize() {
            return memorySize;
        }

        public String getMemory() {
            return memory;
        }
    }

    public static class Priority implements Convertible {
        private String priority;

        public String getPriority() {
            return priority;
        }
    }
}
