// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.livy.batches;

public enum BatchState {
    NOT_STARTED("not_started"),
    STARTING("starting"),
    RECOVERING("recovering"),
    IDLE("idle"),
    RUNNING("running"),
    BUSY("busy"),
    SHUTTING_DOWN("shutting_down"),
    ERROR("error"),
    DEAD("dead"),
    SUCCESS("success");

    private final String state;

    BatchState(String state) {
        this.state = state;
    }


    @Override
    public String toString() {
        return state;
    }
}
