// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.events;

public enum MessageInfoType {
    Trace(10),
    Debug(20),
    Info(30),
    Warning(40),
    Error(50),
    Fatal(60),

    Log(100),

    Hyperlink(1000);

    private final int value;

    MessageInfoType(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}