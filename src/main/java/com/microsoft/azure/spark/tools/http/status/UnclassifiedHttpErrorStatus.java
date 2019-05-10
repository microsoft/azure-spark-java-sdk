// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public class UnclassifiedHttpErrorStatus extends HttpErrorStatus {
    public UnclassifiedHttpErrorStatus(
            int statusCode,
            String message,
            @Nullable Header[] headers,
            @Nullable HttpEntity entity) {
        super(statusCode, message, headers, entity);
    }
}
