// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public class UnclassifiedHttpErrorStatus extends HttpErrorStatus {
    public UnclassifiedHttpErrorStatus(
            final int statusCode,
            final String message,
            final @Nullable Header[] headers,
            final @Nullable HttpEntity entity) {
        super(statusCode, message, headers, entity);
    }
}
