// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class ServerError {
    private ServerError() {
        throw new AssertionError("shouldn't be instantiated");
    }

    public static class InternalServerErrorHttpErrorStatus extends HttpErrorStatus {
        public InternalServerErrorHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(500, message, headers, entity);
        }
    }
}
