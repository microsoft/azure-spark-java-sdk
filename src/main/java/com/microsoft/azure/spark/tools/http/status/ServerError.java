// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ServerError extends HttpErrorStatus {
    private ServerError(final int statusCode,
                        final String message,
                        final @Nullable Header[] headers,
                        final @Nullable HttpEntity entity) {
        super(statusCode, message, headers, entity);
    }

    public static class InternalServerErrorHttpErrorStatus extends ServerError {
        public InternalServerErrorHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(500, message, headers, entity);
        }
    }
}
