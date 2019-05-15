// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ClientError extends HttpErrorStatus {
    private ClientError(final int statusCode,
                        final String message,
                        final @Nullable Header[] headers,
                        final @Nullable HttpEntity entity) {
        super(statusCode, message, headers, entity);
    }

    public static class BadRequestHttpErrorStatus extends ClientError {
        public BadRequestHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(400, message, headers, entity);
        }
    }

    public static class UnauthorizedHttpErrorStatus extends ClientError {
        public UnauthorizedHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(401, message, headers, entity);
        }
    }

    public static class ForbiddenHttpErrorStatus extends ClientError {
        public ForbiddenHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(403, message, headers, entity);
        }
    }

    public static class NotFoundHttpErrorStatus extends ClientError {
        public NotFoundHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(404, message, headers, entity);
        }
    }

    public static class MethodNotAllowedHttpErrorStatus extends ClientError {
        public MethodNotAllowedHttpErrorStatus(
                final String message,
                final @Nullable Header[] headers,
                final @Nullable HttpEntity entity) {
            super(405, message, headers, entity);
        }
    }
}
