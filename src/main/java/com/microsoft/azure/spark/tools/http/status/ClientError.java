// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class ClientError {
    private ClientError() {
        throw new AssertionError("shouldn't be instantiated");
    }

    public static class BadRequestHttpErrorStatus extends HttpErrorStatus {
        public BadRequestHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(400, message, headers, entity);
        }
    }

    public static class UnauthorizedHttpErrorStatus extends HttpErrorStatus {
        public UnauthorizedHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(401, message, headers, entity);
        }
    }

    public static class ForbiddenHttpErrorStatus extends HttpErrorStatus {
        public ForbiddenHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(403, message, headers, entity);
        }
    }

    public static class NotFoundHttpErrorStatus extends HttpErrorStatus {
        public NotFoundHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(404, message, headers, entity);
        }
    }

    public static class MethodNotAllowedHttpErrorStatus extends HttpErrorStatus {
        public MethodNotAllowedHttpErrorStatus(
                String message,
                @Nullable Header[] headers,
                @Nullable HttpEntity entity) {
            super(405, message, headers, entity);
        }
    }
}
