// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http.status;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;

public class HttpErrorStatus extends HttpException {
    private final int statusCode;


    private final @Nullable Header[] headers;


    private final @Nullable HttpEntity entity;

    public HttpErrorStatus(
            final int statusCode,
            final String message,
            final @Nullable Header[] headers,
            final @Nullable HttpEntity entity) {
        super(message);
        this.statusCode = statusCode;
        this.headers = headers;
        this.entity = entity;
    }

    public int getStatusCode() {
        return statusCode;
    }


    public @Nullable Header[] getHeaders() {
        return headers;
    }


    public @Nullable HttpEntity getEntity() {
        return entity;
    }

    public String getErrorDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append("Status Code: ").append(getStatusCode()).append("\n");

        if (getHeaders() != null) {
            String headersString = Arrays.stream(getHeaders())
                    .flatMap(header -> header == null ? empty() : of(header.getName() + ": " + header.getValue()))
                    .collect(Collectors.joining("\n"));
            sb.append("Headers:\n").append(headersString).append("\n");
        }
        sb.append("Error message: ").append(getMessage());
        return sb.toString();
    }

    public static HttpErrorStatus classifyHttpError(final CloseableHttpResponse httpResponse) throws IOException {
        StatusLine status = httpResponse.getStatusLine();
        int statusCode = status.getStatusCode();
        HttpEntity httpEntity = httpResponse.getEntity();
        String message = EntityUtils.toString(httpEntity);
        Header[] headers = httpResponse.getAllHeaders();
        if (statusCode == 400) {
            return new ClientError.BadRequestHttpErrorStatus(message, headers, httpEntity);
        } else if (statusCode == 401) {
            return new ClientError.UnauthorizedHttpErrorStatus(message, headers, httpEntity);
        } else if (statusCode == 403) {
            return new ClientError.ForbiddenHttpErrorStatus(message, headers, httpEntity);
        } else if (statusCode == 404) {
            return new ClientError.NotFoundHttpErrorStatus(message, headers, httpEntity);
        } else if (statusCode == 405) {
            return new ClientError.MethodNotAllowedHttpErrorStatus(message, headers, httpEntity);
        } else if (statusCode == 500) {
            return new ServerError.InternalServerErrorHttpErrorStatus(message, headers, httpEntity);
        } else {
            return new UnclassifiedHttpErrorStatus(statusCode, message, headers, httpEntity);
        }
    }
}
