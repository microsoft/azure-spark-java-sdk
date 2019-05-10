// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.microsoft.azure.spark.tools.utils.Lazy;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpResponse {
    private final CloseableHttpResponse raw;

    private Lazy<String> message = new Lazy<>();

    public HttpResponse(final CloseableHttpResponse response) {
        this.raw = response;
    }

    public CloseableHttpResponse getRaw() {
        return raw;
    }

    public int getCode() {
        return getRaw().getStatusLine().getStatusCode();
    }

    public String getMessage() throws IOException {
        try {
            return this.message.getOrEvaluate(() -> {
                try {
                    return EntityUtils.toString(getRaw().getEntity());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException innerEx) {
            throw (IOException) innerEx.getCause();
        }
    }

    public Header[] getHeaders() {
        return raw.getAllHeaders();
    }

    public String getReason() {
        return raw.getStatusLine().getReasonPhrase();
    }
}