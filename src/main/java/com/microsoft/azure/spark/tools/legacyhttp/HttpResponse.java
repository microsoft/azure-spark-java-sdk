// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.legacyhttp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpResponse {
    private int code;
    private String message;
    private Map<String, List<String>> headers;
    private String content;

    public HttpResponse(int code, String message, Map<String, List<String>> headers,
                        String content) {
        this.code = code;
        this.message = message;
        this.headers = new HashMap<>(headers);
        this.content = content;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public String getContent() {
        return content;
    }
}