// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.http.message.BasicHeader;

public class AuthorizationHeader extends BasicHeader {
    public static final String TOKEN_HEADER_NAME = "Authorization";

    public static final class OAuthTokenHeader extends AuthorizationHeader {
        public static final String OAUTH_TOKEN_PREFIX = "Bearer";
        /**
         * Constructs with name and value.
         *
         * @param token the header value
         */
        public OAuthTokenHeader(String token) {
            super(String.format("%s %s", OAUTH_TOKEN_PREFIX, token));
        }
    }

    /**
     * Constructs with name and value.
     *
     * @param value the header value
     */
    public AuthorizationHeader(String value) {
        super(TOKEN_HEADER_NAME, value);
    }
}
