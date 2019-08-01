// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;

public class AuthorizationHeader extends BasicHeader {
    public static final class OAuthTokenHeader extends AuthorizationHeader {
        public static final String OAUTH_TOKEN_PREFIX = "Bearer";
        /**
         * Constructs with name and value.
         *
         * @param token the access token
         */
        public OAuthTokenHeader(final String token) {
            super(String.format("%s %s", OAUTH_TOKEN_PREFIX, token));
        }
    }

    public static final class BasicAuthHeader extends AuthorizationHeader {
        public static final String BASIC_AUTH_PREFIX = "Basic";
        /**
         * Constructs with name and value.
         *
         * @param encodedAuth the encoded auth byte array
         */
        public BasicAuthHeader(final byte[] encodedAuth) {
            super(String.format("%s %s", BASIC_AUTH_PREFIX, new String(encodedAuth)));
        }
    }

    /**
     * Constructs with name and value.
     *
     * @param value the header value
     */
    public AuthorizationHeader(final String value) {
        super(HttpHeaders.AUTHORIZATION, value);
    }
}
