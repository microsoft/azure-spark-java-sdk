// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import java.io.IOException;

/**
 * The interface to fetcher OAuth 2.0 access token.
 */
public interface OAuthTokenFetcher {
    /**
     * The method to get (or refresh) the access token for OAuth 2.0.
     *
     * @return the access token
     * @throws IOException for the networking issue in the progress of getting
     */
    String get() throws IOException;
}
