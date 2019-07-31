// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;

import com.microsoft.azure.spark.tools.http.AuthorizationHeader.OAuthTokenHeader;
import com.microsoft.azure.spark.tools.utils.Lazy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class OAuthTokenHttpObservable extends HttpObservable {
    private final OAuthTokenFetcher accessTokenFetcher;
    private final RequestConfig oauthDefaultRequestConfig;
    private final Lazy<List<Header>> oAuthDefaultHeaders = new Lazy<>();

    public OAuthTokenHttpObservable(final OAuthTokenFetcher toGetAccessToken) {
        super();

        this.accessTokenFetcher = toGetAccessToken;
        this.oauthDefaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.DEFAULT)
                .build();
    }

    public OAuthTokenHttpObservable(final String accessToken) {
        this(() -> accessToken);
    }

    public String getAccessToken() throws IOException {
        return accessTokenFetcher.get();
    }

    @Override
    public RequestConfig getDefaultRequestConfig() {
        return oauthDefaultRequestConfig;
    }

    protected List<Header> getOAuthDefaultHeaders() throws IOException {
        try {
            return oAuthDefaultHeaders.getOrEvaluate(() -> {
                try {
                    return Arrays.asList(new OAuthTokenHeader(getAccessToken()));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (RuntimeException innerEx) {
            if (innerEx.getCause() instanceof IOException) {
                throw (IOException) innerEx.getCause();
            } else {
                throw innerEx;
            }
        }
    }

    @Override
    public Header[] getDefaultHeaders() throws IOException {
        final Stream<Header> defaultHeaders = Arrays.stream(super.getDefaultHeaders());
        final List<Header> oauthDefaults = getOAuthDefaultHeaders();

        // Replace the duplicated headers from parent defaults
        return Stream.concat(
                defaultHeaders.filter(header -> oauthDefaults.stream().noneMatch(headerToReplace ->
                        StringUtils.equalsIgnoreCase(header.getName(), headerToReplace.getName()))),
                oauthDefaults.stream()).toArray(Header[]::new);
    }
}
