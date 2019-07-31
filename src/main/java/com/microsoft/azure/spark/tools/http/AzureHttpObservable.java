// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AzureHttpObservable extends OAuthTokenHttpObservable {
    public static class ApiVersionParam extends BasicNameValuePair {
        public static final String NAME = "api-version";

        /**
         * Default Constructor taking a name and a value.
         *
         * @param value The value.
         */
        public ApiVersionParam(String value) {
            super(NAME, value);
        }
    }

    private final List<ApiVersionParam> azureDefaultParameters;

    public AzureHttpObservable(final AzureOAuthTokenFetcher accessTokenFetcher) {
        this(accessTokenFetcher, null);
    }

    public AzureHttpObservable(final AzureOAuthTokenFetcher accessTokenFetcher, @Nullable String apiVersion) {
        super(accessTokenFetcher);

        if (StringUtils.isNotBlank(apiVersion)) {
            this.azureDefaultParameters = Arrays.asList(new ApiVersionParam(apiVersion));
        } else {
            this.azureDefaultParameters = Collections.emptyList();
        }
    }

    @Override
    public List<NameValuePair> getDefaultParameters() {
        final Stream<NameValuePair> oAuthDefaultParameters = super.getDefaultParameters().stream();

        // Replace the duplicated parameters from parent defaults
        return Stream.concat(
                oAuthDefaultParameters.filter(nameValuePair ->
                        this.azureDefaultParameters.stream().noneMatch(azureDefaultParam ->
                                StringUtils.equalsIgnoreCase(nameValuePair.getName(), azureDefaultParam.getName()))),
                this.azureDefaultParameters.stream()).collect(Collectors.toList());
    }
}
