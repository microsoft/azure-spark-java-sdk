// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.matching.MultiValuePattern.of;

public class AuthorizationHeaderTransformer extends StubMappingTransformer {
    public static final String NAME = "mask-authorization-header";

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
        stubMapping.getRequest().getHeaders().entrySet().stream()
                .filter(header -> parameters.containsKey(param(header.getKey())))
                .forEach(header -> header.setValue(of(
                        equalTo(String.valueOf(parameters.get(param(header.getKey())))))));

        return stubMapping;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static String param(String key) {
        return NAME + "." + key;
    }
}
