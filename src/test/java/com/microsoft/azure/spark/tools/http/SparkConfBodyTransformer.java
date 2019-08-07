// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.EqualToJsonPattern;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import wiremock.com.fasterxml.jackson.databind.JsonNode;
import wiremock.com.fasterxml.jackson.databind.node.TextNode;
import wiremock.com.google.common.collect.Streams;

import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

public class SparkConfBodyTransformer extends StubMappingTransformer {
    public static final String NAME = "mask-spark-conf";

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
        ListIterator<ContentPattern<?>> iterator = Optional.ofNullable(stubMapping)
                .map(StubMapping::getRequest)
                .map(RequestPattern::getBodyPatterns)
                .map(List::listIterator)
                .orElse(null);

        while (iterator != null && iterator.hasNext()) {
            ContentPattern contentPattern = iterator.next();

            if (contentPattern instanceof EqualToJsonPattern) {
                EqualToJsonPattern pattern = (EqualToJsonPattern) contentPattern;
                JsonNode json = Json.read(pattern.getEqualToJson(), JsonNode.class);

                JsonNode confNode = json.findValue("conf");
                if (confNode != null && confNode.isObject()) {
                    Streams.stream(confNode.fields())
                            .forEach(entry -> {
                                Object paramValue = parameters.get(param(entry.getKey()));

                                if (paramValue != null) {
                                    entry.setValue(new TextNode(String.valueOf(paramValue)));
                                }
                            });
                }

                iterator.remove();
                iterator.add(new EqualToJsonPattern(
                        json, pattern.isIgnoreArrayOrder(), pattern.isIgnoreExtraElements()));
            }
        }

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
