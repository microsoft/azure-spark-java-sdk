// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

public final class JsonConverter<T> {
    @SuppressWarnings("initialization")
    private final Class<T> targetType;

    private JsonConverter(final Class<T> targetType) {
        this.targetType = targetType;
    }

    private JsonFactory jsonFactory = new JsonFactory();
    private ObjectMapper objectMapper = new ObjectMapper(jsonFactory);

    public @Nullable T parseFrom(final String jsonString) {
        try {
            return objectMapper.readValue(jsonString, targetType);
        } catch (IOException e) {
            return null;
        }
    }

    public String toJson(final Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    public static <T> JsonConverter<T> of(final Class<T> type) {
        return new JsonConverter<>(type);
    }
}
