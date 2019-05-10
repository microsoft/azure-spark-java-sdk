// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
package com.microsoft.azure.spark.tools.legacyhttp;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Optional;


public final class ObjectConvertUtils {
    private static JsonFactory jsonFactory = new JsonFactory();
    private static ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
    private static XmlMapper xmlMapper = new XmlMapper();

    public static <T> Optional<T> convertJsonToObject(String jsonString, Class<T> tClass) throws IOException {
        return Optional.ofNullable(objectMapper.readValue(jsonString, tClass));
    }

    public static @Nullable <T> T convertToObjectQuietly(String jsonString, Class<T> tClass) {
        try {
            return objectMapper.readValue(jsonString, tClass);
        } catch (IOException e) {
            // ignore the exception
        }
        return null;
    }

    public static <T> Optional<T> convertEntityToObject(HttpEntity entity, Class<T> tClass) throws IOException {
        final String type = entity.getContentType().getValue().toLowerCase();

        switch (type) {
            case "application/json" :
                return convertJsonToObject(EntityUtils.toString(entity), tClass);
            case "application/xml" :
                return convertXmlToObject(EntityUtils.toString(entity), tClass);
            default:
        }

        return Optional.empty();
    }

    public static <T> Optional<String> convertObjectToJsonString(T obj) {
        try {
            return Optional.ofNullable(objectMapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    public static <T> Optional<String> convertObjectToXmlString(T obj) {
        try {
            return Optional.ofNullable(xmlMapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private static <T> Optional<T> convertXmlToObject(String xmlString, Class<T> tClass) throws IOException {
        return Optional.ofNullable(xmlMapper.readValue(xmlString, tClass));
    }
}