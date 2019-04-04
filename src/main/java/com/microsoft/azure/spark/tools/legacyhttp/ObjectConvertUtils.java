// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
package com.microsoft.azure.spark.tools.legacyhttp;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;


public final class ObjectConvertUtils {
    private static JsonFactory jsonFactory = new JsonFactory();
    private static ObjectMapper objectMapper = new ObjectMapper(jsonFactory);
    private static XmlMapper xmlMapper = new XmlMapper();

    public static  <T> Optional<T> convertJsonToObject(String jsonString, Class<T> tClass) throws IOException {
        return Optional.ofNullable(objectMapper.readValue(jsonString, tClass));
    }

    @Nullable
    public static <T> T convertToObjectQuietly(String jsonString, Class<T> tClass) {
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

    public static <T> Optional<List<T>> convertEntityToList(HttpEntity entity, Class<T> tClass) throws IOException {
        final String type = entity.getContentType().getValue().toLowerCase();
        switch (type) {
            case "application/json" :
                return convertJsonToList(EntityUtils.toString(entity), tClass);
            case "application/xml" :
                return convertXmlToList(EntityUtils.toString(entity), tClass);
            default:
        }
        return Optional.empty();
    }

    private static <T> Optional<List<T>> convertJsonToList(String jsonString, Class<T> tClass) throws IOException {
        List<T> myLists = objectMapper.readValue(
                jsonString, TypeFactory.defaultInstance().constructCollectionType(List.class, tClass));
        return Optional.ofNullable(myLists);
    }

    private static <T> Optional<List<T>> convertXmlToList(String jsonString, Class<T> tClass) throws IOException {
        List<T> myLists = xmlMapper.readValue(
                jsonString, TypeFactory.defaultInstance().constructCollectionType(List.class, tClass));
        return Optional.ofNullable(myLists);
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