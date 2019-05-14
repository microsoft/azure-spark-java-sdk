// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi;

import com.microsoft.azure.spark.tools.utils.JsonConverter;

/**
 * The interface is to provide the convert methods for JSON/XML objects.
 */
public interface Convertible {
    // serialize an object to xml-format string
//    default Optional<String> convertToXml() {
//    }

    // serialize an object to json-format string
    default String convertToJson() {
        return JsonConverter.of(this.getClass()).toJson(this);
    }
}
