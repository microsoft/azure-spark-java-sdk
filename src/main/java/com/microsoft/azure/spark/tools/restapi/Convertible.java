// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi;

import java.util.Optional;

/**
 * The interface is to provide the convert methods for JSON/XML objects.
 */
public interface Convertible {
    // serialize an object to xml-format string
    default Optional<String> convertToXml() {
        return ObjectConvertUtils.convertObjectToJsonString(this);
    }

    // serialize an object to json-format string
    default Optional<String> convertToJson() {
        return ObjectConvertUtils.convertObjectToJsonString(this);
    }
}
