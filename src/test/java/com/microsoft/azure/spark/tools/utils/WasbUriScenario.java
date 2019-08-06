// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import cucumber.api.java.en.Then;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WasbUriScenario {
    @Then("^check parsing WASB URI table should be")
    public void checkWasbUriParsingTable(List<Map<String, String>> table) {
        table.forEach(row -> {
            WasbUri actureUri = WasbUri.parse(row.get("rawUri"));

            assertEquals(row.get("container"), actureUri.getContainer());
            assertEquals(row.get("storageAccount"), actureUri.getStorageAccount());
            assertEquals(row.get("endpointSuffix"), actureUri.getEndpointSuffix());
            assertEquals(row.get("absolutePath"), actureUri.getAbsolutePath());
        });

    }
}
