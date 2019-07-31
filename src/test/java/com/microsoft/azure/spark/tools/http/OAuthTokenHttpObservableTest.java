// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {"html:target/cucumber"},
        features = { "src/test/resources/com/microsoft/azure/spark/tools/" +
                "http/OAuthTokenHttpObservableScenario.feature"}
)
public class OAuthTokenHttpObservableTest {
}
