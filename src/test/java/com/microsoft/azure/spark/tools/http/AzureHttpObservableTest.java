// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "html:target/cucumber/"
                        + AzureHttpObservableTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + AzureHttpObservableTest.SCENARIO_ID
                        + ".feature"
        }
)

public class AzureHttpObservableTest {
    static final String SCENARIO_ID = "http/AzureHttpObservableScenario";
}
