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
                        + OAuthTokenHttpObservableTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + OAuthTokenHttpObservableTest.SCENARIO_ID
                        + ".feature"
        }
)
public class OAuthTokenHttpObservableTest {
    static final String SCENARIO_ID = "http/OAuthTokenHttpObservableScenario";
}
