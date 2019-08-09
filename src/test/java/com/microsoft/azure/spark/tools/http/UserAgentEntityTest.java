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
                        + UserAgentEntityTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + UserAgentEntityTest.SCENARIO_ID
                        + ".feature"
        }
)
public class UserAgentEntityTest {
    static final String SCENARIO_ID = "http/UserAgentEntityScenario";
}
