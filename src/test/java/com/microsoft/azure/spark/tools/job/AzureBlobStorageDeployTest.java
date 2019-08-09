// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "html:target/cucumber/"
                        + AzureBlobStorageDeployTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + AzureBlobStorageDeployTest.SCENARIO_ID
                        + ".feature"
        }
)
public class AzureBlobStorageDeployTest {
    static final String SCENARIO_ID = "job/AzureBlobStorageDeployScenario";
}
