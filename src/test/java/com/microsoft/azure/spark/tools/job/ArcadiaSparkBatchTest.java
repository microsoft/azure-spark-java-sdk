// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "pretty",
                "html:target/cucumber/job/ArcadiaSparkBatchScenario"},
        features = { "src/test/resources/com/microsoft/azure/spark/tools/" +
                "job/ArcadiaSparkBatchScenario.feature"}
)
public class ArcadiaSparkBatchTest {
}
