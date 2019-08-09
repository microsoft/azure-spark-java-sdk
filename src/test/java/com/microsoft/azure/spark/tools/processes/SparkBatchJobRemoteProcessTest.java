// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "html:target/cucumber/"
                        + SparkBatchJobRemoteProcessTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + SparkBatchJobRemoteProcessTest.SCENARIO_ID
                        + ".feature"
        }
)

public class SparkBatchJobRemoteProcessTest {
    static final String SCENARIO_ID = "processes/SparkBatchJobRemoteProcessScenario";
}
