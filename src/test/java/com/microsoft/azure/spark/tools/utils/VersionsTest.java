package com.microsoft.azure.spark.tools.utils;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {
                "html:target/cucumber/"
                        + VersionsTest.SCENARIO_ID
        },
        features = {
                "src/test/resources/com/microsoft/azure/spark/tools/"
                        + VersionsTest.SCENARIO_ID
                        + ".feature"
        }
)
public class VersionsTest {
    static final String SCENARIO_ID = "utils/VersionsScenario";
}
