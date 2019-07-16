package com.microsoft.azure.spark.tools.utils;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        plugin = {"html:target/cucumber"},
        features = { "src/test/resources/com/microsoft/azure/spark/tools/" +
                "utils/VersionsScenario.feature"}
)
public class VersionsTest {
}
