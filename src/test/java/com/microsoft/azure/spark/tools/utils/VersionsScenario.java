package com.microsoft.azure.spark.tools.utils;

import cucumber.api.java.en.Then;

import static com.microsoft.azure.spark.tools.utils.Versions.UNKNOWN_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class VersionsScenario {
    @Then("^Versions should not be `(.+)`")
    public void checkVersion(String notExpect) {
        assertNotEquals(UNKNOWN_VERSION, Versions.getVersion());
        assertNotEquals(notExpect, Versions.getVersion());
    }

    @Then("^Production ID should be `(.+)`$")
    public void checkProductionID(String expect) {
        assertEquals(expect, Versions.getId());

    }
}
