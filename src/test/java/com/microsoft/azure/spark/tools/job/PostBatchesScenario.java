// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.JsonConverter;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PostBatchesScenario {
    private JsonConverter converter = JsonConverter.of(PostBatches.class);
    private PostBatches.Options sparkParameterOptions = new PostBatches.Options();

    @Given("^apply spark configs$")
    public void applySparkConfigures(Map<String, String> config) {
        config.forEach( (k, v) -> sparkParameterOptions.conf(k, v));
    }


    @Given("^create PostBatches with the following job config$")
    public void createPostBatchesWithJobConfig(Map<String, String> jobConfig) {
        applySparkConfigures(jobConfig);
    }

    @Then("^the parameter map should include key '(.+)' with value '(.+)'$")
    public void verifyParameterKeyAndValueExist(String key, String value) {
        Map<String, Object> param = JsonConverter.of(Map.class).parseFrom(converter.toJson(sparkParameterOptions.build()));
        assertTrue(param.containsKey(key));
        assertEquals(value, param.get(key).toString());
    }

    @Then("^the serialized JSON should be '(.+)'$")
    public void verifySerializedJSON(String expect) throws Throwable {
        String actualJson = converter.toJson(sparkParameterOptions.build());

        JSONAssert.assertEquals(expect, actualJson, JSONCompareMode.STRICT);
    }

    @Then("^the convertToJson result should be '(.+)'$")
    public void verifyConvertToJSON(String expect) throws Throwable {
        String actualJson = sparkParameterOptions.build().convertToJson();

        JSONAssert.assertEquals(expect, actualJson, JSONCompareMode.STRICT);
    }

    @And("^mock className to (.+)$")
    public void mockClassName(String className) {
        sparkParameterOptions.className(className);
    }

    @And("^mock reference jars to (.+)$")
    public void mockReferenceJars(String jars) {
        sparkParameterOptions.referJars(jars.split(","));
    }

    @And("^mock args to (.+)$")
    public void mockArgs(String args) {
        sparkParameterOptions.args(args.split(","));
    }

    @And("^mock file to (.+)$")
    public void mockFilePath(String filePath) {
        sparkParameterOptions.artifactUri(filePath);
    }

    @And("^mock reference files to (.+)$")
    public void mockReferencedFiles(String files) {
        sparkParameterOptions.referFiles(files.split(","));
    }

    @And("mock application name to (.+)$")
    public void mockApplicationName(String appName) {
        sparkParameterOptions.name(appName);
    }

    @And("mock executor memory size to {float} Gigabytes")
    public void mockExecutorMemorySizeToGigabytes(float size) {
        sparkParameterOptions.setExecutorMemory(new PostBatches.MemorySize(size, PostBatches.MemorySize.Unit.GIGABYTES));
    }

    @And("mock driver memory size to {int} Megabytes")
    public void mockDriverMemorySizeToMegabytes(int size) {
        sparkParameterOptions.setDriverMemory(new PostBatches.MemorySize(size, PostBatches.MemorySize.Unit.MEGABYTES));
    }
}