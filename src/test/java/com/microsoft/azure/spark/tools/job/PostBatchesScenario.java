// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PostBatchesScenario {
    private static ObjectMapper mapper = new ObjectMapper();
    private String mockSelectedClusterName = "";
    private boolean mockIsLocalArtifactRadioButtionSelected = true;
    private String mockArtifactName = "";
    private String mockLocalArtifactPath = "";
    private String mockFilePath = "";
    private String mockClassName = "";
    private List<String> mockReferencedFiles = new ArrayList<>();
    private List<String> mockReferencedJars = new ArrayList<>();
    private List<String> mockArgs = new ArrayList<>();

    private PostBatches sparkSubmissionParameter = new PostBatches();
    private Map<String, String> sparkConfig;

    @Given("^apply spark configs$")
    public void applySparkConfigures(Map<String, String> config) throws Throwable {
        List<Pair<String, String>> mergedJobConf = sparkSubmissionParameter.flatJobConfig();
        config.forEach( (k, v) -> mergedJobConf.add(Pair.of(k, v)));
        sparkSubmissionParameter.applyFlattedJobConf(mergedJobConf);
    }


    @Given("^create PostBatches with the following job config$")
    public void createPostBatchesWithJobConfig(Map<String, String> jobConfig) throws Throwable {
        sparkSubmissionParameter = new PostBatches(
                mockSelectedClusterName,
                mockIsLocalArtifactRadioButtionSelected,
                mockArtifactName,
                mockLocalArtifactPath,
                mockFilePath,
                mockClassName,
                mockReferencedFiles,
                mockReferencedJars,
                mockArgs,
                new HashMap<>());

        applySparkConfigures(jobConfig);
    }

    @Then("^the parameter map should include key '(.+)' with value '(.+)'$")
    public void verifyParameterKeyAndValueExist(String key, String value) throws Throwable{
        Map<String, Object> param = sparkSubmissionParameter.getJobConfig();
        assertTrue(param.containsKey(key));
        assertEquals(value, param.get(key).toString());
    }

    @Then("^the serialized JSON should be '(.+)'$")
    public void verifySerializedJSON(String json) throws Throwable {
        String actualJson = sparkSubmissionParameter.serializeToJson();

        assertThat(mapper.readValue(actualJson, HashMap.class)).isEqualTo(mapper.readValue(json, HashMap.class));
    }


    @And("^mock className to (.+)$")
    public void mockClassName(String className) throws Throwable {
        mockClassName = className;
    }

    @And("^mock reference jars to (.+)$")
    public void mockReferenceJars(String jars) throws Throwable {
        mockReferencedJars = Arrays.asList(jars.split(","));
    }

    @And("^mock args to (.+)$")
    public void mockArgs(String args) throws Throwable {
        mockArgs = Arrays.asList(args.split(","));
    }

    @And("^mock file to (.+)$")
    public void mockFilePath(String filePath) throws Throwable {
        mockFilePath = filePath;
    }

    @And("^mock reference files to (.+)$")
    public void mockReferencedFiles(String files) throws Throwable {
        mockReferencedFiles = Arrays.asList(files.split(","));
    }
}