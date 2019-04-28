// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.mockito.ArgumentCaptor;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.net.URI;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class YarnSparkApplicationDriverLogScenario {
    private SparkBatchSubmission submissionMock;
    private Throwable caught;
    private ArgumentCaptor<PostBatches> submissionParameterArgumentCaptor;
    private MockHttpService httpServerMock;
    private YarnCluster yarnClusterMock;
    private YarnSparkApplicationDriverLog yarnDriverLogMock;
    private TestLogger logger = TestLoggerFactory.getTestLogger(YarnSparkApplicationDriverLog.class);

    @Before
    public void setUp() throws Throwable {
        submissionParameterArgumentCaptor = ArgumentCaptor.forClass(PostBatches.class);
        submissionMock = mock(SparkBatchSubmission.class);
        when(submissionMock.getBatchSparkJobStatus(anyString(), anyInt())).thenCallRealMethod();
        when(submissionMock.getHttpResponseViaGet(anyString())).thenCallRealMethod();
        when(submissionMock.getHttpClient()).thenCallRealMethod();
        when(submissionMock.createBatchSparkJob(anyString(), submissionParameterArgumentCaptor.capture())).thenCallRealMethod();

        caught = null;

        this.httpServerMock = new MockHttpService();
    }

    @Given("^setup a mock Yarn service for (.+) request '(.+)' to return '(.+)' with status code (\\d+)$")
    public void mockLivyService(String action, String serviceUrl, String response, int statusCode) {
        httpServerMock.stub(action, serviceUrl, statusCode, response);
    }

    @Given("^prepare a Yarn cluster with Node Manager base URL (\\S*) and UI base URL (\\S*)$")
    public void prepareAYarnClusterWithNodeManagerBaseURLAndUIBaseURL(String nmBaseMock, String uiBaseMock) {
        yarnClusterMock = mock(YarnCluster.class);
        when(yarnClusterMock.getYarnNMConnectionUrl()).thenReturn(httpServerMock.normalizeResponse(nmBaseMock));
        when(yarnClusterMock.getYarnUIBaseUrl()).thenReturn(httpServerMock.normalizeResponse(uiBaseMock));
    }

    @Given("^create a yarn application driver with id (.+)$")
    public void createAYarnApplicationDriverWithId(String appIdMock) {
        yarnDriverLogMock = new YarnSparkApplicationDriverLog(appIdMock, yarnClusterMock, submissionMock);
    }

    @Then("^Parsing driver HTTP address '(.+)' should get host '(.+)'$")
    public void checkParsingDriverHTTPAddressHost(
            String httpAddress,
            String expectedHost) {
        assertEquals(expectedHost, yarnDriverLogMock.parseAmHostHttpAddressHost(httpAddress));
    }

    @Then("^Parsing driver HTTP address '(.+)' should be null$")
    public void checkParsingDriverHTTPAddressHostFailure(String httpAddress) {
        assertNull(yarnDriverLogMock.parseAmHostHttpAddressHost(httpAddress));
    }

    @Then("^getting Spark driver host should be '(.+)'$")
    public void checkGetSparkDriverHost(String expectedHost) {
        try {
            assertEquals(expectedHost, yarnDriverLogMock.getDriverHost().toBlocking().single());
        } catch (Exception e) {
            caught = e.getCause();
            assertEquals(expectedHost, "__exception_got__");
        }
    }

    @Then("^getting current Yarn App attempt should be '(.+)'$")
    public void checkGetCurrentYarnAppAttemptResult(String appAttemptLogsUrlExpect) {
        URI appAttemptLogsLink = yarnDriverLogMock
                .getSparkJobYarnCurrentAppAttemptLogsLink(yarnDriverLogMock.getApplicationId())
                .toBlocking()
                .first();

        assertEquals(appAttemptLogsUrlExpect, appAttemptLogsLink.toString());
    }

    @Then("^getting Spark Job driver log URL Observable should be '(.+)'$")
    public void checkSparkJobDriverLogURLObservable(String expect) {
        URI url = yarnDriverLogMock.getSparkJobDriverLogUrlObservable().toBlocking().last();

        assertEquals(httpServerMock.normalizeResponse(expect), url.toString());
    }

    @Then("^getting Spark Job driver log URL Observable should be empty$")
    public void gettingSparkJobDriverLogURLObservableShouldBeEmpty() throws Throwable {
        assertTrue(yarnDriverLogMock.getSparkJobDriverLogUrlObservable().isEmpty().toBlocking().last());
    }

    @After
    public void cleanUp(){
        TestLoggerFactory.clear();
    }
}
