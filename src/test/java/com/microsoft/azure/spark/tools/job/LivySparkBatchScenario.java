// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmissionMock;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import rx.Observable;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.net.URI;

import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class LivySparkBatchScenario {
    private SparkBatchSubmission submissionMock;
    private Throwable caught;
    private MockHttpService httpServerMock;
    private LivySparkBatch jobMock;
    private TestLogger logger = TestLoggerFactory.getTestLogger(LivySparkBatchScenario.class);

    @Before
    public void setUp() throws Throwable {
        submissionMock = SparkBatchSubmissionMock.create();

        jobMock = mock(LivySparkBatch.class, CALLS_REAL_METHODS);
        when(jobMock.getSubmission()).thenReturn(submissionMock);

        caught = null;

        this.httpServerMock = MockHttpService.create();
    }

    @Given("^setup a mock Livy service for (.+) request '(.+)' to return '(.+)' with status code (\\d+)$")
    public void mockLivyService(String action, String serviceUrl, String response, int statusCode) {
        httpServerMock.stub(action, serviceUrl, statusCode, response);
    }

    @Then("^throw exception '(.+)' with checking type only$")
    public void checkException(String exceptedName) throws Throwable {
        assertNotNull(caught);
        assertEquals(exceptedName, caught.getClass().getName());
    }

    @Then("^throw exception '(.+)' with message '(.*)'$")
    public void checkExceptionWithMessage(String exceptedName, String expectedMessage) throws Throwable {
        assertNotNull(caught);
        assertEquals(exceptedName, caught.getClass().getName());
        assertEquals(expectedMessage, caught.getMessage());
    }

    @Then("^getting spark job application id should be '(.+)'$")
    public void checkGetSparkJobApplicationId(
            String expectedApplicationId) throws Throwable {
        caught = null;
        try {
            assertEquals(expectedApplicationId, jobMock.getSparkJobApplicationIdObservable().toBlocking().first());
        } catch (Exception e) {
            caught = e;
            assertEquals(expectedApplicationId, "__exception_got__" + e);
        }
    }

    @Then("^getting spark job application id, '(.+)' should be got with (\\d+) times retried$")
    public void checkGetSparkJobApplicationIdRetryCount(
            String getUrl,
            int expectedRetriedCount) throws Throwable {
        when(jobMock.getDelaySeconds()).thenReturn(1);
        when(jobMock.getRetriesMax()).thenReturn(3);

        try {
            jobMock.getSparkJobApplicationIdObservable().retry(expectedRetriedCount - 1).toBlocking().first();
        } catch (Exception ignore) { }

        verify(expectedRetriedCount, getRequestedFor(urlEqualTo(getUrl)));
    }

    @And("^mock method getSparkJobApplicationIdObservable to return '(.+)' Observable$")
    public void mockMethodGetSparkJobApplicationIdObservable(String appIdMock) {
        when(jobMock.getSparkJobApplicationIdObservable()).thenReturn(Observable.just(appIdMock));
    }

    @And("^mock Spark job connect URI to be '(.+)'$")
    public void mockSparkJobConnectURI(String mock) throws Throwable {
        doReturn(URI.create(httpServerMock.normalizeResponse(mock))).when(jobMock).getConnectUri();
    }

    @And("^submit Spark job$")
    public void submitSparkJob() {
        caught = null;

        try {
            jobMock = (LivySparkBatch) jobMock.submit().toBlocking().singleOrDefault(null);
        } catch (Exception e) {
            caught = e;
        }
    }

    @After
    public void cleanUp(){
        this.httpServerMock.getServer().stop();
        TestLoggerFactory.clear();
    }

    @And("mock Spark job batch id to {int}")
    public void mockSparkJobBatchIdTo(int expectBatchId) {
        doReturn(expectBatchId).when(jobMock).getBatchId();
    }
}