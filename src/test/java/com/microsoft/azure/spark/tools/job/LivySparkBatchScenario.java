// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.commons.lang3.StringEscapeUtils;
import rx.subjects.PublishSubject;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.AmbariHttpObservable;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import com.microsoft.azure.spark.tools.utils.Pair;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LivySparkBatchScenario {
    private HttpObservable httpMock;
    private LaterInit<Integer> batchIdMock;
    private Throwable caught;
    private MockHttpService httpServerMock;
    private LivySparkBatch jobMock;
    private TestLogger logger = TestLoggerFactory.getTestLogger(LivySparkBatchScenario.class);
    private Map<LivySparkBatch.LivyLogType, List<String>> parsedLivyLogs = Collections.emptyMap();
    private PublishSubject<Pair<MessageInfoType, String>> mockCtrlSubject = PublishSubject.create();

    @Before("@LivySparkBatchScenario")
    public void setUp() throws Throwable {
        httpMock = new AmbariHttpObservable();
        batchIdMock = new LaterInit<>();

        jobMock = mock(LivySparkBatch.class, CALLS_REAL_METHODS);
        when(jobMock.getHttp()).thenReturn(httpMock);
        when(jobMock.getLaterBatchId()).thenReturn(batchIdMock);
        when(jobMock.getCtrlSubject()).thenReturn(mockCtrlSubject);

        caught = null;

        this.httpServerMock = MockHttpService.create();
    }

    @After("@LivySparkBatchScenario")
    public void cleanUp(){
        this.httpServerMock.stop();
        TestLoggerFactory.clear();
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
        LaterInit<String> mockAppId = new LaterInit<>();
        when(jobMock.getLaterAppId()).thenReturn(mockAppId);
        try {
            assertEquals(
                    expectedApplicationId,
                    jobMock.get().map(LivySparkBatch::getSparkJobApplicationId).toBlocking().single());
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
            jobMock.get().retry(expectedRetriedCount - 1).toBlocking().first();
        } catch (Exception ignore) { }

        verify(expectedRetriedCount, getRequestedFor(urlEqualTo(getUrl)));
    }

    @And("^mock method getSparkJobApplicationId to return '(.+)' Observable$")
    public void mockMethodGetSparkJobApplicationIdObservable(String appIdMock) {
        when(jobMock.getSparkJobApplicationId()).thenReturn(appIdMock);
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

    @And("mock Spark job batch id to {int}")
    public void mockSparkJobBatchIdTo(int expectBatchId) {
        doReturn(expectBatchId).when(jobMock).getBatchId();
    }

    @Given("setup a mock Livy service with the following scenario {string}")
    public void setupAMockLivyServiceWithTheFollowingScenarioAwaitJobIsDoneUT(String scenario,
                                                                              List<Map<String, String>> stubs) {
        for (Map<String, String> stub : stubs) {
            httpServerMock.stub(
                    scenario,
                    stub.get("PREV_STATE"),
                    stub.get("NEXT_STATE"),
                    stub.get("ACTION"),
                    stub.get("URI"),
                    Integer.parseInt(stub.get("RESPONSE_STATUS")),
                    stub.get("RESPONSE_BODY"));
        }
    }

    @Then("await Livy Spark job done should get state {string}")
    public void awaitLivySparkJobDoneShouldGetStateSuccess(String expect) {
        Pair<String, String> statesWithLogs = jobMock.awaitDone()
                .toBlocking()
                .single();

        assertEquals(expect, statesWithLogs.getFirst());
    }

    @Given("parse Livy Logs from the following")
    public void parseLivyLogsFromTheFollowing(List<String> mockLivyLogs) {
        this.parsedLivyLogs = this.jobMock.parseLivyLogs(LivySparkBatch.LivyLogType.STDOUT,
                mockLivyLogs.stream()
                        .map(StringEscapeUtils::unescapeJava)
                        .collect(Collectors.toList()));
    }

    private void assertLogsMatchedByType(LivySparkBatch.LivyLogType logType, List<String> expectLogsToUnescape) {
        assertThat(this.parsedLivyLogs.get(logType))
                .containsExactlyElementsOf(expectLogsToUnescape.stream()
                        .map(StringEscapeUtils::unescapeJava)
                        .collect(Collectors.toList()));
    }

    @Then("check parsed Livy logs stdout should be")
    public void checkParsedLivyLogsStdoutShouldBe(List<String> expectLogs) {
        assertLogsMatchedByType(LivySparkBatch.LivyLogType.STDOUT, expectLogs);
    }

    @Then("check parsed Livy logs stderr should be")
    public void checkParsedLivyLogsStderrShouldBe(List<String> expectLogs) {
        assertLogsMatchedByType(LivySparkBatch.LivyLogType.STDERR, expectLogs);
    }

    @Then("check parsed Livy logs yarn diagnostics should be")
    public void checkParsedLivyLogsYarnDiagnosticsShouldBe(List<String> expectLogs) {
        assertLogsMatchedByType(LivySparkBatch.LivyLogType.YARN_DIAGNOSTICS, expectLogs);
    }

    @Then("check the spark job request {string} to {string} should include headers")
    public void checkTheSparkJobRequestGETToBatchShouldIncludeHeaders(String method, String targetUrl, Map<String, String> expectHeaders) {
        RequestPatternBuilder requestPatternBuilder = new RequestPatternBuilder(
                RequestMethod.fromString(method.toUpperCase()), urlEqualTo(targetUrl));

        for (Map.Entry<String, String> header : expectHeaders.entrySet()) {
            requestPatternBuilder = requestPatternBuilder.withHeader(header.getKey(), equalTo(header.getValue()));
        }

        verify(requestPatternBuilder);
    }

    @Then("await Livy Spark job is started should get state {string}")
    public void awaitLivySparkJobIsStartedShouldGetStateRunning(String expectedState) {
        String actualState = jobMock.awaitStarted()
                .toBlocking()
                .single();

        assertEquals(expectedState, actualState);
    }

    @Then("await Livy Spark job is started should get Exception {string} with {string}")
    public void awaitLivySparkJobIsStartedShouldGetException(String expectExpType, String expectExpMessage) {
        caught = null;

        try {
            jobMock.awaitStarted()
                    .toBlocking()
                    .single();

            fail("Should get exceptions.");
        } catch (Exception got) {
            Throwable cause = got.getCause();
            assertEquals(expectExpType, cause.getClass().getName());
            assertEquals(StringEscapeUtils.unescapeJava(expectExpMessage), cause.getMessage());
        }
    }
}