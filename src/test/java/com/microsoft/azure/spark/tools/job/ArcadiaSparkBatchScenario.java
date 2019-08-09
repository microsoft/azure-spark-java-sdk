// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.github.tomakehurst.wiremock.extension.Parameters;
import com.nimbusds.jose.util.Base64;
import cucumber.api.Scenario;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.mockito.Mockito;
import picocli.CommandLine;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;
import wiremock.com.google.common.collect.ImmutableMap;

import com.microsoft.azure.spark.tools.clusters.ArcadiaCompute;
import com.microsoft.azure.spark.tools.http.AuthorizationHeaderTransformer;
import com.microsoft.azure.spark.tools.http.AzureHttpObservable;
import com.microsoft.azure.spark.tools.http.AzureOAuthTokenFetcher;
import com.microsoft.azure.spark.tools.http.OAuthTokenHttpObservable;
import com.microsoft.azure.spark.tools.http.SparkConfBodyTransformer;
import com.microsoft.azure.spark.tools.log.Slf4jTestLogApacheAdapter;
import com.microsoft.azure.spark.tools.processes.SparkBatchJobRemoteProcess;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches.Options;
import com.microsoft.azure.spark.tools.utils.LogMonitor;
import com.microsoft.azure.spark.tools.utils.MockHttpRecordingArgs;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import com.microsoft.azure.spark.tools.utils.WasbUri;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.microsoft.azure.spark.tools.utils.LogMonitor.cleanUpSparkToolsLogs;
import static com.microsoft.azure.spark.tools.utils.LogMonitor.getSparkToolsLogsStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@CommandLine.Command(
        description = "Record class ArcadiaSparkBatch with SparkBatchJobRemoteProcess all requests and response for testing.",
        name = "ArcadiaSparkBatchScenario",
        mixinStandardHelpOptions = true,
        version = "1.0")
public class ArcadiaSparkBatchScenario implements Callable<Void> {
    private MockHttpService arcadiaServiceMock;
    private SparkBatchJobRemoteProcess sparkJobRemoteProcess;
    private Options sparkParameterOptions = new Options();
    private OAuthTokenHttpObservable oauthHttp;

    @Before
    public void setUpGlobal() {
        System.setProperty("org.apache.commons.logging.Log", Slf4jTestLogApacheAdapter.class.getCanonicalName());
    }

    @Before("@ArcadiaSparkBatchScenario")
    public void setUp() {
        arcadiaServiceMock = MockHttpService.createFromSaving(this.getClass().getName());
    }

    @After("@ArcadiaSparkBatchScenario")
    public void cleanUp(Scenario scenario) {
        if (scenario.isFailed()) {
            scenario.write("Failure Apache common logs:");
            // Print out Apache logs for failure
            TestLoggerFactory.getInstance().getAllLoggingEventsFromLoggers().stream()
                    .filter(event -> event.getCreatingLogger().getName().startsWith("org.apache"))
                    .sorted(Comparator.comparing(LoggingEvent::getTimestamp))
                    .map(event -> String.format("%s %s %-10s (%s) -- %s",
                            event.getTimestamp().toString(),
                            event.getLevel(),
                            event.getCreatingLogger().getName(),
                            event.getThreadName(),
                            event.getMessage()))
                    .forEach(scenario::write);
        }

        arcadiaServiceMock.stop();
    }

    @Given("create PostBatches with the following job config for ArcadiaBatch")
    public void createPostBatchesWithTheFollowingJobConfigForArcadiaBatch(Map<String, String> config) {
        config.forEach( (k, v) -> {
            switch (k) {
                case "className":
                    sparkParameterOptions.className(v);
                    break;
                case "name":
                    sparkParameterOptions.name(v);
                    break;
                case "file":
                    sparkParameterOptions.artifactUri(v);
                    break;
                default:
                    sparkParameterOptions.conf(k, v);
            }
        });
    }

    @And("mock the Arcadia HTTP OAuth token to {string}")
    public void mockOauthHttp(String mockOauthToken) {
        this.oauthHttp = new OAuthTokenHttpObservable(Base64.encode(mockOauthToken).toString());
    }

    @And("mock the Arcadia workspace to {string}")
    public void mockWorkspace(String mockWorkspace) {
        this.workspace = mockWorkspace;
    }

    @And("submit Arcadia Spark job")
    public void submitArcadiaSparkJob() {
        cleanUpSparkToolsLogs();

        sparkJobRemoteProcess = createSparkJobRemoteProcess(arcadiaServiceMock, sparkParameterOptions);
        sparkJobRemoteProcess.start();
        assertEquals(0, sparkJobRemoteProcess.exitValue());
    }

    @Then("no any error after submitting Arcadia Spark job")
    public void noAnyErrorAfterSubmittingArcadiaSparkJob() {
        getSparkToolsLogsStream().map(LoggingEvent::getMessage).forEach(logEntry -> {
            if (StringUtils.containsIgnoreCase(logEntry, "ERR")
                    || StringUtils.containsIgnoreCase(logEntry, "Exception")) {
                fail(logEntry);
            }
        });
    }

    @CommandLine.Mixin
    private MockHttpRecordingArgs recordingArgs = new MockHttpRecordingArgs();

    @CommandLine.Option(names = "--mainclass-name", description = "Spark job main class name")
    private String mainClassName;

    @CommandLine.Option(names = "--artifact-uri", description = "Spark job artifact URI")
    private URI artifactUri;

    @CommandLine.Option(names = "--compute", description = "Arcadia Spark compute")
    private String compute;

    @CommandLine.Option(names = "--workspace", description = "Arcadia workspace")
    private String workspace;

    @CommandLine.Option(names = "--storage-key", description = "Artifact Blob Storage Key")
    private String storageKey;

    @CommandLine.Option(names = "--resource", description = "Artifact ARM resource")
    private String resource;

    @CommandLine.Option(names = "--log", description = "Output the logs captured")
    private boolean doesPrintLog;

    @CommandLine.Option(names = "--dry-run", description = "Dry run without saving response")
    private boolean isDryRun;

    private SparkBatchJobRemoteProcess createSparkJobRemoteProcess(MockHttpService recordingProxyService, Options options) {
        ArcadiaCompute cluster = new ArcadiaCompute() {
            @Override
            public String getWorkspace() {
                return workspace;
            }

            @Override
            public String getLivyConnectionUrl() {
                return "http://localhost:" + recordingProxyService.getPort() + "/";
            }
        };

        OAuthTokenHttpObservable http = this.oauthHttp != null
                ? this.oauthHttp
                : new AzureHttpObservable(AzureOAuthTokenFetcher.buildFromAzureCli(resource));

        Deployable deployable = Mockito.mock(Deployable.class);

        return SparkBatchJobRemoteProcess.create(new ArcadiaSparkBatchFactory(cluster, options, http, deployable));
    }

    private Options createSubmitParamFromArgs() {
        WasbUri wasbUri = WasbUri.parse(artifactUri.toString());

        Options batchParamOptions = new Options()
                .name(mainClassName)
                .setYarnNumExecutors(PostBatches.NUM_EXECUTORS_DEFAULT_VALUE)
                .setExecutorCores(PostBatches.EXECUTOR_CORES_DEFAULT_VALUE)
                .setExecutorMemory(PostBatches.EXECUTOR_MEMORY_DEFAULT_VALUE)
                .setDriverCores(PostBatches.DRIVER_CORES_DEFAULT_VALUE)
                .setDriverMemory(PostBatches.DRIVER_MEMORY_DEFAULT_VALUE)
                .className(mainClassName)
                .conf("spark.hadoop.fs.azure.account.key." + wasbUri.getStorageAccount() + ".blob.core.windows.net",
                      storageKey)
                .artifactUri(artifactUri.toString());

        return batchParamOptions;
    }

    // Main function for recording mode
    public static void main(String[] args) throws IOException {
        ArcadiaSparkBatchScenario scenario = new ArcadiaSparkBatchScenario();
        CommandLine.call(scenario, args);

        assertNotNull(scenario.recordingArgs.getTargetUrl());
        assertNotNull(scenario.mainClassName);
        assertNotNull(scenario.artifactUri);

        // Connect Apache Loggers
        System.setProperty("org.apache.commons.logging.Log", Slf4jTestLogApacheAdapter.class.getCanonicalName());

        WasbUri wasbUri = WasbUri.parse(scenario.artifactUri.toString());
        String targetUrl = scenario.recordingArgs.getTargetUrl().toString();
        MockHttpService recordingProxyService = MockHttpService.createForRecord(
                ArcadiaSparkBatchScenario.class.getName(),
                recordSpec()
                        .forTarget(targetUrl)
                        .captureHeader("Accept")
                        .captureHeader("Content-Type", true)
                        .captureHeader("X-Requested-By")
                        .captureHeader(HttpHeaders.AUTHORIZATION)
                        .captureHeader("x-ms-workspace-name")
                        .transformers(AuthorizationHeaderTransformer.NAME, SparkConfBodyTransformer.NAME)
                        .transformerParameters(Parameters.from(ImmutableMap.of(
                                AuthorizationHeaderTransformer.param(HttpHeaders.AUTHORIZATION), "Bearer " + Base64.encode("masked_oauth_token"),
                                SparkConfBodyTransformer.param("spark.hadoop." + wasbUri.getHadoopBlobFsPropertyKey()), "masked_blob_access_token")))
                        .makeStubsPersistent(!scenario.isDryRun)
                        .build()
                );
        SparkBatchJobRemoteProcess sparkJobRemoteProcess = scenario.createSparkJobRemoteProcess(
                recordingProxyService, scenario.createSubmitParamFromArgs());
        sparkJobRemoteProcess.start();
        String stdout = IOUtils.toString(sparkJobRemoteProcess.getInputStream(), StandardCharsets.UTF_8);
        System.out.println("========= stdout =========");
        System.out.print(stdout);

        System.out.println("========= stderr =========");
        String stderr = IOUtils.toString(sparkJobRemoteProcess.getErrorStream(), StandardCharsets.UTF_8);
        System.out.print(stderr);

        recordingProxyService.getServer().stopRecording();
        recordingProxyService.getServer().stop();

        if (scenario.doesPrintLog) {
            System.out.println("========= log4j =========");
            System.out.println(StringUtils.join(LogMonitor.getSparkToolsLogs(), "\n"));
        }

        // Print out error logs
        LogMonitor.getSparkToolsLogs().forEach(logEntry -> {
            if (StringUtils.containsIgnoreCase(logEntry, "ERR")
                    || StringUtils.containsIgnoreCase(logEntry, "Exception")) {
                System.err.println(logEntry);
            }
        });
    }

    @Override
    public Void call() throws Exception {
        return null;
    }
}
