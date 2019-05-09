// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;

import com.microsoft.azure.spark.tools.clusters.HdiCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.job.HdiSparkBatch;
import com.microsoft.azure.spark.tools.job.PostBatchesHelper;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.LogMonitor;
import com.microsoft.azure.spark.tools.utils.MockHttpRecordingArgs;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import com.microsoft.azure.spark.tools.utils.Pair;
import com.microsoft.azure.spark.tools.ux.IdeSchedulers;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.junit.Assert.*;

@CommandLine.Command(
        description = "Record class SparkBatchJobRemoteProcess all requests and response for testing.",
        name = "SparkBatchJobRemoteProcessScenario",
        mixinStandardHelpOptions = true,
        version = "1.0")
public class SparkBatchJobRemoteProcessScenario implements Callable<Void> {
    private PostBatches postBatches;
    private MockHttpService hdiServiceMock;
    private SparkBatchJobRemoteProcess sparkJobRemoteProcess;

    @Before
    public void setUp() {
        hdiServiceMock = MockHttpService.createFromSaving(this.getClass().getName());
    }

    @After
    public void cleanUp() {
        hdiServiceMock.getServer().stop();
    }

    @Given("^create PostBatches with the following job config for SparkBatchJobRemoteProcess$")
    public void createPostBatches(Map<String, String> jobConf) {
        postBatches = PostBatchesHelper.createSubmitParams(jobConf);
    }

    @And("^submit HDInsight Spark job")
    public void submitJob() {
        sparkJobRemoteProcess = createSparkJobRemoteProcess(hdiServiceMock, postBatches);
        sparkJobRemoteProcess.start();
        assertEquals(0, sparkJobRemoteProcess.exitValue());
    }

    @Then("^check the HDInsight Spark job stdout should be")
    public void checkStdout(List<String> expect) throws Exception {
        String actual = IOUtils.toString(sparkJobRemoteProcess.getInputStream(), StandardCharsets.UTF_8);

        assertEquals("Stdout is unmatched, the captured logs:\n"
                        + StringUtils.join(LogMonitor.getAllPackagesLogs(), "\n") + "\n",
                StringUtils.join(expect, "\n") + "\n", actual);
    }

    private SparkBatchJobRemoteProcess createSparkJobRemoteProcess(MockHttpService recordingProxyService, PostBatches batchParam) {
        HdiCluster cluster = new HdiCluster() {
            @Override
            public String getYarnNMConnectionUrl() {
                return "http://localhost:" + recordingProxyService.getPort() + "yarnui/ws/v1/cluster/apps/";
            }

            @Override
            public String getYarnUIBaseUrl() {
                return "http://localhost:" + recordingProxyService.getPort() + "/yarnui/";
            }

            @Override
            public String getLivyConnectionUrl() {
                return "http://localhost:" + recordingProxyService.getPort() + "/livy/";
            }
        };

        PublishSubject<Pair<MessageInfoType, String>> ctrlSubject = PublishSubject.create();
        ctrlSubject.subscribe(
                typedMessage -> {
                    switch (typedMessage.getKey()) {
                        case Error:
                            System.err.println(typedMessage.getValue());
                            fail(typedMessage.getValue());
                            break;
                        case Info:
                        case Log:
                        case Warning:
                            System.out.println(typedMessage.getValue());
                            break;
                        case Hyperlink:
                        case HyperlinkWithText:
                            break;
                    }
                },
                err -> {
                    System.err.println("Got error: " + err);
                    throw new RuntimeException(err);
                }
        );

        SparkBatchSubmission submission = SparkBatchSubmission.getInstance();
        if (recordingArgs.getUsername() != null) {
            submission.setUsernamePasswordCredential(recordingArgs.getUsername(), recordingArgs.getPassword());
        }

        IdeSchedulers cliScheduler = new IdeSchedulers() {
            @Override
            public Scheduler processBarVisibleAsync(String title) {
                return Schedulers.trampoline();
            }

            @Override
            public Scheduler processBarVisibleSync(String title) {
                return null;
            }

            @Override
            public Scheduler dispatchUIThread() {
                return null;
            }
        };

        HdiSparkBatch job = new HdiSparkBatch(cluster, batchParam, submission, ctrlSubject);
        return new SparkBatchJobRemoteProcess(
                cliScheduler, job, "/", "Submit Spark Application", ctrlSubject);
    }

    // Main function for recording mode
    @Mixin
    private MockHttpRecordingArgs recordingArgs = new MockHttpRecordingArgs();

    @Option(names = "--mainClassName", description = "Spark job main class name")
    private String mainClassName;

    @Option(names = "--artifactUri", description = "Spark job artifact URI")
    private URI artifactUri;

    @Option(names = "--log", description = "Output the logs captured")
    private boolean doesPrintLog;

    private PostBatches createSubmitParamFromArgs() {
        PostBatches.Options batchParamOptions = new PostBatches.Options()
                .className(mainClassName)
                .artifactUri(artifactUri.toString());

        return batchParamOptions.build();
    }

    public static void main(String[] args) throws IOException {
        SparkBatchJobRemoteProcessScenario scenario = new SparkBatchJobRemoteProcessScenario();
        CommandLine.call(scenario, args);

        assertNotNull(scenario.recordingArgs.getTargetUrl());
        assertNotNull(scenario.mainClassName);
        assertNotNull(scenario.artifactUri);

        MockHttpService recordingProxyService = MockHttpService.createForRecord(
                SparkBatchJobRemoteProcessScenario.class.getName(), scenario.recordingArgs.getTargetUrl().toString());
        SparkBatchJobRemoteProcess sparkJobRemoteProcess = scenario.createSparkJobRemoteProcess(
                recordingProxyService, scenario.createSubmitParamFromArgs());
        sparkJobRemoteProcess.start();
        System.out.println("========= stdout =========");
        String stdout = IOUtils.toString(sparkJobRemoteProcess.getInputStream(), StandardCharsets.UTF_8);
        System.out.print(stdout);

        System.out.println("========= stderr =========");
        String stderr = IOUtils.toString(sparkJobRemoteProcess.getErrorStream(), StandardCharsets.UTF_8);
        System.out.print(stderr);

        recordingProxyService.getServer().stopRecording();
        recordingProxyService.getServer().stop();

        if (scenario.doesPrintLog) {
            System.out.println("========= log4j =========");
            System.out.println(StringUtils.join(LogMonitor.getAllPackagesLogs(), "\n"));
        }
    }

    @Override
    public Void call() throws Exception {
        return null;
    }
}
