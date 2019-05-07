// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;

import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.job.LivySparkBatch;
import com.microsoft.azure.spark.tools.job.PostBatchesHelper;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.MockHttpRecordingArgs;
import com.microsoft.azure.spark.tools.utils.MockHttpService;
import com.microsoft.azure.spark.tools.utils.Pair;
import com.microsoft.azure.spark.tools.ux.IdeSchedulers;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.net.URI;
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
        SparkBatchJobRemoteProcess sparkJobRemoteProcess = createSparkJobRemoteProcess(hdiServiceMock, postBatches);
        sparkJobRemoteProcess.start();
        assertEquals(0, sparkJobRemoteProcess.exitValue());
    }

    private SparkBatchJobRemoteProcess createSparkJobRemoteProcess(MockHttpService recordingProxyService, PostBatches batchParam) {
        LivyCluster cluster = new LivyCluster() {
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

        LivySparkBatch job = new LivySparkBatch(cluster, batchParam, submission, ctrlSubject);
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

    private PostBatches createSubmitParamFromArgs() {
        PostBatches.Options batchParamOptions = new PostBatches.Options()
                .className(mainClassName)
                .artifactUri(artifactUri.toString());

        return batchParamOptions.build();
    }

    public static void main(String[] args) {
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

        recordingProxyService.getServer().stopRecording();
        recordingProxyService.getServer().stop();
    }

    @Override
    public Void call() throws Exception {
        return null;
    }
}
