// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;

import com.microsoft.azure.spark.tools.errors.SparkJobFinishedException;
import com.microsoft.azure.spark.tools.errors.SparkJobUploadArtifactException;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.events.SparkBatchJobSubmissionEvent;
import com.microsoft.azure.spark.tools.events.SparkBatchJobSubmittedEvent;
import com.microsoft.azure.spark.tools.job.DeployableBatch;
import com.microsoft.azure.spark.tools.job.SparkBatchJob;
import com.microsoft.azure.spark.tools.job.SparkBatchJobFactory;
import com.microsoft.azure.spark.tools.job.SparkLogFetcher;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;
import com.microsoft.azure.spark.tools.ux.ConsoleScheduler;
import com.microsoft.azure.spark.tools.ux.IdeSchedulers;
import org.apache.commons.io.output.NullOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.subjects.PublishSubject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.microsoft.azure.spark.tools.events.MessageInfoType.Info;


public class SparkBatchJobRemoteProcess extends Process implements Logger {
    private IdeSchedulers schedulers;
    private @Nullable File artifactPath;
    private final String title;
    private final Observer<Pair<MessageInfoType, String>> ctrlSubject;
    private SparkJobLogInputStream jobStdoutLogInputSteam;
    private SparkJobLogInputStream jobStderrLogInputSteam;
    private LaterInit<Subscription> jobSubscription = new LaterInit<>();
    private final SparkBatchJob sparkJob;
    private final PublishSubject<SparkBatchJobSubmissionEvent> eventSubject = PublishSubject.create();
    private boolean isDestroyed = false;

    private boolean isDisconnected;

    public SparkBatchJobRemoteProcess(final IdeSchedulers schedulers,
                                      final SparkBatchJob sparkJob,
                                      final @Nullable File artifactToUpload,
                                      final String title,
                                      final Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        this.schedulers = schedulers;
        this.sparkJob = sparkJob;
        this.artifactPath = artifactToUpload;
        this.title = title;
        this.ctrlSubject = ctrlSubject;

        this.jobStdoutLogInputSteam = new SparkJobLogInputStream("stdout");
        this.jobStderrLogInputSteam = new SparkJobLogInputStream("stderr");
    }

    /**
     * Is the Spark job session connected.
     *
     * @return is the Spark Job log getting session still connected
     */
    public boolean isDisconnected() {
        return isDisconnected;
    }

    @Override
    public OutputStream getOutputStream() {
        return new NullOutputStream();
    }

    @Override
    public InputStream getInputStream() {
        return jobStdoutLogInputSteam;
    }

    @Override
    public InputStream getErrorStream() {
        return jobStderrLogInputSteam;
    }

    @Override
    public int waitFor() {
        return 0;
    }

    @Override
    public int exitValue() {
        return 0;
    }

    @Override
    public void destroy() {
        getSparkJob().killBatchJob().subscribe(
                job -> log().trace("Killed Spark batch job " + job.getBatchId()),
                err -> log().warn("Got error when killing Spark batch job", err),
                () -> { }
        );

        this.isDestroyed = true;

        this.disconnect();
    }

    public SparkBatchJob getSparkJob() {
        return sparkJob;
    }

    public void start() {
        // Build, deploy and wait for the job done.
        jobSubscription.set(prepareArtifact()
                .flatMap(this::submitJob)
                .flatMap(this::awaitForJobStarted)
                .flatMap(this::attachInputStreams)
                .flatMap(this::awaitForJobDone)
                .subscribe(
                        sdPair -> {
                            if (sparkJob.isSuccess(sdPair.getKey())) {
                                ctrlInfo("");
                                ctrlInfo("========== RESULT ==========");
                                ctrlInfo("Job run successfully.");
                            } else {
                                ctrlInfo("");
                                ctrlInfo("========== RESULT ==========");
                                ctrlError("Job state is " + sdPair.getKey());
                                ctrlError("Diagnostics: " + sdPair.getValue());
                            }
                        },
                        err -> {
                            if (err instanceof SparkJobFinishedException
                                    || err.getCause() instanceof SparkJobFinishedException) {
                                // If we call destroy() when job is dead,
                                // we will get exception with `job is finished` error message
                                ctrlError("Job is already finished.");
                                isDestroyed = true;
                                disconnect();
                            } else {
                                ctrlError(err.getMessage());
                                destroy();
                            }
                        },
                        () -> {
                            disconnect();
                        }));
    }

    private Observable<? extends SparkBatchJob> awaitForJobStarted(final SparkBatchJob job) {
        return job.awaitStarted()
                .map(state -> job);
    }

    private Observable<? extends SparkBatchJob> attachJobInputStream(final SparkJobLogInputStream inputStream,
                                                                     final SparkBatchJob job) {
        return Observable.just(inputStream)
                .observeOn(schedulers.processBarVisibleAsync(
                        "Attach Spark batch job outputs " + inputStream.getLogType()))
                .map(stream -> {
                    if (job instanceof SparkLogFetcher) {
                        stream.attachLogFetcher((SparkLogFetcher) job);
                    }

                    return job;
                });
    }

    public void disconnect() {
        this.isDisconnected = true;

        try {
            this.jobStdoutLogInputSteam.close();
            this.jobStderrLogInputSteam.close();
        } catch (IOException ignored) {
        }

        this.ctrlSubject.onCompleted();
        this.eventSubject.onCompleted();

        if (this.jobSubscription.isInitialized()) {
            this.jobSubscription.get().unsubscribe();
        }
    }

    protected void ctrlInfo(final String message) {
        ctrlSubject.onNext(new Pair<>(Info, message));
    }

    protected void ctrlError(final String message) {
        ctrlSubject.onNext(new Pair<>(MessageInfoType.Error, message));
    }

    public PublishSubject<SparkBatchJobSubmissionEvent> getEventSubject() {
        return eventSubject;
    }

    protected Observable<SparkBatchJob> startJobSubmissionLogReceiver(final SparkBatchJob job) {
        return job.getSubmissionLog()
                .doOnNext(ctrlSubject::onNext)
                // "ctrlSubject::onNext" lead to uncaught exception
                // while "ctrlError" only print error message in console view
                .doOnError(err -> ctrlError(err.getMessage()))
                .lastOrDefault(null)
                .map((@Nullable Pair<MessageInfoType, String> messageTypeText) -> job);
    }

    // Build and deploy artifact
    protected Observable<? extends SparkBatchJob> prepareArtifact() {
        File artifactToUpload = this.artifactPath;
        SparkBatchJob batchJob = getSparkJob();

        if (artifactToUpload != null && batchJob instanceof DeployableBatch) {
            return ((DeployableBatch) batchJob)
                    .deployAndUpdateOptions(artifactToUpload)
                    .onErrorResumeNext(err -> {
                        Throwable rootCause = err.getCause() != null ? err.getCause() : err;
                        return Observable.error(new SparkJobUploadArtifactException(
                                "Failed to upload Spark application artifacts: " + rootCause.getMessage(), rootCause));
                    })
                    .map(deployedBatch -> (SparkBatchJob) deployedBatch)
                    .subscribeOn(schedulers.processBarVisibleAsync("Deploy the jar file into cluster"));
        }

        return Observable.just(getSparkJob());
    }

    protected Observable<? extends SparkBatchJob> submitJob(final SparkBatchJob batchJob) {
        return batchJob
                .submit()
                .subscribeOn(schedulers.processBarVisibleAsync("Submit the Spark batch job"))
                .flatMap(this::startJobSubmissionLogReceiver)   // To receive the Livy submission log
                .doOnNext(job -> eventSubject.onNext(new SparkBatchJobSubmittedEvent(job)));
    }

    public IdeSchedulers getSchedulers() {
        return schedulers;
    }

    public String getTitle() {
        return title;
    }

    private Observable<? extends SparkBatchJob> attachInputStreams(final SparkBatchJob job) {
        return Observable.zip(
                attachJobInputStream((SparkJobLogInputStream) getErrorStream(), job),
                attachJobInputStream((SparkJobLogInputStream) getInputStream(), job),
                (job1, job2) -> job);
    }

    Observable<Pair<String, String>> awaitForJobDone(final SparkBatchJob runningJob) {
        return runningJob.awaitDone()
                .subscribeOn(schedulers.processBarVisibleAsync("Spark batch job " + getTitle() + " is running"))
                .flatMap(jobStateDiagnosticsPair -> runningJob
                        .awaitPostDone()
                        .subscribeOn(schedulers.processBarVisibleAsync(
                                "Waiting for " + getTitle() + " log aggregation is done"))
                        .map(any -> jobStateDiagnosticsPair));
    }

    public Observer<Pair<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }

    public static SparkBatchJobRemoteProcess create(final SparkBatchJobFactory sparkBatchJobFactory) {
        return create(sparkBatchJobFactory, (File) null);
    }

    public static SparkBatchJobRemoteProcess create(final SparkBatchJobFactory sparkBatchJobFactory,
                                                    final @Nullable File artifactToUpload) {
        return create(sparkBatchJobFactory, artifactToUpload, null);
    }

    public static SparkBatchJobRemoteProcess create(final SparkBatchJobFactory sparkBatchJobFactory,
                                      final @Nullable Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        return create(sparkBatchJobFactory, null, ctrlSubject);
    }

    public static SparkBatchJobRemoteProcess create(final SparkBatchJobFactory sparkBatchJobFactory,
                                      final @Nullable File artifactToUpload,
                                      final @Nullable Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        SparkBatchJob sparkBatch = sparkBatchJobFactory.factory();
        String title = "Spark remote batch process: " + sparkBatch.getName();
        Observer<Pair<MessageInfoType, String>> subject = ctrlSubject != null
                ? ctrlSubject
                : sparkBatch.getCtrlSubject();

        return new SparkBatchJobRemoteProcess(
                new ConsoleScheduler(),
                sparkBatch,
                artifactToUpload,
                title,
                subject);
    }
}
