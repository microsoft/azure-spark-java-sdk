// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;

import com.google.common.net.HostAndPort;
import com.microsoft.azure.spark.tools.errors.SparkJobFinishedException;
import com.microsoft.azure.spark.tools.errors.SparkJobUploadArtifactException;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.events.SparkBatchJobSubmissionEvent;
import com.microsoft.azure.spark.tools.events.SparkBatchJobSubmittedEvent;
import com.microsoft.azure.spark.tools.job.SparkBatchJob;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.ux.IdeSchedulers;
import org.apache.commons.io.output.NullOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Optional;

import static com.microsoft.azure.spark.tools.events.MessageInfoType.Info;


public class SparkBatchJobRemoteProcess extends Process implements Logger {
    private IdeSchedulers schedulers;
    private String artifactPath;
    private final String title;
    private final PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject;
    private SparkJobLogInputStream jobStdoutLogInputSteam;
    private SparkJobLogInputStream jobStderrLogInputSteam;
    @Nullable
    private Subscription jobSubscription;
    private final SparkBatchJob sparkJob;
    private final PublishSubject<SparkBatchJobSubmissionEvent> eventSubject = PublishSubject.create();
    private boolean isDestroyed = false;

    private boolean isDisconnected;

    public SparkBatchJobRemoteProcess(IdeSchedulers schedulers,
                                      SparkBatchJob sparkJob,
                                      String artifactPath,
                                      String title,
                                      PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> ctrlSubject) {
        this.schedulers = schedulers;
        this.sparkJob = sparkJob;
        this.artifactPath = artifactPath;
        this.title = title;
        this.ctrlSubject = ctrlSubject;

        this.jobStdoutLogInputSteam = new SparkJobLogInputStream("stdout");
        this.jobStderrLogInputSteam = new SparkJobLogInputStream("stderr");
    }

    /**
     * To Kill the remote job.
     *
     * @return is the remote Spark Job killed
     */
    public boolean killProcessTree() {
        return false;
    }

    /**
     * Is the Spark job session connected.
     *
     * @return is the Spark Job log getting session still connected
     */
    public boolean isDisconnected() {
        return isDisconnected;
    }

    @Nullable
    public HostAndPort getLocalTunnel(int i) {
        return null;
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

    public Optional<Subscription> getJobSubscription() {
        return Optional.ofNullable(jobSubscription);
    }

    public void start() {
        // Build, deploy and wait for the job done.
        // jobSubscription = prepareArtifact()
        jobSubscription = Observable.just(sparkJob)
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
                        });
    }

    private Observable<? extends SparkBatchJob> awaitForJobStarted(SparkBatchJob job) {
        return job.awaitStarted()
                .map(state -> job);
    }

    private Observable<? extends SparkBatchJob> attachJobInputStream(SparkJobLogInputStream inputStream,
                                                                     SparkBatchJob job) {
        return Observable.just(inputStream)
                .map(stream -> stream.attachJob(job))
                .subscribeOn(schedulers.processBarVisibleAsync(
                        "Attach Spark batch job outputs " + inputStream.getLogType()));
    }

    public void disconnect() {
        this.isDisconnected = true;

        this.ctrlSubject.onCompleted();
        this.eventSubject.onCompleted();

        this.getJobSubscription().ifPresent(Subscription::unsubscribe);
    }

    protected void ctrlInfo(String message) {
        ctrlSubject.onNext(new SimpleImmutableEntry<>(Info, message));
    }

    protected void ctrlError(String message) {
        ctrlSubject.onNext(new SimpleImmutableEntry<>(MessageInfoType.Error, message));
    }

    public PublishSubject<SparkBatchJobSubmissionEvent> getEventSubject() {
        return eventSubject;
    }

    protected Observable<SparkBatchJob> startJobSubmissionLogReceiver(SparkBatchJob job) {
        return job.getSubmissionLog()
                .doOnNext(ctrlSubject::onNext)
                // "ctrlSubject::onNext" lead to uncaught exception
                // while "ctrlError" only print error message in console view
                .doOnError(err -> ctrlError(err.getMessage()))
                .lastOrDefault(null)
                .map((@Nullable SimpleImmutableEntry<MessageInfoType, String> messageTypeText) -> job);
    }

    // Build and deploy artifact
    protected Observable<? extends SparkBatchJob> prepareArtifact() {
        return getSparkJob()
                .deploy(artifactPath)
                .onErrorResumeNext(err -> {
                    Throwable rootCause = err.getCause() != null ? err.getCause() : err;
                    return Observable.error(new SparkJobUploadArtifactException(
                            "Failed to upload Spark application artifacts: " + rootCause.getMessage(), rootCause));
                })
                .subscribeOn(schedulers.processBarVisibleAsync("Deploy the jar file into cluster"));
    }

    protected Observable<? extends SparkBatchJob> submitJob(SparkBatchJob batchJob) {
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

    private Observable<? extends SparkBatchJob> attachInputStreams(SparkBatchJob job) {
        return Observable.zip(
                attachJobInputStream((SparkJobLogInputStream) getErrorStream(), job),
                attachJobInputStream((SparkJobLogInputStream) getInputStream(), job),
                (job1, job2) -> job);
    }

    Observable<SimpleImmutableEntry<String, String>> awaitForJobDone(SparkBatchJob runningJob) {
        return runningJob.awaitDone()
                .subscribeOn(schedulers.processBarVisibleAsync("Spark batch job " + getTitle() + " is running"))
                .flatMap(jobStateDiagnosticsPair -> runningJob
                        .awaitPostDone()
                        .subscribeOn(schedulers.processBarVisibleAsync(
                                "Waiting for " + getTitle() + " log aggregation is done"))
                        .map(any -> jobStateDiagnosticsPair));
    }

    public PublishSubject<SimpleImmutableEntry<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }
}
