// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.errors.SparkJobException;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.legacyhttp.HttpResponse;
import com.microsoft.azure.spark.tools.legacyhttp.ObjectConvertUtils;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.livy.batches.BatchState;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatchesResponse;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.batch.GetLogResponse;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownServiceException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.microsoft.azure.spark.tools.events.MessageInfoType.Info;
import static com.microsoft.azure.spark.tools.events.MessageInfoType.Log;
import static java.lang.Thread.sleep;

//@SuppressWarnings("argument.type.incompatible")
public class LivySparkBatch implements SparkBatchJob, Logger {
    public static final String WebHDFSPathPattern = "^(https?://)([^/]+)(/.*)?(/webhdfs/v1)(/.*)?$";
    public static final String AdlsPathPattern = "^adl://([^/.\\s]+\\.)+[^/.\\s]+(/[^/.\\s]+)*/?$";

    private final Observer<Pair<MessageInfoType, String>> ctrlSubject;

    /**
     * The LIVY Spark batch job ID got from job submission.
     */
    private LaterInit<Integer> batchId = new LaterInit<>();

    /**
     * The Spark Batch Job submission parameter.
     */
    protected PostBatches submissionParameter;

    /**
     * The Spark Batch Job submission for RestAPI transaction.
     */
    private SparkBatchSubmission submission;

    /**
     * The setting of maximum retry count in RestAPI calling.
     */
    private int retriesMax = 3;

    /**
     * The setting of delay seconds between tries in RestAPI calling.
     */
    private int delaySeconds = 10;

    private final LivyCluster cluster;

    @Nullable
    private String destinationRootPath;

    public LivySparkBatch(
            final LivyCluster cluster,
            final PostBatches submissionParameter,
            final SparkBatchSubmission sparkBatchSubmission,
            final Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        this(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject, null);
    }


    public LivySparkBatch(
            final LivyCluster cluster,
            final PostBatches submissionParameter,
            final SparkBatchSubmission sparkBatchSubmission,
            final Observer<Pair<MessageInfoType, String>> ctrlSubject,
            @Nullable final String destinationRootPath) {
        this.cluster = cluster;
        this.submissionParameter = submissionParameter;
        this.submission = sparkBatchSubmission;
        this.ctrlSubject = ctrlSubject;
        this.destinationRootPath = destinationRootPath;
    }

    /**
     * Getter of Spark Batch Job submission parameter.
     *
     * @return the instance of Spark Batch Job submission parameter
     */
    public PostBatches getSubmissionParameter() {
        return submissionParameter;
    }

    /**
     * Getter of the Spark Batch Job submission for RestAPI transaction.
     *
     * @return the Spark Batch Job submission
     */
    public SparkBatchSubmission getSubmission() {
        return submission;
    }

    /**
     * Getter of the base connection URI for HDInsight Spark Job service.
     *
     * @return the base connection URI for HDInsight Spark Job service
     */
    @Override
    public URI getConnectUri() {
        return URI.create(getCluster().getLivyBatchUrl());
    }

    public LivyCluster getCluster() {
        return cluster;
    }

    /**
     * Getter of the LIVY Spark batch job ID got from job submission.
     *
     * @return the LIVY Spark batch job ID
     */
    @Override
    public int getBatchId() {
        return batchId.get();
    }

    /**
     * Getter of the maximum retry count in RestAPI calling.
     *
     * @return the maximum retry count in RestAPI calling
     */
    @Override
    public int getRetriesMax() {
        return retriesMax;
    }

    /**
     * Setter of the maximum retry count in RestAPI calling.
     *
     * @param retriesMax the maximum retry count in RestAPI calling
     */
    @Override
    public void setRetriesMax(final int retriesMax) {
        this.retriesMax = retriesMax;
    }

    /**
     * Getter of the delay seconds between tries in RestAPI calling.
     *
     * @return the delay seconds between tries in RestAPI calling
     */
    @Override
    public int getDelaySeconds() {
        return delaySeconds;
    }

    /**
     * Setter of the delay seconds between tries in RestAPI calling.
     *
     * @param delaySeconds the delay seconds between tries in RestAPI calling
     */
    @Override
    public void setDelaySeconds(final int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    /**
     * Create a batch Spark job.
     *
     * @return the current instance for chain calling
     * @throws IOException the exceptions for networking connection issues related
     */
    private LivySparkBatch createBatchJob()
            throws IOException {
        // Submit the batch job
        HttpResponse httpResponse = this.getSubmission().createBatchSparkJob(
                getConnectUri().toString(), this.getSubmissionParameter());

        // Get the batch ID from response and save it
        if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
            PostBatchesResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                    httpResponse.getMessage(), PostBatchesResponse.class)
                    .orElseThrow(() -> new UnknownServiceException(
                            "Bad spark job response: " + httpResponse.getMessage()));

            this.batchId.set(jobResp.getId());

            return this;
        }

        throw new UnknownServiceException(String.format(
                "Failed to submit Spark batch job. error code: %d, type: %s, reason: %s.",
                httpResponse.getCode(), httpResponse.getContent(), httpResponse.getMessage()));
    }

    /**
     * Kill the batch job specified by ID.
     *
     * @return the current instance for chain calling
     */
    @Override
    public Observable<? extends SparkBatchJob> killBatchJob() {
        return Observable.fromCallable(() -> {
            HttpResponse deleteResponse = this.getSubmission().killBatchJob(
                    this.getConnectUri().toString(), this.getBatchId());

            if (deleteResponse.getCode() > 300) {
                throw new UnknownServiceException(String.format(
                        "Failed to stop spark job. error code: %d, reason: %s.",
                        deleteResponse.getCode(), deleteResponse.getContent()));
            }

            return this;
        });
    }

    /**
     * Get Spark Job Yarn application state with retries.
     *
     * @return the Yarn application state got
     * @throws IOException exceptions in transaction
     */
    public String getState() throws IOException {
        int retries = 0;

        do {
            try {
                HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                        this.getConnectUri().toString(), getBatchId());

                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    PostBatchesResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), PostBatchesResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    return jobResp.getState();
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }

            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException(
                "Failed to get job state: Unknown service error after " + --retries + " retries");
    }

    /**
     * New RxAPI: Get current job application Id.
     *
     * @return Application Id Observable
     */
    Observable<String> getSparkJobApplicationIdObservable() {
        return Observable.fromCallable(() -> {
            HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                    getConnectUri().toString(), getBatchId());

            if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                PostBatchesResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                        httpResponse.getMessage(), PostBatchesResponse.class)
                        .orElseThrow(() -> new UnknownServiceException(
                                "Bad spark job response: " + httpResponse.getMessage()));

                return jobResp.getAppId();
            }

            throw new UnknownServiceException("Can't get Spark Application Id");
        });
    }

    @Override
    public Observable<Pair<MessageInfoType, String>> getSubmissionLog() {
        // Those lines are carried per response,
        // if there is no value followed, the line should not be sent to console
        final Set<String> ignoredEmptyLines = new HashSet<>(Arrays.asList(
                "stdout:",
                "stderr:",
                "yarn diagnostics:"));

        return Observable.create(ob -> {
            try {
                int start = 0;
                final int maxLinesPerGet = 128;
                int linesGot;
                boolean isSubmitting = true;

                while (isSubmitting) {
                    String status = this.getState();
                    boolean isAppIdAllocated = !this.getSparkJobApplicationIdObservable().isEmpty()
                            .toBlocking()
                            .lastOrDefault(true);

                    String logUrl = String.format("%s/%d/log?from=%d&size=%d",
                            this.getConnectUri().toString(), getBatchId(), start, maxLinesPerGet);

                    HttpResponse httpResponse = this.getSubmission().getHttpResponseViaGet(logUrl);

                    log().debug("Status: " + status
                            + ", Is Application ID allocated: " + isAppIdAllocated
                            + ", Request to " + logUrl
                            + ", got " + httpResponse.getMessage());

                    GetLogResponse getLogResponse = ObjectConvertUtils.convertJsonToObject(httpResponse.getMessage(),
                            GetLogResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark log response: " + httpResponse.getMessage()));

                    // To subscriber
                    getLogResponse.getLog().stream()
                            .filter(line -> !ignoredEmptyLines.contains(line.trim().toLowerCase()))
                            .forEach(line -> ob.onNext(new Pair<>(Log, line)));

                    linesGot = getLogResponse.getLog().size();
                    start += linesGot;

                    // Retry interval
                    if (linesGot == 0) {
                        isSubmitting = StringUtils.equalsIgnoreCase(status, "starting") || !isAppIdAllocated;

                        sleep(200);
                    }
                }
            } catch (IOException ex) {
                ob.onNext(new Pair<>(MessageInfoType.Error, ex.getMessage()));
            } catch (InterruptedException ignored) {
            } finally {
                ob.onCompleted();
            }
        });
    }

    public boolean isActive() throws IOException {
        int retries = 0;

        do {
            try {
                HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                        this.getConnectUri().toString(), getBatchId());

                if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                    PostBatchesResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                            httpResponse.getMessage(), PostBatchesResponse.class)
                            .orElseThrow(() -> new UnknownServiceException(
                                    "Bad spark job response: " + httpResponse.getMessage()));

                    return jobResp.isAlive();
                }
            } catch (IOException e) {
                log().debug("Got exception " + e.toString() + ", waiting for a while to try", e);
            }

            try {
                // Retry interval
                sleep(TimeUnit.SECONDS.toMillis(this.getDelaySeconds()));
            } catch (InterruptedException ex) {
                throw new IOException("Interrupted in retry attempting", ex);
            }
        } while (++retries < this.getRetriesMax());

        throw new UnknownServiceException(
                "Failed to detect job activity: Unknown service error after " + --retries + " retries");
    }

    protected Observable<Pair<String, String>> getJobDoneObservable() {
        return Observable.create((Subscriber<? super Pair<String, String>> ob) -> {
            try {
                boolean isJobActive;
                BatchState state = BatchState.NOT_STARTED;
                String diagnostics = "";

                do {
                    HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                            this.getConnectUri().toString(), getBatchId());

                    if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                        PostBatchesResponse jobResp = ObjectConvertUtils.convertJsonToObject(
                                httpResponse.getMessage(), PostBatchesResponse.class)
                                .orElseThrow(() -> new UnknownServiceException(
                                        "Bad spark job response: " + httpResponse.getMessage()));

                        state = BatchState.valueOf(jobResp.getState().toUpperCase());
                        diagnostics = String.join("\n", jobResp.getLog());

                        isJobActive = !isDone(state.toString());
                    } else {
                        isJobActive = false;
                    }


                    // Retry interval
                    sleep(1000);
                } while (isJobActive);

                ob.onNext(new Pair<>(state.toString(), diagnostics));
                ob.onCompleted();
            } catch (IOException ex) {
                ob.onError(ex);
            } catch (InterruptedException ignored) {
                ob.onCompleted();
            }
        });
    }

    @Override
    public Observer<Pair<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
    }

    /**
     * The method is to deploy artifact to cluster (Not supported for Livy Spark Batch job).
     *
     * @param artifactPath the artifact to deploy
     * @return the observable error since not support deploy yet
     */
    @Override
    public Observable<? extends SparkBatchJob> deploy(final String artifactPath) {
        return Observable.error(new UnsupportedOperationException());
    }

    /**
     * New RxAPI: Submit the job.
     *
     * @return Spark Job observable
     */
    @Override
    public Observable<? extends SparkBatchJob> submit() {
        return Observable.fromCallable(() -> createBatchJob());
    }

    @Override
    public boolean isDone(final String state) {
        switch (BatchState.valueOf(state.toUpperCase())) {
            case SHUTTING_DOWN:
            case ERROR:
            case DEAD:
            case SUCCESS:
                return true;
            case NOT_STARTED:
            case STARTING:
            case RUNNING:
            case RECOVERING:
            case BUSY:
            case IDLE:
            default:
                return false;
        }
    }

    @Override
    public boolean isRunning(final String state) {
        return BatchState.valueOf(state.toUpperCase()) == BatchState.RUNNING;
    }

    @Override
    public boolean isSuccess(final String state) {
        return BatchState.valueOf(state.toUpperCase()) == BatchState.SUCCESS;
    }

    /**
     * New RxAPI: Get the job status (from livy).
     *
     * @return Spark Job observable
     */
    public Observable<? extends PostBatchesResponse> getStatus() {
        return Observable.fromCallable(() -> {
            HttpResponse httpResponse = this.getSubmission().getBatchSparkJobStatus(
                    this.getConnectUri().toString(), getBatchId());

            if (httpResponse.getCode() >= 200 && httpResponse.getCode() < 300) {
                return ObjectConvertUtils.convertJsonToObject(
                        httpResponse.getMessage(), PostBatchesResponse.class)
                        .orElseThrow(() -> new UnknownServiceException(
                                "Bad spark job response: " + httpResponse.getMessage()));
            }

            throw new SparkJobException("Can't get cluster " + cluster.getLivyConnectionUrl() + " status.");
        });
    }

    @Override
    public Observable<String> awaitStarted() {
        return getStatus()
                .map(status -> new Pair<>(status.getState(), String.join("\n", status.getLog())))
                .retry(getRetriesMax())
                .repeatWhen(ob -> ob
                        .doOnNext(ignored -> {
                            getCtrlSubject().onNext(new Pair<>(Info, "The Spark job is starting..."));
                        })
                        .delay(getDelaySeconds(), TimeUnit.SECONDS)
                )
                .takeUntil(stateLogPair -> isDone(stateLogPair.getKey()) || isRunning(stateLogPair.getKey()))
                .filter(stateLogPair -> isDone(stateLogPair.getKey()) || isRunning(stateLogPair.getKey()))
                .flatMap(stateLogPair -> {
                    if (isDone(stateLogPair.getKey()) && !isSuccess(stateLogPair.getKey())) {
                        return Observable.error(new SparkJobException(
                                "The Spark job failed to start due to " + stateLogPair.getValue()));
                    }

                    return Observable.just(stateLogPair.getKey());
                });
    }

    @Override
    public Observable<Pair<String, String>> awaitDone() {
        return getJobDoneObservable();
    }

    @Override
    public Observable<String> awaitPostDone() {
        return Observable.empty();
    }
}
