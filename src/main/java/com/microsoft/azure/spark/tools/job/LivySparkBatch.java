// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.errors.SparkJobException;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.http.HttpResponse;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.livy.batches.Batch;
import com.microsoft.azure.spark.tools.restapi.livy.batches.BatchState;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.batchid.GetLog;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.batchid.GetLogResponse;
import com.microsoft.azure.spark.tools.utils.JsonConverter;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.entity.StringEntity;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Emitter;
import rx.Observable;
import rx.Observer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.microsoft.azure.spark.tools.events.MessageInfoType.Info;
import static com.microsoft.azure.spark.tools.events.MessageInfoType.Log;

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
    private final HttpObservable http;

    /**
     * The setting of maximum retry count in RestAPI calling.
     */
    private int retriesMax = 3;

    /**
     * The setting of delay seconds between tries in RestAPI calling.
     */
    private int delaySeconds = 10;

    private final LivyCluster cluster;

    private @Nullable String destinationRootPath;
    private String state = "__new_instance";

    private @Nullable String appId;
    private @Nullable Map<String, String> appInfo;

    private @Nullable List<String> submissionLogs;

    public LivySparkBatch(
            final LivyCluster cluster,
            final PostBatches submissionParameter,
            final HttpObservable http,
            final Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        this(cluster, submissionParameter, http, ctrlSubject, null);
    }


    public LivySparkBatch(
            final LivyCluster cluster,
            final PostBatches submissionParameter,
            final HttpObservable http,
            final Observer<Pair<MessageInfoType, String>> ctrlSubject,
            final @Nullable String destinationRootPath) {
        this.cluster = cluster;
        this.submissionParameter = submissionParameter;
        this.http = http;
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

    LaterInit<Integer> getLaterBatchId() {
        return batchId;
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
     * Kill the batch job specified by ID.
     *
     * @return the current instance for chain calling
     */
    @Override
    public Observable<? extends SparkBatchJob> killBatchJob() {
        return deleteSparkBatchRequest()
                .map(resp -> {
                    return this;
                })
                .defaultIfEmpty(this);
    }

    /**
     * Get Spark Job Yarn application state saved.
     *
     * @return the Yarn application state of last got
     */
    public String getState() {
        return state;
    }

    /**
     * New RxAPI: Get current job application Id.
     *
     * @return Application Id Observable
     */
    Observable<@Nullable String> getSparkJobApplicationId() {
        return get()
                .map(batch -> batch.appId);
    }

    @Override
    public Observable<Pair<MessageInfoType, String>> getSubmissionLog() {
        // Those lines are carried per response,
        // if there is no value followed, the line should not be sent to console
        final Set<String> ignoredEmptyLines = new HashSet<>(Arrays.asList(
                "stdout:",
                "stderr:",
                "yarn diagnostics:"));

        final int maxLinesPerGet = 128;
        return Observable.create(ob -> {
            AtomicInteger start = new AtomicInteger(0);

            Predicate<Pair<String, GetLogResponse>> noMoreLogs = appIdWithLog ->
                    appIdWithLog.getRight().getLog().size() == 0
                            && !StringUtils.equalsIgnoreCase(getState(), "starting")
                            && appIdWithLog.getLeft() != null;

            getSparkJobApplicationId()
                    .flatMap(applicationId -> getSparkBatchLogRequest(start.get(), maxLinesPerGet), Pair::of)
                    .doOnNext(appIdWithLog -> start.getAndAdd(appIdWithLog.getRight().getLog().size()))
                    .repeatWhen(repeat -> repeat.delay(200, TimeUnit.MILLISECONDS))
                    .takeUntil(noMoreLogs::test)
                    .filter(appIdWithLog -> !noMoreLogs.test(appIdWithLog))
                    .map(appIdWithLog -> appIdWithLog.getRight().getLog())
                    .subscribe(logs -> logs.stream()
                                    .filter(line -> !ignoredEmptyLines.contains(line.trim().toLowerCase()))
                                    .forEach(line -> ob.onNext(new Pair<>(Log, line))),
                            err -> ob.onNext(new Pair<>(MessageInfoType.Error, err.getMessage())),
                            () -> ob.onCompleted());

        }, Emitter.BackpressureMode.BUFFER);
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
     * Submit the job.
     *
     * @return Spark Job observable
     */
    @Override
    public Observable<? extends SparkBatchJob> submit() {
        return createSparkBatchRequest()
                .map(this::updateWithBatchResponse)
                .defaultIfEmpty(this);
    }

    @Override
    public boolean isDone(final String toCheck) {
        switch (BatchState.valueOf(toCheck.toUpperCase())) {
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
    public boolean isRunning(final String toCheck) {
        return BatchState.valueOf(toCheck.toUpperCase()) == BatchState.RUNNING;
    }

    @Override
    public boolean isSuccess(final String toCheck) {
        return BatchState.valueOf(toCheck.toUpperCase()) == BatchState.SUCCESS;
    }

    @Override
    public Observable<String> awaitStarted() {
        return get()
                .retry(getRetriesMax())
                .repeatWhen(ob -> ob
                        .doOnNext(ignored -> {
                            getCtrlSubject().onNext(new Pair<>(Info, "The Spark job is starting..."));
                        })
                        .delay(getDelaySeconds(), TimeUnit.SECONDS)
                )
                .takeUntil(batch -> isDone(batch.state) || isRunning(batch.state))
                .filter(batch -> isDone(batch.state) || isRunning(batch.state))
                .flatMap(batch -> {
                    if (isDone(batch.state) && !isSuccess(batch.state)) {
                        return Observable.error(new SparkJobException("The Spark job failed to start due to "
                                + String.join("\n", batch.submissionLogs)));
                    }

                    return Observable.just(batch.state);
                });
    }

    @Override
    public Observable<Pair<String, String>> awaitDone() {
        return get()
                .repeatWhen(ob -> {
                    log().debug("Deploy " + 1 //getDelaySeconds()
                            + " seconds for next job status probe");
                    return ob.delay(
                            1, //getDelaySeconds(),
                            TimeUnit.SECONDS);
                })
                .takeUntil(batch -> !isDone(batch.state))
                .filter(batch -> !isDone(batch.state))
                .map(batch -> new Pair<>(batch.state, String.join("\n", batch.submissionLogs)));
    }

    @Override
    public Observable<String> awaitPostDone() {
        return Observable.empty();
    }

    protected HttpObservable getHttp() {
        return this.http;
    }

    public URI getUri() {
        return URI.create(String.format("%s/%s",
                StringUtils.stripEnd(getConnectUri().toString(), "/"), getBatchId()));
    }

    public Observable<LivySparkBatch> get() {
        return getSparkBatchRequest()
                .map(this::updateWithBatchResponse)
                .defaultIfEmpty(this);
    }

    private Observable<Batch> createSparkBatchRequest() {
        URI uri = getConnectUri();

        PostBatches body = this.getSubmissionParameter();
        String json = JsonConverter.of(PostBatches.class).toJson(body);

        StringEntity entity = new StringEntity(json, StandardCharsets.UTF_8);
        entity.setContentType("application/json");

        return getHttp()
//                .withUuidUserAgent()
                .post(uri.toString(), entity, null, null, Batch.class);
    }

    private Observable<HttpResponse> deleteSparkBatchRequest() {
        URI uri = getUri();

        return getHttp()
//                .withUuidUserAgent()
                .delete(uri.toString(), null, null);
    }

    private Observable<Batch> getSparkBatchRequest() {
        URI uri = getUri();

        return getHttp()
//                .withUuidUserAgent()
                .get(uri.toString(), null, null, Batch.class);
    }

    private Observable<GetLogResponse> getSparkBatchLogRequest(final int from, final int size) {
        URI uri = URI.create(getUri() + "/log");

        List<NameValuePair> params = Arrays.asList(new GetLog.FromParameter(from), new GetLog.SizeParameter(size));

        return getHttp()
//                .withUuidUserAgent()
                .get(uri.toString(), params, null, GetLogResponse.class);
    }

    private LivySparkBatch updateWithBatchResponse(final Batch batch) {
        getLaterBatchId().setIfNull(batch.getId());
        this.state = batch.getState();
        this.appId = batch.getAppId();
        this.appInfo = batch.getAppInfo();
        this.submissionLogs = batch.getLog();

        return this;
    }
}
