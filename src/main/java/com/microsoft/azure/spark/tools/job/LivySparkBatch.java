// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.google.common.collect.Streams;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;

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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.microsoft.azure.spark.tools.events.MessageInfoType.Debug;
import static com.microsoft.azure.spark.tools.events.MessageInfoType.Info;
import static com.microsoft.azure.spark.tools.events.MessageInfoType.Log;
import static java.util.Collections.emptyList;
import static rx.Observable.from;

public class LivySparkBatch implements SparkBatchJob, Logger {
    private final Observer<Pair<MessageInfoType, String>> ctrlSubject;

    /**
     * The LIVY Spark batch job ID got from job submission.
     */
    private LaterInit<Integer> batchId = new LaterInit<>();
    private LaterInit<String> appId = new LaterInit<>();

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

    private List<String> submissionLogs = emptyList();

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

    @Override
    public String getName() {
        String name = getSubmissionParameter().getName();

        return name != null ? name : getSubmissionParameter().getClassName();
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
        return getLaterBatchId().get();
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
     * Get current job application Id.
     *
     * @return Application Id Observable
     */
    String getSparkJobApplicationId() {
        return this.getLaterAppId().get();
    }

    LaterInit<String> getLaterAppId() {
        return this.appId;
    }

    @Override
    public Observable<Pair<MessageInfoType, String>> getSubmissionLog() {
        final int maxLinesPerGet = 128;
        final AtomicInteger start = new AtomicInteger(0);
        final LivyLogType defaultLogType = LivyLogType.STDOUT;

        return get()
                .flatMap(batch -> getSparkBatchLogRequest(start.get(), maxLinesPerGet)
                        .map(logResponse -> parseLivyLogs(defaultLogType, logResponse.getLog())))
                .doOnNext(typedLogs -> start.getAndAdd(typedLogs.get(defaultLogType).size()))
                .repeatWhen(repeat -> repeat.delay(200, TimeUnit.MILLISECONDS))
                .takeUntil(typedLogs -> isNoMoreLivyLogs(defaultLogType, typedLogs))
                .flatMap(typedLogs -> from(typedLogs.get(defaultLogType)) // handle other type logs later
                                                .map(line -> new Pair<>(Log, line)))
                .concatWith(Observable.fromCallable(start::get)
                        // spacial fetch for other type logs
                        .flatMap(end -> getSparkBatchLogRequest(end, maxLinesPerGet))
                        .map(logResponse -> parseLivyLogs(defaultLogType, logResponse.getLog()))
                        .flatMap(typedLogs -> from(typedLogs.entrySet()).flatMap(entry -> {
                            switch (entry.getKey()) {
                                case STDERR:
                                case YARN_DIAGNOSTICS:
                                    return entry.getValue().size() > 1
                                            ? from(entry.getValue()).map(line -> Pair.of(MessageInfoType.Error, line))
                                            : Observable.empty();
                                default:
                                    return from(entry.getValue()).map(line -> Pair.of(MessageInfoType.Warning, line));
                            }
                        })))
                .onErrorReturn(err -> new Pair<>(MessageInfoType.Error, err.getMessage()));
    }

    boolean isNoMoreLivyLogs(final LivyLogType defaultLogType, final Map<LivyLogType, List<String>> typedLogs) {
        return typedLogs.get(defaultLogType).isEmpty()
                && ((!StringUtils.equalsIgnoreCase(getState(), "starting") && this.appId.isInitialized())
                        || StringUtils.equalsIgnoreCase(getState(), "dead"));
    }

    public enum LivyLogType {
        STDOUT("stdout:"),
        STDERR("\nstderr:"),
        YARN_DIAGNOSTICS("\nyarn diagnostics:");

        private final String titlePrefix;

        LivyLogType(String titlePrefix) {
            this.titlePrefix = titlePrefix;
        }

        public String getTitlePrefix() {
            return titlePrefix;
        }
    }

    Map<LivyLogType, List<String>> parseLivyLogs(final LivyLogType defaultLogType, final List<String> mixLogs) {
        // Those lines are carried per response,
        // if there is no value followed, the line should not be sent to console
        //     "stdout:"
        //     "\nstderr:"
        //     "\nyarn diagnostics:"

        LivyLogType logTypeFound = defaultLogType;
        final Map<LivyLogType, List<String>> logsParsed = Arrays.stream(LivyLogType.values())
                .map(logType -> Pair.of(logType, (List<String>) new ArrayList<String>()))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

        for (String log : mixLogs) {
            for (LivyLogType logTypeToTest : LivyLogType.values()) {
                if (StringUtils.startsWithIgnoreCase(log, logTypeToTest.getTitlePrefix())) {
                    if (!logsParsed.getOrDefault(logTypeToTest, emptyList()).isEmpty()) {
                        throw new UnknownFormatConversionException("Duplicated log type " + logTypeToTest + " found.");
                    }

                    logTypeFound = logTypeToTest;
                    break;
                }
            }

            logsParsed.get(logTypeFound).add(log);
        }

        return logsParsed;
    }

    @Override
    public Observer<Pair<MessageInfoType, String>> getCtrlSubject() {
        return ctrlSubject;
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
                .repeatWhen(ob -> ob
                        .doOnNext(v -> log().debug("Deploy " + 1 //getDelaySeconds()
                                        + " seconds for next job status probe"))
                        .delay(
                                1, //getDelaySeconds(),
                                TimeUnit.SECONDS))
                .takeUntil(batch -> isDone(batch.state))
                .filter(batch -> isDone(batch.state))
                .map(batch -> new Pair<>(batch.state, String.join("\n", batch.submissionLogs)));
    }

    @Override
    public Observable<String> awaitPostDone() {
        return Observable.empty();
    }

    protected List<Header> getHeadersToAddOrReplace() {
        return emptyList();
    }

    protected HttpObservable getHttp() {
        return this.http;
    }

    public URI getUri() {
        return URI.create(String.format("%s/%s",
                StringUtils.stripEnd(getConnectUri().toString(), "/"), getBatchId()));
    }

    public Observable<LivySparkBatch> get() {
        final String caller = Streams.findLast(Arrays.stream(Thread.currentThread().getStackTrace()).limit(3))
                .map(StackTraceElement::getMethodName)
                .orElse("Unknown");

        return getSparkBatchRequest(caller)
                .map(this::updateWithBatchResponse)
                .defaultIfEmpty(this);
    }

    private Observable<Batch> createSparkBatchRequest() {
        URI uri = getConnectUri();

        PostBatches body = this.getSubmissionParameter();
        String json = JsonConverter.of(PostBatches.class).toJson(body);

        StringEntity entity = new StringEntity(json, StandardCharsets.UTF_8);
        entity.setContentType("application/json");

        getCtrlSubject().onNext(Pair.of(
                Debug, String.format("Spark Batch request to %s, body: %s", uri, body.convertToJson())));

        return getHttp()
                .post(uri.toString(), entity, emptyList(), getHeadersToAddOrReplace(), Batch.class)
                .map(Pair::getFirst);
    }

    private Observable<HttpResponse> deleteSparkBatchRequest() {
        return Observable.fromCallable(this::getUri)
                .flatMap(uri -> getHttp()
                        .delete(uri.toString(), emptyList(), getHeadersToAddOrReplace()));
    }

    private Observable<Batch> getSparkBatchRequest(final String caller) {
        final List<Header> headers = new ArrayList<>(getHeadersToAddOrReplace());
        headers.add(new BasicHeader("X-Invoked-By-Method", caller));

        return Observable.fromCallable(this::getUri)
                .flatMap(uri -> getHttp()
                .get(uri.toString(), emptyList(), headers, Batch.class)
                .map(Pair::getFirst));
    }

    private Observable<GetLogResponse> getSparkBatchLogRequest(final int from, final int size) {
        List<NameValuePair> params = Arrays.asList(new GetLog.FromParameter(from), new GetLog.SizeParameter(size));

        return Observable.fromCallable(() -> URI.create(getUri() + "/log"))
                .flatMap(uri -> getHttp()
                .get(uri.toString(), params, getHeadersToAddOrReplace(), GetLogResponse.class)
                .map(Pair::getFirst));
    }

    private LivySparkBatch updateWithBatchResponse(final Batch batch) {
        this.getLaterBatchId().setIfNull(batch.getId());
        if (batch.getAppId() != null) {
            this.getLaterAppId().setIfNull(batch.getAppId());
        }
        this.state = batch.getState();
        this.submissionLogs = (batch.getLog() == null) ? emptyList() : batch.getLog();

        return this;
    }
}
