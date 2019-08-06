// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches.Options;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.subjects.PublishSubject;

public class LivySparkBatchFactory implements SparkBatchJobFactory, Logger {
    private final LivyCluster cluster;
    private final PostBatches submissionParameter;
    private @Nullable HttpObservable http;
    private @Nullable Observer<Pair<MessageInfoType, String>> ctrlSubject;

    public LivySparkBatchFactory(
            final LivyCluster cluster,
            final PostBatches submissionParameter,
            final @Nullable HttpObservable http) {
        this.cluster = cluster;
        this.submissionParameter = submissionParameter;
        this.http = http;
    }

    public LivySparkBatchFactory(
            final LivyCluster cluster,
            final PostBatches submissionParameter) {
        this(cluster, submissionParameter, null);
    }

    public LivySparkBatchFactory(
            final LivyCluster cluster,
            final Options options,
            final @Nullable HttpObservable http) {
        this.cluster = cluster;
        this.submissionParameter = options.build();
        this.http = http;
    }

    public LivySparkBatchFactory(
            final LivyCluster cluster,
            final Options options) {
        this(cluster, options, null);
    }

    public LivySparkBatchFactory controlSubject(final Observer<Pair<MessageInfoType, String>> subject) {
        this.ctrlSubject = subject;

        return this;
    }

    private PublishSubject<Pair<MessageInfoType, String>> createLogAsControlSubject(final org.slf4j.Logger log) {
        PublishSubject<Pair<MessageInfoType, String>> logAsCtrlSubject = PublishSubject.create();
        logAsCtrlSubject.subscribe(
                typedMessage -> {
                    switch (typedMessage.getFirst()) {
                        case Debug:
                            log.debug(typedMessage.getRight());
                            break;
                        case Error:
                            log.error(typedMessage.getRight());
                            break;
                        case Info:
                            log.info(typedMessage.getRight());
                            break;
                        case Log:
                        case Hyperlink:
                            log.trace(typedMessage.getRight());
                            break;
                        case Warning:
                            log.warn(typedMessage.getRight());
                            break;
                        default:
                            throw new AssertionError(typedMessage.getFirst());
                    }
                },
                err -> {
                    throw new RuntimeException(err);
                }
        );

        return logAsCtrlSubject;
    }

    protected HttpObservable createDefaultHttpObservable() {
        return new HttpObservable();
    }

    protected org.slf4j.Logger getLoggerForControlSubject() {
        return LoggerFactory.getLogger(LivySparkBatch.class);
    }

    protected LivySparkBatch createBatch(
            final LivyCluster livyCluster,
            final PostBatches postBatches,
            final HttpObservable httpObservable,
            final Observer<Pair<MessageInfoType, String>> observer) {
        return new LivySparkBatch(livyCluster, postBatches, httpObservable, observer);
    }

    @Override
    public SparkBatchJob factory() {
        Observer<Pair<MessageInfoType, String>> subject = this.ctrlSubject != null
                ? this.ctrlSubject
                : createLogAsControlSubject(getLoggerForControlSubject());

        HttpObservable httpObservable = this.http != null
                ? this.http
                : createDefaultHttpObservable();

        return createBatch(cluster, submissionParameter, httpObservable, subject);
    }
}
