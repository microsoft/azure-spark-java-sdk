// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import org.slf4j.LoggerFactory;
import rx.Observer;
import rx.subjects.PublishSubject;

import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;

public class LivySparkBatchFactory implements SparkBatchJobFactory {
    private final LivyCluster cluster;
    private final PostBatches submissionParameter;
    private final LaterInit<HttpObservable> http = new LaterInit<>();
    private final LaterInit<Observer<Pair<MessageInfoType, String>>> ctrlSubject = new LaterInit<>();

    public LivySparkBatchFactory(final LivyCluster cluster, final PostBatches parameter) {
        this.cluster = cluster;
        this.submissionParameter = parameter;
    }

    public LivySparkBatchFactory http(final HttpObservable httpObservable) {
        this.http.set(httpObservable);

        return this;
    }

    public LivySparkBatchFactory controlSubject(final Observer<Pair<MessageInfoType, String>> subject) {
        this.ctrlSubject.set(subject);

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
        this.ctrlSubject.setIfNull(() -> createLogAsControlSubject(getLoggerForControlSubject()));
        this.http.setIfNull(this::createDefaultHttpObservable);

        return createBatch(cluster, submissionParameter, this.http.get(), this.ctrlSubject.get());
    }
}
