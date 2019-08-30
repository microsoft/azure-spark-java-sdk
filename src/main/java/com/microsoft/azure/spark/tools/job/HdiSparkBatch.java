// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;
import rx.functions.Action1;

import com.microsoft.azure.spark.tools.clusters.HdiCluster;
import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class HdiSparkBatch extends LivySparkBatch implements SparkLogFetcher, DeployableBatch {
    private final LaterInit<SparkLogFetcher> driverLogFetcherDelegate = new LaterInit<>();
    private final Deployable deployDelegate;
    private final PostBatches.Options hdiSubmitOptions;

    public HdiSparkBatch(final HdiCluster cluster,
                         final PostBatches submissionParameter,
                         final HttpObservable httpObservable,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject,
                         final @Nullable String destinationRootPath,
                         final Deployable deployDelegate) {
        super(cluster, submissionParameter, httpObservable, ctrlSubject, destinationRootPath);
        this.deployDelegate = deployDelegate;
        this.hdiSubmitOptions = new PostBatches.Options().apply(submissionParameter);
    }

    public HdiSparkBatch(final HdiCluster cluster,
                         final PostBatches submissionParameter,
                         final HttpObservable httpObservable,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject,
                         final Deployable deployDelegate) {
        super(cluster, submissionParameter, httpObservable, ctrlSubject);
        this.deployDelegate = deployDelegate;
        this.hdiSubmitOptions = new PostBatches.Options().apply(submissionParameter);
    }

    @Override
    public Observable<String> fetch(final String type,
                                                final long logOffset,
                                                final int size) {
        return getDriverLogFetcherDelegate()
                .observable()
                .flatMap(delegate -> delegate.fetch(type, logOffset, size));
    }

    @Override
    public Observable<String> awaitLogAggregationDone() {
        return getDriverLogFetcherDelegate()
                .observable()
                .flatMap(SparkLogFetcher::awaitLogAggregationDone);
    }

    @Override
    public Observable<String> awaitPostDone() {
        return awaitLogAggregationDone()
                .delaySubscription(super.awaitPostDone());
    }

    private LaterInit<SparkLogFetcher> getDriverLogFetcherDelegate() {
        return driverLogFetcherDelegate;
    }

    @Override
    public Observable<String> awaitStarted() {
        return super.awaitStarted()
                .flatMap(state -> super.getSparkJobApplicationId()
                        .repeatWhen(repeat -> repeat.delay(getDelaySeconds(), TimeUnit.SECONDS))
                        .takeUntil(Objects::nonNull)
                        .filter(Objects::nonNull)
                        .doOnNext(new Action1<String>() { // Anonymous function to avoid Nullness check error
                            @Override
                            public void call(final String appId) {
                                YarnContainerLogFetcher driverContainerLogFetcher = new YarnContainerLogFetcher(
                                        appId,
                                        (YarnCluster) HdiSparkBatch.this.getCluster(),
                                        HdiSparkBatch.this.getHttp());

                                driverLogFetcherDelegate.setIfNull(driverContainerLogFetcher);
                            }
                        })
                );
    }

    @Override
    public PostBatches getSubmissionParameter() {
        return this.hdiSubmitOptions.build();
    }

    @Override
    public Deployable getDeployDelegate() {
        return this.deployDelegate;
    }

    @Override
    public void updateOptions(String uploadedUri) {
        getOptions().artifactUri(uploadedUri);
    }

    public PostBatches.Options getOptions() {
        return this.hdiSubmitOptions;
    }
}
