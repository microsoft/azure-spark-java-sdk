// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.HdiCluster;
import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;

public class HdiSparkBatch extends LivySparkBatch implements SparkLogFetcher {
    private final LaterInit<SparkLogFetcher> driverLogFetcherDelegate = new LaterInit<>();

    public HdiSparkBatch(final HdiCluster cluster,
                         final PostBatches submissionParameter,
                         final SparkBatchSubmission sparkBatchSubmission,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject,
                         final @Nullable String destinationRootPath) {
        super(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject, destinationRootPath);
    }

    public HdiSparkBatch(final HdiCluster cluster,
                         final PostBatches submissionParameter,
                         final SparkBatchSubmission sparkBatchSubmission,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        super(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject);
    }

    @Override
    public Observable<Pair<String, Long>> fetch(final String type,
                                                final long logOffset,
                                                final int size) {
        return getDriverLogFetcherDelegate()
                .observable()
                .flatMap(delegate -> delegate.fetch(type, logOffset, size));
    }

    LaterInit<SparkLogFetcher> getDriverLogFetcherDelegate() {
        return driverLogFetcherDelegate;
    }

    @Override
    public Observable<String> awaitStarted() {
        return super.awaitStarted()
                .flatMap(state -> super.getSparkJobApplicationIdObservable()
                        .doOnNext(appId -> {
                            YarnContainerLogFetcher driverContainerLogFetcher = new YarnContainerLogFetcher(
                                    appId, (YarnCluster) getCluster(), getSubmission());

                            driverLogFetcherDelegate.setIfNull(driverContainerLogFetcher);
                        })
                );
    }
}
