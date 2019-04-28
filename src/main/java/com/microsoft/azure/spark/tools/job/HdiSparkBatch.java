// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.HdiClusterDetail;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.Observer;

import java.net.URI;

public class HdiSparkBatch extends LivySparkBatch implements SparkDriverLog {
    //    private final BehaviorSubject<SparkDriverLog> driverLogDelegate = BehaviorSubject.create();
    private final LaterInit<SparkDriverLog> driverLogDelegate = new LaterInit<>();

    public HdiSparkBatch(final HdiClusterDetail cluster,
                         final PostBatches submissionParameter,
                         final SparkBatchSubmission sparkBatchSubmission,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject,
                         @Nullable final String accessToken,
                         @Nullable final String destinationRootPath) {
        super(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject, accessToken, destinationRootPath);
    }

    public HdiSparkBatch(final HdiClusterDetail cluster,
                         final PostBatches submissionParameter,
                         final SparkBatchSubmission sparkBatchSubmission,
                         final Observer<Pair<MessageInfoType, String>> ctrlSubject) {
        super(cluster, submissionParameter, sparkBatchSubmission, ctrlSubject);
    }

    @Override
    public Observable<URI> getYarnNMConnectUri() {
        return getDriverLogDelegate()
                .observable()
                .flatMap(SparkDriverLog::getYarnNMConnectUri);
    }

    @Override
    public Observable<String> getDriverHost() {
        return getDriverLogDelegate()
                .observable()
                .flatMap(SparkDriverLog::getDriverHost);
    }

    @Override
    public Observable<Pair<String, Long>> getDriverLog(final String type,
                                                       final long logOffset,
                                                       final int size) {
        return getDriverLogDelegate()
                .observable()
                .flatMap(delegate -> delegate.getDriverLog(type, logOffset, size));
    }

    LaterInit<SparkDriverLog> getDriverLogDelegate() {
        return driverLogDelegate;
    }

    @Override
    public Observable<String> awaitStarted() {
        return super.awaitStarted()
                .flatMap(state -> super.getSparkJobApplicationIdObservable()
                        .doOnNext(appId -> {
                            YarnSparkApplicationDriverLog yarnDriver = new YarnSparkApplicationDriverLog(
                                    appId, (HdiClusterDetail) getCluster(), getSubmission());

                            driverLogDelegate.setIfNull(yarnDriver);
                        })
                );
    }
}
