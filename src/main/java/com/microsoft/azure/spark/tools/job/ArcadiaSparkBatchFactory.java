// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

import com.microsoft.azure.spark.tools.clusters.ArcadiaCompute;
import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.AzureHttpObservable;
import com.microsoft.azure.spark.tools.http.AzureOAuthTokenFetcher;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.Pair;

public class ArcadiaSparkBatchFactory extends LivySparkBatchFactory {
    private final Deployable deployable;
    public ArcadiaSparkBatchFactory(final ArcadiaCompute cluster,
                                    final PostBatches.Options options,
                                    final @Nullable HttpObservable http,
                                    final Deployable deployable) {
        super(cluster, options, http);
        this.deployable = deployable;
    }

    public ArcadiaSparkBatchFactory(final ArcadiaCompute cluster,
                                    final PostBatches.Options options,
                                    final Deployable deployable) {
        this(cluster, options, null, deployable);
    }

    @Override
    protected HttpObservable createDefaultHttpObservable() {
        return new AzureHttpObservable(AzureOAuthTokenFetcher.buildFromAzureCli());
    }

    @Override
    protected Logger getLoggerForControlSubject() {
        return LoggerFactory.getLogger(ArcadiaSparkBatch.class);
    }

    @Override
    protected LivySparkBatch createBatch(final LivyCluster livyCluster,
                                         final PostBatches postBatches,
                                         final HttpObservable httpObservable,
                                         final Observer<Pair<MessageInfoType, String>> observer) {
        return new ArcadiaSparkBatch((ArcadiaCompute) livyCluster, postBatches, httpObservable, observer, deployable);
    }
}
