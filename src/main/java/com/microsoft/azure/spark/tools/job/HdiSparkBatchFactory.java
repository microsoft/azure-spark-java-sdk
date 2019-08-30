// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.HdiCluster;
import com.microsoft.azure.spark.tools.clusters.LivyCluster;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.AmbariHttpObservable;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

public class HdiSparkBatchFactory extends LivySparkBatchFactory {
    private final Deployable deployable;

    public HdiSparkBatchFactory(final HdiCluster cluster, final Deployable deployable) {
        super(cluster);

        this.deployable = deployable;
    }

    @Override
    protected HttpObservable createDefaultHttpObservable() {
        return new AmbariHttpObservable();
    }

    @Override
    protected Logger getLoggerForControlSubject() {
        return LoggerFactory.getLogger(HdiSparkBatch.class);
    }

    @Override
    protected LivySparkBatch createBatch(final LivyCluster livyCluster,
                                         final PostBatches postBatches,
                                         final HttpObservable httpObservable,
                                         final Observer<Pair<MessageInfoType, String>> observer) {
        return new HdiSparkBatch((HdiCluster) livyCluster, postBatches, httpObservable, observer, deployable);
    }
}
