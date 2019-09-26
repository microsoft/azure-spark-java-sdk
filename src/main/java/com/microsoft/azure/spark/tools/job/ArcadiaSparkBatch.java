// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import rx.Observable;
import rx.Observer;

import com.microsoft.azure.spark.tools.clusters.ArcadiaCompute;
import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.Pair;

import java.util.Arrays;
import java.util.List;

public class ArcadiaSparkBatch extends LivySparkBatch implements DeployableBatch {
    private final Deployable deployDelegate;
    private final PostBatches.Options arcadiaSubmitOptions;
    private final ArcadiaCompute arcadiaCompute;

    public ArcadiaSparkBatch(final ArcadiaCompute compute,
                             final PostBatches submissionParameter,
                             final HttpObservable http,
                             final Observer<Pair<MessageInfoType, String>> ctrlSubject,
                             final Deployable deployDelegate) {
        super(compute, submissionParameter, http, ctrlSubject);
        this.arcadiaCompute = compute;
        this.deployDelegate = deployDelegate;
        this.arcadiaSubmitOptions = new PostBatches.Options().apply(submissionParameter);
    }

    @Override
    protected List<Header> getHeadersToAddOrReplace() {
        return Arrays.asList(new BasicHeader("x-ms-workspace-name", this.arcadiaCompute.getWorkspace()));
    }

    @Override
    public PostBatches getSubmissionParameter() {
        return this.arcadiaSubmitOptions.build();
    }

    @Override
    public Observable<Pair<MessageInfoType, String>> getSubmissionLog() {
        // No batches/{id}/log API support yet
        return Observable.empty();
    }

    @Override
    public Observable<String> awaitStarted() {
        // No submission log and driver log fetching supports
        return Observable.empty();
    }

    @Override
    public Observable<Pair<String, String>> awaitDone() {
        return Observable.empty();
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
        return this.arcadiaSubmitOptions;
    }
}
