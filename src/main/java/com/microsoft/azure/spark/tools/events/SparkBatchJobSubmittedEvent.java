// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.events;


import com.microsoft.azure.spark.tools.job.SparkBatchJob;

public class SparkBatchJobSubmittedEvent implements SparkBatchJobSubmissionEvent {
    private SparkBatchJob job;

    public SparkBatchJobSubmittedEvent(SparkBatchJob job) {
        this.job = job;
    }

    public SparkBatchJob getJob() {
        return job;
    }
}
