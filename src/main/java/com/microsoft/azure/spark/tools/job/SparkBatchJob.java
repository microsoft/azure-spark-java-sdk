// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.utils.Pair;
import rx.Observable;
import rx.Observer;

import java.net.URI;

public interface SparkBatchJob {
    /**
     * Getter of the base connection URI for HDInsight Spark Job service.
     *
     * @return the base connection URI for HDInsight Spark Job service
     */
    URI getConnectUri();

    /**
     * Getter of the LIVY Spark batch job ID got from job submission.
     *
     * @return the LIVY Spark batch job ID
     */
    int getBatchId();

    /**
     * Getter of the maximum retry count in RestAPI calling.
     *
     * @return the maximum retry count in RestAPI calling
     */
    int getRetriesMax();

    /**
     * Setter of the maximum retry count in RestAPI calling.
     *
     * @param retriesMax the maximum retry count in RestAPI calling
     */
    void setRetriesMax(int retriesMax);

    /**
     * Getter of the delay seconds between tries in RestAPI calling.
     *
     * @return the delay seconds between tries in RestAPI calling
     */
    int getDelaySeconds();

    /**
     * Setter of the delay seconds between tries in RestAPI calling.
     *
     * @param delaySeconds the delay seconds between tries in RestAPI calling
     */
    void setDelaySeconds(int delaySeconds);

    /**
     * Kill the batch job specified by ID.
     *
     * @return the current instance observable for chain calling,
     *         Observable Error: IOException exceptions for networking connection issues related
     */
    Observable<? extends SparkBatchJob> killBatchJob();

    /**
     * Get Spark batch job driver host by ID.
     *
     * @return Spark driver node host observable
     *         Observable Error: IOException exceptions for the driver host not found
     */
    // Observable<String> getSparkDriverHost();

    /**
     * Get Spark job submission log observable.
     *
     * @return the log type and content pair observable
     */
    Observable<Pair<MessageInfoType, String>> getSubmissionLog();

    /**
     * Await the job started observable.
     *
     * @return the job state string
     */
    Observable<String> awaitStarted();

    /**
     * Await the job done observable.
     *
     * @return the job state string and its diagnostics message
     */
    Observable<Pair<String, String>> awaitDone();

    /**
     * Await the job post actions done, such as the log aggregation.
     * @return the job post action status string
     */
    Observable<String> awaitPostDone();

    /**
     * Get the job control messages observable.
     *
     * @return the job control message type and content pair observable
     */
    Observer<Pair<MessageInfoType, String>> getCtrlSubject();

    /**
     * Deploy the job artifact into cluster.
     *
     * @param artifactPath the artifact to deploy
     * @return ISparkBatchJob observable
     *         Observable Error: IOException;
     */
    Observable<? extends SparkBatchJob> deploy(String artifactPath);

    /**
     * Create a batch Spark job and submit the job into cluster.
     *
     * @return ISparkBatchJob observable
     *         Observable Error: IOException;
     */
    Observable<? extends SparkBatchJob> submit();

    /**
     * Is the job done, success or failure.
     *
     * @return true for success or failure
     */
    boolean isDone(String state);

    /**
     * Is the job running.
     *
     * @return true for running
     */
    boolean isRunning(String state);

    /**
     * Is the job finished with success.
     *
     * @return true for success
     */
    boolean isSuccess(String state);
}
