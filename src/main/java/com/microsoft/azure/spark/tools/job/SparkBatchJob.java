// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.events.MessageInfoType;
import com.microsoft.azure.spark.tools.utils.Pair;
import rx.Observable;
import rx.Observer;

import java.io.File;
import java.net.URI;

public interface SparkBatchJob {
    String getName();

    /**
     * Getter of the base connection URI for Spark Job service.
     *
     * @return the base connection URI for Spark Job service
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
     * @return Observable of the current instance for chain calling,
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
     * @return Observable of the log type and content pair
     */
    Observable<Pair<MessageInfoType, String>> getSubmissionLog();

    /**
     * Await the job started observable.
     *
     * @return Observable of the job state string
     */
    Observable<String> awaitStarted();

    /**
     * Await the job done observable.
     *
     * @return Observable of the job state string and its diagnostics message
     */
    Observable<Pair<String, String>> awaitDone();

    /**
     * Await the job post actions done, such as the log aggregation.
     * @return Observable of job post action status string
     */
    Observable<String> awaitPostDone();

    /**
     * Get the job control messages observable.
     *
     * @return Observable of the job control message type and content pair
     */
    Observer<Pair<MessageInfoType, String>> getCtrlSubject();

    /**
     * Deploy the job artifact into cluster.
     *
     * @param artifactPath the artifact to deploy
     * @return Observable of the current instance for chain calling,
     *         Observable Error: IOException;
     */
    Observable<? extends SparkBatchJob> deploy(final File artifactPath);

    /**
     * Create a batch Spark job and submit the job into cluster.
     *
     * @return Observable of the current instance for chain calling,
     *         Observable Error: IOException;
     */
    Observable<? extends SparkBatchJob> submit();

    /**
     * Is the job done, success or failure.
     *
     * @param state to check if the job is done or not
     * @return true for the job is done (success or failure)
     */
    boolean isDone(String state);

    /**
     * Is the job running.
     *
     * @param state to check if the job is running or not
     * @return true for running
     */
    boolean isRunning(String state);

    /**
     * Is the job finished with success.
     *
     * @param state to check if the job is finished with success or not
     * @return true for success
     */
    boolean isSuccess(String state);
}
