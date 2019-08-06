// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import java.io.File;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import rx.Observable;

public interface Deployable {
    /**
     * Deploy the job artifact into cluster.
     *
     * @param src the artifact to deploy
     * @return Observable: uploaded URI
     *         Observable Error: IOException;
     */
    Observable<String> deploy(final File src);

    /**
     * Get a related path to destination parent folder path based for uploading artifacts.
     * such as: `SparkSubmission/2019/01/20/_random_uuid_/`
     *
     * @return a related path for artifacts uploading. The default method return a path
     *         with destination folder, date and random UUID
     */
    default String getRelatedDestParentPath() {
        int year = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.YEAR);
        int month = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.MONTH) + 1;
        int day = Calendar.getInstance(TimeZone.getTimeZone("UTC")).get(Calendar.DAY_OF_MONTH);

        String uniqueFolderId = UUID.randomUUID().toString();

        return String.format("%s/%04d/%02d/%02d/%s/", getDestinationFolderName(), year, month, day, uniqueFolderId);
    }

    default String getDestinationFolderName() {
        return "SparkSubmission";
    }
}