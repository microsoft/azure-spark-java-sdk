// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;


import com.microsoft.azure.spark.tools.errors.HDIException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

/**
 * The interface is for Spark cluster properties.
 */
public interface ClusterDetail {

    default boolean isEmulator() {
        return false;
    }
    default boolean isConfigInfoAvailable() {
        return false;
    }

    String getName();

    String getTitle();

    @Nullable
    default String getState() {
        return null;
    }

    @Nullable
    default String getLocation() {
        return null;
    }

    String getConnectionUrl();

    @Nullable
    default String getCreateDate() {
        return null;
    }

    // default ClusterType getType() {
    //     return null;
    // }

    @Nullable
    default String getVersion() {
        return null;
    }

    // SubscriptionDetail getSubscription();

    default int getDataNodes() {
        return 0;
    }

    @Nullable
    default String getHttpUserName() throws HDIException {
        return null;
    }

    @Nullable
    default String getHttpPassword() throws HDIException {
        return null;
    }

    @Nullable
    default String getOSType() {
        return null;
    }

    @Nullable
    default String getResourceGroup() {
        return null;
    }

    // @Nullable
    // default IHDIStorageAccount getStorageAccount() throws HDIException {
    //     return null;
    // }

    // default List<HDStorageAccount> getAdditionalStorageAccounts() {
    //     return null;
    // }

    default void getConfigurationInfo() throws IOException, HDIException {
    }

    @Nullable
    default String getSparkVersion() {
        return null;
    }

    // default SparkSubmitStorageType getDefaultStorageType(){
    //     return SparkSubmitStorageType.DEFAULT_STORAGE_ACCOUNT;
    // }
    //
    // default SparkSubmitStorageTypeOptionsForCluster getStorageOptionsType(){
    //     return SparkSubmitStorageTypeOptionsForCluster.ClusterWithFullType;
    // }
}