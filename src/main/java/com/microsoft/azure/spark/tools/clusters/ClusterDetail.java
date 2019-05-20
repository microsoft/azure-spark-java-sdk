// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

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

    default @Nullable String getState() {
        return null;
    }

    default @Nullable String getLocation() {
        return null;
    }

    String getConnectionUrl();

    default @Nullable String getCreateDate() {
        return null;
    }

    // default ClusterType getType() {
    //     return null;
    // }


    default @Nullable String getVersion() {
        return null;
    }

    // SubscriptionDetail getSubscription();

    default int getDataNodes() {
        return 0;
    }


    default @Nullable String getHttpUserName() throws IOException {
        return null;
    }


    default @Nullable String getHttpPassword() throws IOException {
        return null;
    }


    default @Nullable String getOSType() {
        return null;
    }


    default @Nullable String getResourceGroup() {
        return null;
    }

    // @Nullable
    // default IHDIStorageAccount getStorageAccount() throws HDIException {
    //     return null;
    // }

    // default List<HDStorageAccount> getAdditionalStorageAccounts() {
    //     return null;
    // }

    default void getConfigurationInfo() throws IOException {
    }


    default @Nullable String getSparkVersion() {
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