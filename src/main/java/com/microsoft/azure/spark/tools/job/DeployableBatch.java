// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import rx.Observable;

import java.io.File;

public interface DeployableBatch {
    Deployable getDeployDelegate();

    void updateOptions(final String uploadedUri);

    default Observable<? extends DeployableBatch> deployAndUpdateOptions(final File artifactPath) {
        return getDeployDelegate().deploy(artifactPath)
                .map(uploadedUri -> {
                    updateOptions(uploadedUri);

                    return this;
                });
    }
}
