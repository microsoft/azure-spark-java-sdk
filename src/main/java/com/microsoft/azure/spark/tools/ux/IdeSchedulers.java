// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.ux;

import rx.Scheduler;

public interface IdeSchedulers {
    public Scheduler processBarVisibleAsync(String title);

    public Scheduler processBarVisibleSync(String title);

    public Scheduler dispatchUIThread();
}
