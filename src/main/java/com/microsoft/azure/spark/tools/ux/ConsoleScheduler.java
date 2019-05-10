// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.ux;

import rx.Scheduler;
import rx.schedulers.Schedulers;

public class ConsoleScheduler implements IdeSchedulers {
    @Override
    public Scheduler processBarVisibleAsync(final String title) {
        return Schedulers.trampoline();
    }

    @Override
    public Scheduler processBarVisibleSync(final String title) {
        return Schedulers.immediate();
    }

    @Override
    public Scheduler dispatchUIThread() {
        return Schedulers.trampoline();
    }
}
