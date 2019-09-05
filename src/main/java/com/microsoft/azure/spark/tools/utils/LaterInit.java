// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import com.microsoft.azure.spark.tools.errors.InitializedException;
import com.microsoft.azure.spark.tools.errors.NotInitializedException;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.util.function.Supplier;

public class LaterInit<T> {
    private final BehaviorSubject<T> delegation = BehaviorSubject.create();

    public Observable<T> observable() {
        return delegation.filter(obj -> obj != null).first();
    }

    public synchronized void set(final T value) {
        if (isInitialized()) {
            throw new InitializedException(this.toString() + " delegation has already been initialized.");
        }

        delegation.onNext(value);
    }

    public synchronized void setIfNull(final T value) {
        if (!isInitialized()) {
            set(value);
        }
    }

    public synchronized void setIfNull(final Supplier<T> valueGetter) {
        if (!isInitialized()) {
            set(valueGetter.get());
        }
    }

    public @Nullable T getWithNull() {
        return delegation.getValue();
    }

    public T get() {
        if (!isInitialized()) {
            throw new NotInitializedException(this.toString() + " delegation has not been initialized.");
        }

        return delegation.getValue();
    }

    public boolean isInitialized() {
        return delegation.hasValue();
    }
}
