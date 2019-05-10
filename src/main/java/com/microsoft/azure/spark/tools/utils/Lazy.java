// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import com.microsoft.azure.spark.tools.errors.InitializedException;
import com.microsoft.azure.spark.tools.errors.NotInitializedException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

public class Lazy<T> implements Supplier<T> {

    private final @Nullable Supplier<T> supplier;

    private @Nullable T value;

    public Lazy() {
        this(null);
    }

    @SuppressWarnings("uninitialized")
    public Lazy(final @Nullable Supplier<T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public T get() {
        if (this.value == null) {
            synchronized (this) {
                if (this.value == null) {
                    if (this.supplier == null) {
                        throw new NotInitializedException(
                                "The supplier method is not set, call getOrEvaluate().");
                    }

                    this.value = this.supplier.get();
                }
            }
        }

        return this.value;
    }

    public T getOrEvaluate(final Supplier<T> evaluate) {
        if (this.value == null) {
            synchronized (this) {
                if (this.value == null) {
                    if (this.supplier != null) {
                        throw new InitializedException("The supplier method has been set, call get().");
                    }

                    this.value = evaluate.get();
                }
            }
        }

        return this.value;
    }
}
