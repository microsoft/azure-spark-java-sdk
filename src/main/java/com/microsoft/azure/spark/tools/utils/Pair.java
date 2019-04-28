// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractMap;
import java.util.Map;

public class Pair<K, V> extends AbstractMap.SimpleImmutableEntry<K, V> {
    public Pair(@Nullable final K key, @Nullable final V value) {
        super(key, value);
    }

    public Pair(final Map.Entry<? extends K, ? extends V> entry) {
        super(entry);
    }

    @Nullable
    public K getFirst() {
        return getKey();
    }

    @Nullable
    public K getLeft() {
        return getKey();
    }

    @Nullable
    public V getSecond() {
        return getValue();
    }

    @Nullable
    public V getRight() {
        return getValue();
    }

    public AbstractMap.SimpleImmutableEntry<@Nullable K, @Nullable V> toImmutableEntry() {
        return new AbstractMap.SimpleImmutableEntry<>(getKey(), getValue());
    }

    public static <K, V> Pair<@Nullable K, @Nullable V> of(@Nullable final K key, @Nullable final V value) {
        return new Pair<>(key, value);
    }
}
