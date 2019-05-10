// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import java.util.AbstractMap;
import java.util.Map;

public class Pair<K, V> extends AbstractMap.SimpleImmutableEntry<K, V> {
    public Pair(final K key, final V value) {
        super(key, value);
    }

    public Pair(final Map.Entry<? extends K, ? extends V> entry) {
        super(entry);
    }

    public K getFirst() {
        return getKey();
    }

    public K getLeft() {
        return getKey();
    }

    public V getSecond() {
        return getValue();
    }

    public V getRight() {
        return getValue();
    }

    public AbstractMap.SimpleImmutableEntry<K, V> toImmutableEntry() {
        return new AbstractMap.SimpleImmutableEntry<>(getKey(), getValue());
    }

    public static <K, V> Pair<K, V> of(final K key, final V value) {
        return new Pair<>(key, value);
    }
}
