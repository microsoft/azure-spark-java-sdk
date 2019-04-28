// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
package com.microsoft.azure.spark.tools.restapi.livy.batches.api;

import java.util.HashMap;
import java.util.Map;

public class SparkConfigures extends HashMap<String, Object> {
    public SparkConfigures(Map<String, ?> m) {
        super(m);
    }

    @SuppressWarnings("unchecked")
    public SparkConfigures(Object o) {
        this((Map<String, Object>) o);
    }

    public SparkConfigures() {
        super();
    }
}
