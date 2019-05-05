// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.restapi.livy.batches.api.PostBatches;
import com.microsoft.azure.spark.tools.utils.JsonConverter;

import java.util.Map;

public class PostBatchesHelper {
    public static PostBatches createSubmitParams(Map<String, String> config) {
        String json = JsonConverter.of(PostBatches.class).toJson(config);

        return JsonConverter.of(PostBatches.class).parseFrom(json);
    }
}
