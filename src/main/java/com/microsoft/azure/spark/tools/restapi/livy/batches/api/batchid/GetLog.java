// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.restapi.livy.batches.api.batchid;

import org.apache.http.message.BasicNameValuePair;

public final class GetLog {
    private GetLog() {
        // there is no body in GetLog request
        throw new AssertionError("shouldn't be instantiated");
    }

    public static class FromParameter extends BasicNameValuePair {
        public FromParameter(final int from) {
            super("from", Integer.toString(from));
        }
    }

    public static class SizeParameter extends BasicNameValuePair {
        public SizeParameter(final int size) {
            super("size", Integer.toString(size));
        }
    }
}
