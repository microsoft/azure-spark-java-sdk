// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;


import com.microsoft.azure.spark.tools.job.SparkLogFetcher;
import com.microsoft.azure.spark.tools.utils.LaterInit;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

public class SparkJobLogInputStream extends InputStream {
    private String logType;
    private LaterInit<SparkLogFetcher> sparkLogFetcher = new LaterInit<>();

    private boolean isClosed = false;

    private long offset = 0;
    private byte[] buffer = new byte[0];
    private int bufferPos;

    public SparkJobLogInputStream(final String logType) {
        this.logType = logType;
    }

    public SparkLogFetcher attachLogFetcher(final SparkLogFetcher fetcher) {
        sparkLogFetcher.set(fetcher);

        return fetcher;
    }

    @Override
    public int read() throws IOException {
        if (isClosed) {
            return -1;
        }

        if (bufferPos >= buffer.length) {
            try {
                final String logSlice = sparkLogFetcher.observable()
                        .flatMap(fetcher -> fetcher.fetch(getLogType(), offset, -1))
                        .toBlocking()
                        .first();

                buffer = logSlice.getBytes();
                bufferPos = 0;
                offset += logSlice.length();
            } catch (NoSuchElementException ignored) {
                return -1;
            }
        }

        return buffer[bufferPos++];
    }

    public String getLogType() {
        return logType;
    }

    @Override
    public void close() throws IOException {
        super.close();

        this.isClosed = true;
    }
}
