// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;


import com.microsoft.azure.spark.tools.job.SparkLogFetcher;
import com.microsoft.azure.spark.tools.utils.LaterInit;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

import static java.lang.Thread.sleep;

public class SparkJobLogInputStream extends InputStream {
    private String logType;
    private LaterInit<SparkLogFetcher> sparkLogFetcher = new LaterInit<>();


    private @Nullable String logUrl;

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

    private synchronized @Nullable Pair<String, Long> fetchLog(final long logOffset, final int fetchSize) {
        try {
            return getAttachedLogFetcher()
                    .fetch(getLogType(), logOffset, fetchSize)
                    .toBlocking()
                    .first();
        } catch (NoSuchElementException ignored) {
            return null;
        }
    }

    public SparkLogFetcher getAttachedLogFetcher() {
        return sparkLogFetcher.get();
    }

    @Override
    public int read() throws IOException {
        if (bufferPos >= buffer.length) {
            // throw new IOException("Beyond the buffer end, needs a new log fetch");
            int avail;

            do {
                avail = available();

                if (avail == -1) {
                    return -1;
                }

                try {
                    sleep(1000);
                } catch (InterruptedException ignore) {
                    return -1;
                }

            } while (avail == 0);
        }

        return buffer[bufferPos++];
    }

    @Override
    public int available() throws IOException {
        if (bufferPos >= buffer.length) {
            Pair<String, Long> sliceOffsetPair = fetchLog(offset, -1);

            if (sliceOffsetPair == null) {
                return 0;
            }

            if (sliceOffsetPair.getValue() == -1L) {
                return -1;
            }

            buffer = sliceOffsetPair.getKey().getBytes();
            bufferPos = 0;
            offset = sliceOffsetPair.getValue() + sliceOffsetPair.getKey().length();

            return buffer.length;
        } else {
            return buffer.length - bufferPos;
        }
    }

    public String getLogType() {
        return logType;
    }
}
