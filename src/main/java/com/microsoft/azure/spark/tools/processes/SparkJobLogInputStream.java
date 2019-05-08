// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;


import com.microsoft.azure.spark.tools.job.SparkLogFetcher;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static java.lang.Thread.sleep;

public class SparkJobLogInputStream extends InputStream {
    private String logType;
    @Nullable
    private SparkLogFetcher sparkLogFetcher;
    @Nullable
    private String logUrl;

    private long offset = 0;
    private byte[] buffer = new byte[0];
    private int bufferPos;

    public SparkJobLogInputStream(final String logType) {
        this.logType = logType;
    }

    public SparkLogFetcher attachLogFetcher(final SparkLogFetcher fetcher) {
        setSparkLogFetcher(fetcher);

        return fetcher;
    }

    private synchronized Optional<Pair<String, Long>> fetchLog(final long logOffset, final int fetchSize) {
        return getAttachedLogFetcher()
                .map(logFetcher -> logFetcher
                        .fetch(getLogType(), logOffset, fetchSize)
                        .toBlocking()
                        .firstOrDefault(null));
    }

    void setSparkLogFetcher(@Nullable final SparkLogFetcher sparkLogFetcher) {
        this.sparkLogFetcher = sparkLogFetcher;
    }

    public Optional<SparkLogFetcher> getAttachedLogFetcher() {
        return Optional.ofNullable(sparkLogFetcher);
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
            return fetchLog(offset, -1)
                    .map(sliceOffsetPair -> {
                        if (sliceOffsetPair.getValue() == -1L) {
                            return -1;
                        }

                        buffer = sliceOffsetPair.getKey().getBytes();
                        bufferPos = 0;
                        offset = sliceOffsetPair.getValue() + sliceOffsetPair.getKey().length();

                        return buffer.length;
                    }).orElseGet(() -> {
                        return 0;
                    });
        } else {
            return buffer.length - bufferPos;
        }
    }

    void setLogUrl(@Nullable String logUrl) {
        this.logUrl = logUrl;
    }

    public Optional<String> getLogUrl() {
        return Optional.ofNullable(logUrl);
    }

    public String getLogType() {
        return logType;
    }
}
