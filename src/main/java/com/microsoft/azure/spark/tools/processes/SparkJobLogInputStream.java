// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.processes;


import com.microsoft.azure.spark.tools.job.SparkBatchJob;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Optional;

import static java.lang.Thread.sleep;

public class SparkJobLogInputStream extends InputStream {
    private String logType;
    @Nullable
    private SparkBatchJob sparkBatchJob;
    @Nullable
    private String logUrl;

    private long offset = 0;
    private byte[] buffer = new byte[0];
    private int bufferPos;

    public SparkJobLogInputStream(String logType) {
        this.logType = logType;
    }

    public SparkBatchJob attachJob(SparkBatchJob sparkJob) {
        setSparkBatchJob(sparkJob);

        return sparkJob;
    }

    private synchronized Optional<SimpleImmutableEntry<String, Long>> fetchLog(long logOffset, int fetchSize) {
        return Optional.empty();
        // return getAttachedJob()
        //         .map(job -> job.getDriverLog(getLogType(), logOffset, fetchSize)
        //                        .toBlocking().singleOrDefault(null));
    }

    void setSparkBatchJob(@Nullable SparkBatchJob sparkBatchJob) {
        this.sparkBatchJob = sparkBatchJob;
    }

    public Optional<SparkBatchJob> getAttachedJob() {
        return Optional.ofNullable(sparkBatchJob);
    }

    @Override
    public int read() throws IOException {
        if (bufferPos >= buffer.length) {
            throw new IOException("Beyond the buffer end, needs a new log fetch");
        }

        return buffer[bufferPos++];
    }

    @Override
    public int available() throws IOException {
        if (bufferPos >= buffer.length) {
            return fetchLog(offset, -1)
                    .map(sliceOffsetPair -> {
                        buffer = sliceOffsetPair.getKey().getBytes();
                        bufferPos = 0;
                        offset = sliceOffsetPair.getValue() + sliceOffsetPair.getKey().length();

                        return buffer.length;
                    }).orElseGet(() -> {
                        try {
                            sleep(3000);
                        } catch (InterruptedException ignore) { }

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
