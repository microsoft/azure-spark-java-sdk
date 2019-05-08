// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class LogMonitor {
    public static List<String> getAllPackagesLogs() {
        return TestLoggerFactory.getInstance().getAllLoggingEventsFromLoggers().stream()
                .filter(event -> event.getCreatingLogger().getName().startsWith("com.microsoft.azure.spark.tools"))
                .sorted(Comparator.comparing(LoggingEvent::getTimestamp))
                .map(event -> String.format("LOG: %s %s %-10s (%s) -- %s",
                        event.getTimestamp().toString(),
                        event.getLevel(),
                        event.getCreatingLogger().getName(),
                        event.getThreadName(),
                        event.getMessage()))
                .collect(Collectors.toList());
    }
}
