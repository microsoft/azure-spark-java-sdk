// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogMonitor {
    public static List<String> getSparkToolsLogs() {
        return getSparkToolsLogsStream()
                .sorted(Comparator.comparing(LoggingEvent::getTimestamp))
                .map(event -> String.format("LOG: %s %s %-10s (%s) -- %s",
                        event.getTimestamp().toString(),
                        event.getLevel(),
                        event.getCreatingLogger().getName(),
                        event.getThreadName(),
                        event.getMessage()))
                .collect(Collectors.toList());
    }

    public static Stream<LoggingEvent> getSparkToolsLogsStream() {
        return getPackageLogsStream("com.microsoft.azure.spark.tools");
    }
    public static Stream<LoggingEvent> getPackageLogsStream(String packageName) {
        return TestLoggerFactory.getInstance().getAllLoggers().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(packageName))
                .flatMap(entry -> entry.getValue().getLoggingEvents().stream())
                .sorted(Comparator.comparing(LoggingEvent::getTimestamp));
    }

    public static void cleanUpPackageLogs(String packageName) {
        TestLoggerFactory.getInstance().getAllLoggers().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(packageName))
                .forEach(entry -> entry.getValue().clear());
    }

    public static void cleanUpSparkToolsLogs() {
        cleanUpPackageLogs("com.microsoft.azure.spark.tools");
    }
}
