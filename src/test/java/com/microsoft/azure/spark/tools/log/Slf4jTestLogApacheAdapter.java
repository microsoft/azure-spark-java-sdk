// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.log;

import org.apache.commons.logging.Log;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class Slf4jTestLogApacheAdapter implements Log {
    private final TestLogger logger;

    public Slf4jTestLogApacheAdapter(String loggerName) {
        this.logger = TestLoggerFactory.getTestLogger(loggerName);
    }

    @Override
    public void debug(Object message) {
        this.logger.debug(String.valueOf(message));
    }

    @Override
    public void debug(Object message, Throwable t) {
        this.logger.debug(String.valueOf(message), t);
    }

    @Override
    public void error(Object message) {
        this.logger.error(String.valueOf(message));
    }

    @Override
    public void error(Object message, Throwable t) {
        this.logger.error(String.valueOf(message), t);
    }

    @Override
    public void fatal(Object message) {
        this.logger.error(String.valueOf(message));
    }

    @Override
    public void fatal(Object message, Throwable t) {
        this.logger.error(String.valueOf(message), t);
    }

    @Override
    public void info(Object message) {
        this.logger.info(String.valueOf(message));
    }

    @Override
    public void info(Object message, Throwable t) {
        this.logger.info(String.valueOf(message), t);
    }

    @Override
    public boolean isDebugEnabled() {
        return this.logger.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return this.logger.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled() {
        return this.logger.isErrorEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return this.logger.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return this.logger.isTraceEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return this.logger.isWarnEnabled();
    }

    @Override
    public void trace(Object message) {
        this.logger.trace(String.valueOf(message));
    }

    @Override
    public void trace(Object message, Throwable t) {
        this.logger.trace(String.valueOf(message), t);
    }

    @Override
    public void warn(Object message) {
        this.logger.warn(String.valueOf(message));
    }

    @Override
    public void warn(Object message, Throwable t) {
        this.logger.warn(String.valueOf(message), t);
    }
}
