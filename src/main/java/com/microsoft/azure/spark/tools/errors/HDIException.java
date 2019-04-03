// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.errors;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * The base exception class for HDInsight cluster.
 */
public class HDIException extends Exception {
    private String mErrorLog;
    private int errorCode;

    public HDIException(String message) {
        super(message);

        mErrorLog = "";
    }

    public HDIException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;

        mErrorLog = "";
    }

    public HDIException(String message, String errorLog) {
        super(message);

        mErrorLog = errorLog;
    }

    public HDIException(String message, Throwable throwable) {
        super(message, throwable);

        if (throwable instanceof HDIException) {
            mErrorLog = ((HDIException) throwable).getErrorLog();
        } else {
            StringWriter sw = new StringWriter();
            PrintWriter writer = new PrintWriter(sw);

            throwable.printStackTrace(writer);
            writer.flush();

            mErrorLog = sw.toString();
        }
    }

    public String getErrorLog() {
        return mErrorLog;
    }

    public int getErrorCode() {
        return errorCode;
    }
}