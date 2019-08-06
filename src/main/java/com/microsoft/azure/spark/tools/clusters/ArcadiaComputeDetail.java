// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.clusters;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArcadiaComputeDetail implements ArcadiaCompute {
    public enum LivyUrlType {
        NO_WORKSPACE,
        WORKSPACE_IN_HOSTNAME,
        UNKNOWN
    }

    public static final Pattern LIVY_URL_WORKSPACE_IN_HOSTNAME_PATTERN = Pattern.compile(
            "https?://(?<workspace>[^/.]+)\\.spark\\.projectarcadia\\.net(:(?<port>[0-9]+))?"
                    + "/versions/(?<apiVersion>[^/]+)/sparkcomputes/(?<compute>[^/]+)/?",
            Pattern.CASE_INSENSITIVE);

    public static final Pattern LIVY_URL_NO_WORKSPACE_IN_HOSTNAME_PATTERN = Pattern.compile(
            "https?://arcadia-spark-service-prod\\.(?<region>[^/.]+)(?<suffix>[^/:]+)(:(?<port>[0-9]+))?"
                    + "/versions/(?<apiVersion>[^/]+)/sparkcomputes/(?<compute>[^/]+)/?",
            Pattern.CASE_INSENSITIVE);

    private final String sparkCompute;
    private final String workspace;
    private final String livyConnectionUrl;

    /**
     * Arcadia Compute Detail instance constructor.
     *
     * @param sparkCompute the Spark compute name
     * @param workspace the Spark workspace name
     * @param url the Livy URL to connect
     */
    public ArcadiaComputeDetail(final String sparkCompute, final String workspace, final String url) {
        this.sparkCompute = sparkCompute;
        this.workspace = workspace;
        this.livyConnectionUrl = url;
    }

    @Override
    public String getLivyConnectionUrl() {
        return livyConnectionUrl;
    }

    public String getSparkCompute() {
        return sparkCompute;
    }

    @Override
    public String getWorkspace() {
        return workspace;
    }

    /**
     * Probe the giving Livy URL type.
     *
     * @param url the Livy URL to probe
     * @return the enum LivyUrlType
     */
    public static LivyUrlType probeLivyUrlType(final String url) {
        if (LIVY_URL_NO_WORKSPACE_IN_HOSTNAME_PATTERN.matcher(url).matches()) {
            return LivyUrlType.NO_WORKSPACE;
        } else if (LIVY_URL_WORKSPACE_IN_HOSTNAME_PATTERN.matcher(url).matches()) {
            return LivyUrlType.WORKSPACE_IN_HOSTNAME;
        }

        return LivyUrlType.UNKNOWN;
    }

    /**
     * Factory method from a Livy connection URL with workspace in hostname.
     *
     * @param url the Livy URL with workspace in hostname to parse
     * @return the {@link ArcadiaComputeDetail} instance
     */
    public static ArcadiaComputeDetail parseFromLivyUrl(final String url) {
        Matcher matcher = LIVY_URL_WORKSPACE_IN_HOSTNAME_PATTERN.matcher(url);

        if (matcher.matches() && matcher.group("workspace") != null && matcher.group("compute") != null) {
            return new ArcadiaComputeDetail(
                    matcher.group("workspace"),
                    matcher.group("compute"),
                    url);
        }

        throw new IllegalArgumentException("Bad Arcadia URL: " + url);
    }

    /**
     * Factory method from a Livy connection URL without workspace in hostname.
     *
     * @param urlWithoutWorkspace the Livy URL without workspace in hostname to parse
     * @param workspace  the workspace name
     * @return the {@link ArcadiaComputeDetail} instance
     */
    public static ArcadiaComputeDetail parseFromLivyUrl(final String urlWithoutWorkspace, final String workspace) {
        Matcher matcher = LIVY_URL_NO_WORKSPACE_IN_HOSTNAME_PATTERN.matcher(urlWithoutWorkspace);

        if (matcher.matches() && matcher.group("compute") != null) {
            return new ArcadiaComputeDetail(
                    matcher.group("compute"),
                    workspace,
                    urlWithoutWorkspace);
        }

        throw new IllegalArgumentException("Bad Arcadia URL: " + urlWithoutWorkspace);
    }
}
