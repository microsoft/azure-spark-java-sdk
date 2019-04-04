// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.Cache;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomElement;
import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlParagraph;
import com.gargoylesoftware.htmlunit.html.HtmlPreformattedText;
import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.legacyhttp.HttpResponse;
import com.microsoft.azure.spark.tools.legacyhttp.ObjectConvertUtils;
import com.microsoft.azure.spark.tools.legacyhttp.SparkBatchSubmission;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.App;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttempt;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttemptsResponse;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.CredentialsProvider;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownServiceException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static rx.exceptions.Exceptions.propagate;

/**
 * The class is to support fetching Spark Driver log from Yarn application UI.
 */
public class YarnSparkApplicationDriverLog implements SparkDriverLog, Logger {
    /**
     * A {DriverLogConversionMode} is a type of enum to present Yarn log UI URI combining ways.
     */
    private enum DriverLogConversionMode {
        WITHOUT_PORT,
        WITH_PORT,
        ORIGINAL;

        public static DriverLogConversionMode next(final DriverLogConversionMode current) {
            List<DriverLogConversionMode> modes = Arrays.asList(DriverLogConversionMode.values());

            int found = modes.indexOf(current);
            return found + 1 >= modes.size() ? ORIGINAL : modes.get(found + 1);
        }
    }

    private final URI yarnNMConnectUri;
    
    @Nullable
    private String currentLogUrl;
    private DriverLogConversionMode logUriConversionMode = DriverLogConversionMode.WITHOUT_PORT;
    private final Cache globalCache;
    private final String applicationId;
    private final YarnCluster cluster;
    private final SparkBatchSubmission submission;

    public YarnSparkApplicationDriverLog(String applicationId, YarnCluster cluster, SparkBatchSubmission submission) {
        this.applicationId = applicationId;
        this.cluster = cluster;
        this.submission = submission;
        this.yarnNMConnectUri = URI.create(this.cluster.getYarnNMConnectionUrl());
        this.globalCache = new Cache();
    }

    public URI getYarnNMConnectUri() {
        return this.yarnNMConnectUri;
    }

    @Nullable
    private String getCurrentLogUrl() {
        return this.currentLogUrl;
    }

    private void setCurrentLogUrl(@Nullable String currentLogUrl) {
        this.currentLogUrl = currentLogUrl;
    }

    private DriverLogConversionMode getLogUriConversionMode() {
        return this.logUriConversionMode;
    }

    private void setLogUriConversionMode(DriverLogConversionMode mode) {
        this.logUriConversionMode = mode;
    }

    /**
     * Get the current Spark job Yarn application attempt log URI Observable.
     */
    private Observable<URI> getSparkJobYarnCurrentAppAttemptLogsLink(final String appId) {
        return Observable.fromCallable(() -> {
            URI getYarnAppAttemptsURI = URI.create(YarnSparkApplicationDriverLog.this.getYarnNMConnectUri() + appId
                    + "/appattempts");
            HttpResponse httpResponse = YarnSparkApplicationDriverLog.this.submission.getHttpResponseViaGet(
                    getYarnAppAttemptsURI.toString());
            Objects.requireNonNull(httpResponse, "httpResponse");
            Optional<AppAttempt> currentAttempt = Optional.empty();

            int httpResponseCode = httpResponse.getCode();
            if (200 <= httpResponseCode && httpResponseCode <= 299) {
                currentAttempt =  ObjectConvertUtils.convertJsonToObject(httpResponse.getMessage(),
                                                                         AppAttemptsResponse.class)
                        .flatMap(it -> it.getAppAttempts().appAttempt.stream()
                                                          .max((o1, o2) -> Integer.compare(o1.getId(), o2.getId())));
            }

            return currentAttempt.orElseThrow(() -> new UnknownServiceException("Bad response when getting from "
                    + getYarnAppAttemptsURI + ", response " + httpResponse.getMessage()));
        }).map(it -> URI.create(it.getLogsLink()));
    }

    private Observable<App> getSparkJobYarnApplication() {
        return Observable.fromCallable(() -> {
            URI getYarnClusterAppURI = URI.create(this.getYarnNMConnectUri() + this.getApplicationId());
            HttpResponse httpResponse = this.submission.getHttpResponseViaGet(getYarnClusterAppURI.toString());
            Objects.requireNonNull(httpResponse, "httpResponse");
            Optional<App> appResponse = Optional.empty();

            int httpResponseCode = httpResponse.getCode();
            if (200 <= httpResponseCode && httpResponseCode <= 299) {
                appResponse = ObjectConvertUtils.convertJsonToObject(httpResponse.getMessage(), AppResponse.class)
                        .map(AppResponse::getApp);
            }

            return appResponse.orElseThrow(() -> new UnknownServiceException("Bad response when getting from "
                    + getYarnClusterAppURI + ", response " + httpResponse.getMessage()));
        });
    }

    protected Observable<String> getJobLogAggregationDoneObservable() {
        return this.getSparkJobYarnApplication()
                .repeatWhen(ob -> ob.delay(2, TimeUnit.SECONDS))
                .filter(Objects::nonNull)
                .takeUntil(this::isYarnAppLogAggregationDone)
                .filter(this::isYarnAppLogAggregationDone)
                .map(yarnApp -> yarnApp.getLogAggregationStatus().toUpperCase());
    }

    private boolean isYarnAppLogAggregationDone(App yarnApp) {
        switch (yarnApp.getLogAggregationStatus()) {
            case "SUCCEEDED":
            case "FAILED":
            case "TIME_OUT":
                return true;
            case "DISABLED":
            case "NOT_START":
            case "RUNNING":
            case "RUNNING_WITH_FAILURE":
            default:
                return false;
        }
    }

    /**
     * Get the Spark job driver log URI observable from the container.
     */
    private Observable<URI> getSparkJobDriverLogUrlObservable() {
        return this.getSparkJobYarnCurrentAppAttemptLogsLink(this.getApplicationId())
                .filter(uri -> StringUtils.isNotBlank(uri.getHost()))
                .flatMap(this::convertToPublicLogUri);
    }

    private Observable<Boolean> isUriValid(final URI uriProbe) {
        return Observable.fromCallable(() ->
                this.submission.getHttpResponseViaGet(uriProbe.toString()).getCode() < 300);
    }

    private Optional<URI> convertToPublicLogUri(DriverLogConversionMode mode, URI internalLogUrl) {
        String normalizedPath = Optional.of(internalLogUrl.getPath()).filter(StringUtils::isNoneBlank)
                .orElse("/");
        URI yarnUiBase = URI.create(cluster.getYarnUIBaseUrl() + (cluster.getYarnUIBaseUrl().endsWith("/") ? "" : "/"));

        switch (mode) {
            case WITHOUT_PORT:
                return Optional.of(yarnUiBase.resolve(String.format("%s%s", internalLogUrl.getHost(), normalizedPath)));
            case WITH_PORT:
                return Optional.of(yarnUiBase.resolve(String.format("%s/port/%s%s",
                        internalLogUrl.getHost(), internalLogUrl.getPath(), normalizedPath)));
            case ORIGINAL:
                return Optional.of(internalLogUrl);
            default:
                throw new AssertionError("Unknown DriverLogConversionMode, shouldn't be reached");
        }
    }

    private Observable<URI> convertToPublicLogUri(final URI internalLogUri) {
        // New version, without port info in log URL
        return this.convertToPublicLogUri(this.getLogUriConversionMode(), internalLogUri)
                .map(Observable::just)
                .orElseGet(() -> {
                    // Probe usable driver log URI
                    DriverLogConversionMode probeMode = YarnSparkApplicationDriverLog.this.getLogUriConversionMode();

                    while (probeMode != null) {
                        Optional<URI> uri = this.convertToPublicLogUri(probeMode, internalLogUri)
                                .filter(uriProbe -> isUriValid(uriProbe).toBlocking().singleOrDefault(false));

                        if (uri.isPresent()) {
                            // Find usable one
                            YarnSparkApplicationDriverLog.this.setLogUriConversionMode(probeMode);
                            return Observable.just(uri.get());
                        }

                        probeMode = DriverLogConversionMode.next(probeMode);
                    }

                    // All modes were probed and all failed
                    return Observable.empty();
                });
    }

    public Observable<SimpleImmutableEntry<String, Long>> getDriverLog(final String type,
                                                                       final long logOffset,
                                                                       final int size) {
        return this.getSparkJobDriverLogUrlObservable()
                .map(Object::toString)
                .flatMap(logUrl -> {
                    long offset = logOffset;

                    if (!StringUtils.equalsIgnoreCase(logUrl, this.getCurrentLogUrl())) {
                        this.setCurrentLogUrl(logUrl);
                        offset = 0L;
                    }

                    String driverLogUrl = this.getCurrentLogUrl();
                    if (driverLogUrl == null) {
                        return Observable.empty();
                    }

                    String logGot = this.getInformationFromYarnLogDom(
                            this.submission.getCredentialsProvider(), driverLogUrl, type, offset, size);

                    return StringUtils.isEmpty(logGot)
                            ? Observable.empty()
                            : Observable.just(new SimpleImmutableEntry<>(logGot, offset));
                });
    }

    public Observable<String> getDriverHost() {
        return this.getSparkJobYarnApplication().map(yarnApp -> {
            if (yarnApp.isFinished()) {
                throw propagate(new UnknownServiceException(
                        "The Livy job " + this.getApplicationId() + " on yarn is not running."));
            }

            String driverHttpAddress = yarnApp.getAmHostHttpAddress();

            /*
             * The sample here is:
             *     host.domain.com:8900
             *       or
             *     10.0.0.15:30060
             */
            String driverHost = this.parseAmHostHttpAddressHost(driverHttpAddress);
            if (driverHost == null) {
                throw propagate(new UnknownServiceException(
                        "Bad amHostHttpAddress got from /yarnui/ws/v1/cluster/apps/" + this.getApplicationId()));
            }

            return driverHost;
        });
    }

    /*
     * Parse host from host:port combination string
     *
     * @param driverHttpAddress the host:port combination string to parse
     * @return the host got, otherwise null
     */
    private String parseAmHostHttpAddressHost(String driverHttpAddress) {
        if (driverHttpAddress == null) {
            return null;
        } else {
            Pattern driverRegex = Pattern.compile("(?<host>[^:]+):(?<port>\\d+)");
            Matcher driverMatcher = driverRegex.matcher(driverHttpAddress);
            return driverMatcher.matches() ? driverMatcher.group("host") : null;
        }
    }

    public final Cache getGlobalCache() {
        return this.globalCache;
    }

    private String getInformationFromYarnLogDom(final CredentialsProvider credentialsProvider,
                                                final String baseUrl,
                                                final String type,
                                                final long start,
                                                final int size) {
        WebClient webClient = new WebClient(BrowserVersion.CHROME);
        webClient.setCache(this.globalCache);
        if (credentialsProvider != null) {
            webClient.setCredentialsProvider(credentialsProvider);
        }

        URI url = URI.create("$baseUrl/").resolve(
                String.format("%s?start=%d", type, start) + (size <= 0 ? "" : String.format("&&end=%d", start + size)));

        try {
            HtmlPage htmlPage = webClient.getPage(Objects.requireNonNull(url, "Can't get Yarn log URL").toString());

            Iterator<DomElement> iterator = htmlPage.getElementById("navcell")
                    .getNextElementSibling()
                    .getChildElements()
                    .iterator();

            HashMap<String, String> logTypeMap = new HashMap<>();
            final AtomicReference<String> logType = new AtomicReference<>();
            String logs = "";

            while (iterator.hasNext()) {
                DomElement node = iterator.next();

                if (node instanceof HtmlParagraph) {
                    // In history server, need to read log type paragraph in page
                    final Pattern logTypePattern = Pattern.compile("Log Type:\\s+(\\S+)");

                    Optional.ofNullable(node.getFirstChild())
                            .map(DomNode::getTextContent)
                            .map(StringUtils::trim)
                            .map(logTypePattern::matcher)
                            .filter(Matcher::matches)
                            .map(matcher -> matcher.group(1))
                            .ifPresent(logType::set);
                } else if (node instanceof HtmlPreformattedText) {
                    // In running, no log type paragraph in page
                    logs = Optional.ofNullable(node.getFirstChild())
                            .map(DomNode::getTextContent)
                            .orElse("");

                    if (logType.get() != null) {
                        // Only get the first <pre>...</pre>
                        logTypeMap.put(logType.get(), logs);

                        logType.set(null);
                    }
                }
            }

            return logTypeMap.getOrDefault(type, logs);
        } catch (FailingHttpStatusCodeException | IOException serviceError) {
            // If the URL is wrong, will get 200 response with content:
            //      Unable to locate 'xxx' log for container
            //  OR
            //      Logs not available for <user>. Aggregation may not be complete,
            //      Check back later or try the nodemanager at...
            //  OR
            //      Cannot get container logs without ...
            //
            // if fetching Yarn log hits the gap between the job running and stop, will get the status 403
            // the log is moving to job history server, just wait and retry.
            log().warn("Can't parse information from YarnUI log page " + url, serviceError);
        }

        return "";
    }

    public final String getApplicationId() {
        return this.applicationId;
    }

    public final YarnCluster getCluster() {
        return this.cluster;
    }
}
