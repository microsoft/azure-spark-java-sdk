// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.App;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttempt;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttemptsResponse;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppResponse;
import com.microsoft.azure.spark.tools.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicNameValuePair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import rx.Observable;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
public class YarnContainerLogFetcher implements SparkLogFetcher, Logger {
    private final HttpObservable http;

    /**
     * A {LogConversionMode} is a type of enum to present Yarn log UI URI combining ways.
     */
    private enum LogConversionMode {
        UNKNOWN,
        WITHOUT_PORT,
        WITH_PORT,
        ORIGINAL;

        public static LogConversionMode next(final LogConversionMode current) {
            List<LogConversionMode> modes = Arrays.asList(LogConversionMode.values());

            int found = modes.indexOf(current);
            if (found + 1 >= modes.size()) {
                throw new NoSuchElementException();
            } else {
                return modes.get(found + 1);
            }
        }
    }

    private final URI yarnNMConnectUri;
    
    private @Nullable String currentLogUrl;
    private LogConversionMode logUriConversionMode = LogConversionMode.UNKNOWN;
    private final String applicationId;
    private final YarnCluster cluster;

    public YarnContainerLogFetcher(final String applicationId,
                                   final YarnCluster cluster,
                                   final HttpObservable http) {
        this.applicationId = applicationId;
        this.cluster = cluster;
        this.http = http;
        this.yarnNMConnectUri = URI.create(this.cluster.getYarnNMConnectionUrl());
    }

    public URI getYarnNMConnectUri() {
        return this.yarnNMConnectUri;
    }

    private @Nullable String getCurrentLogUrl() {
        return this.currentLogUrl;
    }

    private void setCurrentLogUrl(final @Nullable String currentLogUrl) {
        this.currentLogUrl = currentLogUrl;
    }

    private LogConversionMode getLogUriConversionMode() {
        return this.logUriConversionMode;
    }

    private void setLogUriConversionMode(final LogConversionMode mode) {
        this.logUriConversionMode = mode;
    }

    /**
     * Get the current Spark job Yarn application attempt log URI Observable.
     */
    Observable<URI> getSparkJobYarnCurrentAppAttemptLogsLink() {
        return this.getYarnApplicationAttemptsRequest()
                .map(appAttempts -> appAttempts.stream().max((o1, o2) -> Integer.compare(o1.getId(), o2.getId())))
                .flatMap(attemptOpt -> attemptOpt.map(it -> Observable.just(URI.create(it.getLogsLink())))
                                                 .orElse(Observable.empty()));
    }

    protected Observable<String> getJobLogAggregationDone() {
        return this.getYarnApplicationRequest()
                .repeatWhen(ob -> ob.delay(2, TimeUnit.SECONDS))
                .filter(Objects::nonNull)
                .takeUntil(this::isYarnAppLogAggregationDone)
                .filter(this::isYarnAppLogAggregationDone)
                .map(yarnApp -> yarnApp.getLogAggregationStatus().toUpperCase());
    }

    private boolean isYarnAppLogAggregationDone(final App yarnApp) {
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
     * Get the Spark job log URI observable from the container.
     */
    Observable<URI> getSparkJobDriverLogUrl() {
        return this.getSparkJobYarnCurrentAppAttemptLogsLink()
                .filter(uri -> StringUtils.isNotBlank(uri.getHost()))
                .flatMap(this::convertToPublicLogUri);
    }

    private Observable<Boolean> isUriValid(final URI uriProbe) {
        return getHttp()
                .request(new HttpGet(uriProbe), null, null, null)
                .map(response -> response.getStatusLine().getStatusCode() < 300);
    }

    private Optional<URI> convertToPublicLogUri(final LogConversionMode mode, final URI internalLogUrl) {
        String normalizedPath = Optional.of(internalLogUrl.getPath()).filter(StringUtils::isNoneBlank)
                .orElse("/");
        URI yarnUiBase = URI.create(getCluster().getYarnUIBaseUrl()
                        + (getCluster().getYarnUIBaseUrl().endsWith("/") ? "" : "/"));

        switch (mode) {
            case UNKNOWN:
                return Optional.empty();
            case WITHOUT_PORT:
                return Optional.of(yarnUiBase.resolve(String.format("%s%s", internalLogUrl.getHost(), normalizedPath)));
            case WITH_PORT:
                return Optional.of(yarnUiBase.resolve(String.format("%s/port/%s%s",
                        internalLogUrl.getHost(), internalLogUrl.getPort(), normalizedPath)));
            case ORIGINAL:
                return Optional.of(internalLogUrl);
            default:
                throw new AssertionError("Unknown LogConversionMode, shouldn't be reached");
        }
    }

    private Observable<URI> convertToPublicLogUri(final URI internalLogUri) {
        // New version, without port info in log URL
        return this.convertToPublicLogUri(this.getLogUriConversionMode(), internalLogUri)
                .map(Observable::just)
                .orElseGet(() -> {
                    // Probe usable log URI
                    LogConversionMode probeMode = YarnContainerLogFetcher.this.getLogUriConversionMode();

                    boolean isNoMoreTry = false;
                    while (!isNoMoreTry) {
                        Optional<URI> uri = this.convertToPublicLogUri(probeMode, internalLogUri)
                                .filter(uriProbe -> isUriValid(uriProbe).toBlocking().firstOrDefault(false));

                        if (uri.isPresent()) {
                            // Find usable one
                            YarnContainerLogFetcher.this.setLogUriConversionMode(probeMode);
                            return Observable.just(uri.get());
                        }

                        try {
                            probeMode = LogConversionMode.next(probeMode);
                        } catch (NoSuchElementException ignore) {
                            log().warn("Can't find conversion mode of Yarn " + getYarnNMConnectUri());
                            isNoMoreTry = true;
                        }
                    }

                    // All modes were probed and all failed
                    return Observable.empty();
                });
    }

    @Override
    public Observable<Pair<String, Long>> fetch(final String type, final long logOffset, final int size) {
        return this.getSparkJobDriverLogUrl()
                .map(Object::toString)
                .flatMap(logUrl -> {
                    long offset = logOffset;

                    if (!StringUtils.equalsIgnoreCase(logUrl, this.getCurrentLogUrl())) {
                        this.setCurrentLogUrl(logUrl);
                        offset = 0L;
                    }

                    String probedLogUrl = this.getCurrentLogUrl();
                    if (probedLogUrl == null) {
                        return Observable.empty();
                    }

                    String logGot = this.getInformationFromYarnLogDom(probedLogUrl, type, offset, size);

                    if (StringUtils.isEmpty(logGot)) {
                        return getYarnApplicationRequest()
                                .flatMap(app -> {
                                    if (isLogFetchable(app.getState())) {
                                        return Observable.empty();
                                    } else {
                                        return Observable.just(Pair.of("", -1L));
                                    }
                                });
                    } else {
                        return Observable.just(Pair.of(logGot, offset));
                    }
                });
    }

    private boolean isLogFetchable(final String status) {
        switch (status.toUpperCase()) {
            case "RUNNING":
            case "SUBMITTED":
            case "ACCEPTED":
                return true;
            default:
                return false;
        }
    }

    public Observable<String> getDriverHost() {
        return this.getYarnApplicationRequest().map(yarnApp -> {
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
    @Nullable String parseAmHostHttpAddressHost(final @Nullable String driverHttpAddress) {
        if (driverHttpAddress == null) {
            return null;
        } else {
            Pattern driverRegex = Pattern.compile("(?<host>[^:]+):(?<port>\\d+)");
            Matcher driverMatcher = driverRegex.matcher(driverHttpAddress);
            return driverMatcher.matches() ? driverMatcher.group("host") : null;
        }
    }

    private String getInformationFromYarnLogDom(final String baseUrl,
                                                final String type,
                                                final long start,
                                                final int size) {
        URI url = URI.create(StringUtils.stripEnd(baseUrl, "/") + "/").resolve(
                String.format("%s?start=%d", type, start) + (size <= 0 ? "" : String.format("&&end=%d", start + size)));

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("start", Long.toString(start)));
        if (size > 0) {
            params.add(new BasicNameValuePair("size", Long.toString(size)));
        }

        String typedLogs = getHttp()
                .requestWithHttpResponse(new HttpGet(url), null, params, null)
                .doOnNext(response -> {
                    try {
                        log().debug("Fetch log from " + url + ", got " + response.getCode()
                                + " with " + response.getMessage());
                    } catch (IOException ignored) {
                        // The upstream requestWithHttpResponse() has already get message buffered.
                    }
                })
                .map(response -> {

                    Document doc = null;
                    try {
                        doc = Jsoup.parse(response.getMessage());
                    } catch (IOException ignored) {
                        // The upstream requestWithHttpResponse() has already get message buffered.
                        throw new AssertionError("The upstream has got messages.");
                    }

                    Iterator<Element> iterator = Optional.ofNullable(doc.getElementById("navcell"))
                            .map(Element::nextElementSibling)
                            .map(Element::children)
                            .map(ArrayList::iterator)
                            .orElse(Collections.emptyIterator());

                    HashMap<String, String> logTypeMap = new HashMap<>();
                    final AtomicReference<String> logType = new AtomicReference<>();
                    String logs = "";

                    while (iterator.hasNext()) {
                        Element node = iterator.next();

                        if (StringUtils.equalsIgnoreCase(node.tagName(), "p")) {
                            // In history server, need to read log type paragraph in page
                            final Pattern logTypePattern = Pattern.compile("Log Type:\\s+(\\S+)");

                            node.childNodes().stream()
                                    .findFirst()
                                    .map(Node::toString)
                                    .map(StringUtils::trim)
                                    .map(logTypePattern::matcher)
                                    .filter(Matcher::matches)
                                    .map(matcher -> matcher.group(1))
                                    .ifPresent(logType::set);
                        } else if (StringUtils.equalsIgnoreCase(node.tagName(), "pre")) {
                            // In running, no log type paragraph in page
                            logs = node.childNodes().stream()
                                    .findFirst()
                                    .map(Node::toString)
                                    .orElse("");

                            if (logType.get() != null) {
                                // Only get the first <pre>...</pre>
                                logTypeMap.put(logType.get(), logs);

                                logType.set(null);
                            }
                        }
                    }

                    return logTypeMap.getOrDefault(type, logs);
                })
                .doOnError(err -> log().warn("Can't parse information from YarnUI log page " + url, err))
                .toBlocking()
                .firstOrDefault("");

        return typedLogs;
    }

    public final String getApplicationId() {
        return this.applicationId;
    }

    public final YarnCluster getCluster() {
        return this.cluster;
    }

    public URI getUri() {
        return URI.create(getYarnNMConnectUri().toString() + this.getApplicationId());
    }

    private HttpObservable getHttp() {
        return this.http;
    }

    private Observable<App> getYarnApplicationRequest() {
        URI uri = getUri();

        return getHttp()
//                .withUuidUserAgent()
                .get(uri.toString(), null, null, AppResponse.class)
                .map(Pair::getFirst)
                .map(AppResponse::getApp);
    }

    private Observable<List<AppAttempt>> getYarnApplicationAttemptsRequest() {
        URI uri = URI.create(String.format("%s/appattempts", getUri()));

        return getHttp()
//                .withUuidUserAgent()
                .get(uri.toString(), null, null, AppAttemptsResponse.class)
                .map(Pair::getFirst)
                .map(appAttemptsResponse -> appAttemptsResponse.getAppAttempts().appAttempt);
    }
}
