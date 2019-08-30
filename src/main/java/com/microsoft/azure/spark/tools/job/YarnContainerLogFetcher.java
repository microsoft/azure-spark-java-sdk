// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.job;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicNameValuePair;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import rx.Observable;
import rx.subjects.PublishSubject;

import com.microsoft.azure.spark.tools.clusters.YarnCluster;
import com.microsoft.azure.spark.tools.http.HttpObservable;
import com.microsoft.azure.spark.tools.http.HttpResponse;
import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.App;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttempt;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppAttemptsResponse;
import com.microsoft.azure.spark.tools.restapi.yarn.rm.AppResponse;
import com.microsoft.azure.spark.tools.utils.Pair;
import com.microsoft.azure.spark.tools.utils.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static rx.exceptions.Exceptions.propagate;

/**
 * The class is to support fetching Spark Driver log from Yarn application UI.
 */
public class YarnContainerLogFetcher implements SparkLogFetcher, Logger {
    private static final Pattern LOG_TYPE_PATTERN = Pattern.compile("Log Type:\\s+(?<type>\\S+)");

    private final HttpObservable http;

    /**
     * A {LogConversionMode} is an internal class to present Yarn log UI URI combining ways.
     */
    private class LogConversionMode {
        static final String HOST = "HOST";
        static final String PORT = "PORT";
        static final String PATH = "PATH";          // Internal URI path, starting with slash `/`
        static final String BASE = "BASE";          // Yarn UI Base URI, ending with slash `/`
        static final String ORIGINAL = "ORIGINAL";  // Original internal URI

        private final String name;
        private final String publicPathTemplate;

        LogConversionMode(final String name, final String publicPathTemplate) {
            this.name = name;
            this.publicPathTemplate = publicPathTemplate;
        }

        URI toPublic(final URI internalLogUrl) {
            final Map<String, String> values = ImmutableMap.of(
                    HOST, internalLogUrl.getHost(),
                    PORT, String.valueOf(internalLogUrl.getPort()),
                    PATH, Optional.of(internalLogUrl.getPath()).filter(StringUtils::isNoneBlank).orElse("/"),
                    BASE, UriUtils.normalizeWithSlashEnding(URI.create(getCluster().getYarnUIBaseUrl())).toString(),
                    ORIGINAL, internalLogUrl.toString());
            final StrSubstitutor sub = new StrSubstitutor(values);
            final String publicPath = sub.replace(publicPathTemplate);

            return URI.create(publicPath);
        }
    }

    private final Iterator<LogConversionMode> logConversionModes = Arrays.asList(
            new LogConversionMode("WITHOUT_PORT", "${BASE}${HOST}${PATH}"),
            new LogConversionMode("WITH_PORT", "${BASE}${HOST}/port/${PORT}${PATH}"),
            new LogConversionMode("ORIGINAL", "${ORIGINAL}")
    ).iterator();

    private final URI yarnNMConnectUri;
    
    private @Nullable String currentLogUrl;
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

    /**
     * Get the current Spark job Yarn application attempt log URI Observable.
     */
    Observable<URI> getSparkJobYarnCurrentAppAttemptLogsLink() {
        return this.getYarnApplicationAttemptsRequest()
                .map(appAttempts -> appAttempts.stream().max((o1, o2) -> Integer.compare(o1.getId(), o2.getId())))
                .flatMap(attemptOpt -> attemptOpt.map(it -> Observable.just(URI.create(it.getLogsLink())))
                                                 .orElse(Observable.empty()));
    }

    @Override
    public Observable<String> awaitLogAggregationDone() {
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
        return getRequest(uriProbe)
                .map(any -> true)
                .onErrorReturn(err -> false);
    }

    private Observable<URI> convertToPublicLogUri(final URI internalLogUri) {
        while (this.logConversionModes.hasNext()) {
            // Try next mode
            final LogConversionMode probeMode = this.logConversionModes.next();
            final URI uriProbe = probeMode.toPublic(internalLogUri);

            if (isUriValid(uriProbe).toBlocking().firstOrDefault(false)) {
                // Find usable one
                log().debug("The Yarn log URL conversion mode is {} with pattern {}",
                        probeMode.name, probeMode.publicPathTemplate);

                return Observable.just(uriProbe);
            }
        }

        // All modes were probed and all failed
        log().warn("Can't find conversion mode of Yarn " + getYarnNMConnectUri());
        return Observable.empty();
    }

    @Override
    public Observable<String> fetch(final String type, final long logOffset, final int size) {
        return this.getSparkJobDriverLogUrl()
                .map(Object::toString)
                .flatMap(logUrl -> {
                    final long offset = StringUtils.equalsIgnoreCase(logUrl, this.getCurrentLogUrl())
                            ? logOffset
                            : 0L;

                    this.setCurrentLogUrl(logUrl);

                    return this.getContentFromYarnLogDom(logUrl, type, offset, size);
                })
                .repeatWhen(completed -> getYarnApplicationRequest()
                        .filter(app -> isLogFetchable(app.getState()))
                        .flatMap(app -> completed)
                        .delay(1, TimeUnit.SECONDS))
                .first();
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

    private Observable<String> getContentFromYarnLogDom(final String baseUrl,
                                                        final String type,
                                                        final long start,
                                                        final int size) {
        final URI url = UriUtils.normalizeWithSlashEnding(baseUrl).resolve(type);

        final List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("start", Long.toString(start)));
        if (size > 0) {
            params.add(new BasicNameValuePair("size", Long.toString(size)));
        }

        return getRequest(url, params)
                .map(response -> {
                    try {
                        return response.getMessage();
                    } catch (IOException ignored) {
                        // The upstream requestWithHttpResponse() has already get message buffered.
                        throw propagate(new AssertionError("The upstream has got messages."));
                    }
                })
                .flatMap(html -> {
                    final String logs = parseLogsFromHtml(html).getOrDefault(type, StringUtils.EMPTY);

                    return StringUtils.isEmpty(logs) ? Observable.empty() : Observable.just(logs);
                })
                .doOnError(err -> log().warn("Can't parse information from YarnUI log page " + url, err));
    }

    Map<String, String> parseLogsFromHtml(final String webPage) {
        final Document doc = Jsoup.parse(webPage);
        final Elements elements = Optional.ofNullable(doc.getElementById("navcell"))
                .map(Element::nextElementSibling)
                .map(Element::children)
                .orElse(null);

        if (elements == null) {
            return emptyMap();
        }

        // Subject for log type found
        final PublishSubject<String> logTypeWindowOpenings = PublishSubject.create();

        return Observable.from(elements)
                .doOnNext(node -> {
                    if (StringUtils.equalsIgnoreCase(node.tagName(), "p")) {
                        // In history server, need to read log type paragraph in page
                        node.childNodes().stream()
                                .findFirst()
                                .map(this::findLogTypeDomNode)
                                .ifPresent(logTypeWindowOpenings::onNext);
                    }
                })
                .filter(node -> StringUtils.equalsIgnoreCase(node.tagName(), "pre"))
                // Classify elements as window by log types
                .window(logTypeWindowOpenings)
                // Only take the first `<pre>` element as log
                .flatMap(Observable::first)
                // Pair elements window with log type
                .withLatestFrom(logTypeWindowOpenings, Pair::of)
                // Only take non-empty logs
                .filter(nodesWithType -> !nodesWithType.getFirst().childNodes().isEmpty())
                // Take log type as key, the element content as log value
                .toMap(Pair::getSecond, nodeWithType -> String.valueOf(nodeWithType.getFirst().childNodes().get(0)))
                .toBlocking()
                .singleOrDefault(emptyMap());
    }

    private @Nullable String findLogTypeDomNode(Node node) {
        final Matcher matcher = LOG_TYPE_PATTERN.matcher(node.toString().trim());

        return matcher.matches() ? matcher.group("type") : null;
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
                .get(uri.toString(), emptyList(), emptyList(), AppResponse.class)
                .map(Pair::getFirst)
                .map(AppResponse::getApp);
    }

    private Observable<List<AppAttempt>> getYarnApplicationAttemptsRequest() {
        URI uri = URI.create(String.format("%s/appattempts", getUri()));

        return getHttp()
//                .withUuidUserAgent()
                .get(uri.toString(), emptyList(), emptyList(), AppAttemptsResponse.class)
                .map(Pair::getFirst)
                .map(appAttemptsResponse -> appAttemptsResponse.getAppAttempts().appAttempt);
    }

    private Observable<HttpResponse> getRequest(URI url, List<NameValuePair> params) {
        return getHttp()
                .requestWithHttpResponse(new HttpGet(url), null, params, emptyList())
                .doOnNext(response -> {
                    try {
                        log().debug("Get page from " + url + ", got " + response.getCode()
                                + " with " + response.getMessage());
                    } catch (IOException ignored) {
                        // The upstream requestWithHttpResponse() has already get message buffered.
                    }
                });
    }

    private Observable<HttpResponse> getRequest(URI url) {
        return getRequest(url, emptyList());
    }
}
