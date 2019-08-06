// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.exceptions.Exceptions;

import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.utils.JsonConverter;
import com.microsoft.azure.spark.tools.utils.Lazy;
import com.microsoft.azure.spark.tools.utils.Pair;
import com.microsoft.azure.spark.tools.utils.Versions;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.UnknownServiceException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.microsoft.azure.spark.tools.http.status.HttpErrorStatus.classifyHttpError;
import static com.microsoft.azure.spark.tools.utils.Configurations.BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY;
import static com.microsoft.azure.spark.tools.utils.Configurations.TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY;
import static rx.exceptions.Exceptions.propagate;

public class HttpObservable implements Logger {
    private RequestConfig defaultRequestConfig;

    private final List<String> userAgents = new ArrayList<>();

    private Lazy<HeaderGroup> defaultHeaders = new Lazy<>();

    private CookieStore cookieStore;

    private HttpContext httpContext;

    /**
     * Lazy initialized Http client.
     */
    private final Lazy<CloseableHttpClient> httpClient = new Lazy<>();

    private final List<NameValuePair> defaultParameters = new ArrayList<>();

    private final byte[] encodedAuth;

    /**
     * The SSL trust strategy for HTTPS connection, which can refer to platform implementations.
     */
    private @Nullable TrustStrategy trustStrategy = null;

    /*
     * Constructors
     */

    public HttpObservable() {
        this(null, null);
    }

    /**
     * Constructor with basic authentication.
     *
     * @param username Basic authentication user name
     * @param password Basic authentication password
     */
    public HttpObservable(final @Nullable String username, final @Nullable String password) {

//        String loadingClass = this.getClass().getClassLoader().getClass().getName().toLowerCase();
//        this.userAgentPrefix = loadingClass.contains("intellij")
//                ? "Azure Toolkit for IntelliJ"
//                : (loadingClass.contains("eclipse")
//                        ? "Azure Toolkit for Eclipse"
//                        : "Azure HDInsight SDK HTTP RxJava client");

        this.cookieStore = new BasicCookieStore();
        this.httpContext = new BasicHttpContext();

        // Create global request configuration
        this.defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.DEFAULT)
                .setTargetPreferredAuthSchemes(Arrays.asList(
                        AuthSchemes.KERBEROS, AuthSchemes.DIGEST, AuthSchemes.BASIC))
                .setProxyPreferredAuthSchemes(Collections.singletonList(AuthSchemes.BASIC))
                .build();

        if (StringUtils.isNotBlank(username) && password != null) {
            String auth = username + ":" + password;
            encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
        } else {
            encodedAuth = new byte[0];
        }

        this.userAgents.add(Versions.DEFAULT_USER_AGENT.get().toString());
    }

    /*
     * Getter / Setter
     */

    /**
     * The Http Client builder getter.
     *
     * @return instance of {@link HttpClientBuilder}
     */
    protected HttpClientBuilder getClientBuilder() {
        return HttpClients.custom()
                .useSystemProperties()
                .setDefaultCookieStore(getCookieStore())
                .setSSLSocketFactory(createSSLSocketFactory())
                .setDefaultRequestConfig(getDefaultRequestConfig());
    }

    public RequestConfig getDefaultRequestConfig() {
        return defaultRequestConfig;
    }

    public HttpObservable setDefaultRequestConfig(final RequestConfig config) {
        this.defaultRequestConfig = config;

        return this;
    }

    public CookieStore getCookieStore() {
        return cookieStore;
    }

    public HttpObservable setCookieStore(final CookieStore store) {
        this.cookieStore = store;

        return this;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient.getOrEvaluate(() -> getClientBuilder().build());
    }

    public Header[] getDefaultHeaders() throws IOException {
        return getDefaultHeaderGroup().getAllHeaders();
    }

    public HeaderGroup getDefaultHeaderGroup()  {
        return defaultHeaders.getOrEvaluate(() -> {
            HeaderGroup headerGroup = new HeaderGroup();

            // set default headers
            headerGroup.setHeaders(new Header[] {
                    new BasicHeader("Content-Type", "application/json"),
                    new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "*/*"),
            });

            if (StringUtils.isNotBlank(getUserAgent())) {
                headerGroup.addHeader(new BasicHeader("User-Agent", getUserAgent()));
            }

            if (encodedAuth.length > 0) {
                headerGroup.addHeader(new AuthorizationHeader.BasicAuthHeader(encodedAuth));
            }

            return headerGroup;
        });
    }

    public HttpObservable setContentType(final String type) {
        this.getDefaultHeaderGroup().updateHeader(new BasicHeader("Content-Type", type));
        return this;
    }

    public HttpContext getHttpContext() {
        return httpContext;
    }

    public String getUserAgent() {
        return String.join(" ", userAgents);
    }

    public HttpObservable addUserAgentEntity(UserAgentEntity uaEntity) {
        this.userAgents.add(uaEntity.toString());

        return this;
    }

    public List<NameValuePair> getDefaultParameters() {
        return defaultParameters;
    }

    /*
     * Helper functions
     */

    /**
     * Check if SSL Certificated validation is forced bypassed in System properties or not.
     * @see com.microsoft.azure.spark.tools.utils.Configurations#BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY
     *
     * @return is forced bypassed or not
     */
    public static boolean isSSLCertificateValidationDisabled() {
        String bypassSetting = System.getProperty(BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY);

        return Boolean.parseBoolean(bypassSetting);
    }

    /**
     * Check if SSL Trust All Strategy is forced enabled in System properties or not.
     * @see com.microsoft.azure.spark.tools.utils.Configurations#TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY
     *
     * @return is forced enabled or not
     */
    public static boolean isSSLTrustAllStrategyEnabled() {
        String bypassSetting = System.getProperty(TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY);

        return Boolean.parseBoolean(bypassSetting);
    }

    /**
     * Set external SSL trust strategy instance.
     *
     * @param externalTrustStrategy platform implemented strategy instance
     * @return {@link HttpObservable} instance for fluent chain calling
     */
    public HttpObservable setTrustStrategy(final TrustStrategy externalTrustStrategy) {
        this.trustStrategy = externalTrustStrategy;

        return this;
    }

    /**
     * Create a SSL socket factory for HTTPS connection.
     *
     * @return instance of {@link SSLConnectionSocketFactory}
     */
    private @Nullable SSLConnectionSocketFactory createSSLSocketFactory() {
        TrustStrategy ts = isSSLTrustAllStrategyEnabled()
                ? (chain, authType) -> true
                : this.trustStrategy;

        SSLConnectionSocketFactory sslSocketFactory = null;

        if (ts != null) {
            try {
                SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(ts)
                        .build();

                sslSocketFactory = new SSLConnectionSocketFactory(
                        sslContext,
                        HttpObservable.isSSLCertificateValidationDisabled()
                                ? NoopHostnameVerifier.INSTANCE
                                : new DefaultHostnameVerifier());
            } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
                log().error("Prepare SSL Context for HTTPS failure. " + ExceptionUtils.getStackTrace(e));
            }
        }

        return sslSocketFactory;
    }

    /**
     * Helper to convert the closeable stream good Http response (2xx) to String result.
     * If the response is bad, propagate a HttpResponseException
     *
     * @param closeableHttpResponse the source closeable stream
     * @return Http Response as String
     */
    public static Observable<HttpResponse> toStringOnlyOkResponse(
            final CloseableHttpResponse closeableHttpResponse) {
        return Observable.using(
                // Resource factory
                () -> closeableHttpResponse,
                // Observable factory
                streamResp -> {
                    try {
                        StatusLine status = streamResp.getStatusLine();

                        if (status.getStatusCode() >= 300) {
                            return Observable.error(classifyHttpError(streamResp));
                        }

                        HttpResponse messageGot = new HttpResponse(streamResp);
                        messageGot.getMessage();

                        return Observable.just(messageGot);
                    } catch (IOException e) {
                        return Observable.error(e);
                    }
                },
                // Resource dispose
                streamResp -> {
                    try {
                        streamResp.close();
                    } catch (IOException ignore) {
                        // The connection will be closed automatically after timeout,
                        // the exception in closing can be ignored.
                    }
                });
    }

    /**
     * Helper to convert the http response to a specified type.
     *
     * @param <T> the target type
     * @param resp HTTP response, consumed as String content
     * @param clazz the target type to convert
     * @return the specified type class instance
     */
    public <T> Pair<T, HttpResponse> convertJsonResponseToObject(final HttpResponse resp, final Class<T> clazz) {
        try {
            String body = resp.getMessage();
            T obj = JsonConverter.of(clazz).parseFrom(body);
            if (obj == null) {
                throw new UnknownServiceException("Unknown HTTP server response: " + body);
            }

            return Pair.of(obj, resp);
        } catch (IOException e) {
            throw propagate(e);
        }
    }

    /*
     * Core request
     */
    public Observable<CloseableHttpResponse> request(final HttpRequestBase httpRequest,
                                                     final @Nullable HttpEntity entity,
                                                     final @Nullable List<NameValuePair> parameters,
                                                     final @Nullable List<Header> addOrReplaceHeaders) {
        return Observable.fromCallable(() -> {
            URIBuilder builder = new URIBuilder(httpRequest.getURI());

            // Add parameters
            builder.setParameters(getDefaultParameters());

            if (parameters != null) {
                builder.addParameters(parameters);
            }

            httpRequest.setURI(builder.build());

            // Set the default headers and update Headers
            httpRequest.setHeaders(getDefaultHeaders());
            if (addOrReplaceHeaders != null) {
                addOrReplaceHeaders.forEach(httpRequest::setHeader);
            }

            // Set entity for non-entity
            if (httpRequest instanceof HttpEntityEnclosingRequestBase && entity != null) {
                ((HttpEntityEnclosingRequestBase) httpRequest).setEntity(entity);

                // Update the content type by entity
                httpRequest.setHeader(entity.getContentType());
            }

            return getHttpClient().execute(httpRequest, getHttpContext());
        });
    }

    /*
     * RESTful API operations with response conversion for specified type
     */
    public Observable<HttpResponse> requestWithHttpResponse(final HttpRequestBase httpRequest,
                                                            final @Nullable HttpEntity entity,
                                                            final @Nullable List<NameValuePair> parameters,
                                                            final @Nullable List<Header> addOrReplaceHeaders) {
        return request(httpRequest, entity, parameters, addOrReplaceHeaders)
                .flatMap(HttpObservable::toStringOnlyOkResponse);
    }

    public Observable<HttpResponse> head(final String uri,
                                         final List<NameValuePair> parameters,
                                         final List<Header> addOrReplaceHeaders) {
        return requestWithHttpResponse(new HttpHead(uri), null, parameters, addOrReplaceHeaders);
    }

    public <T> Observable<Pair<T, HttpResponse>> get(final String uri,
                                 final @Nullable List<NameValuePair> parameters,
                                 final @Nullable List<Header> addOrReplaceHeaders,
                                 final Class<T> clazz) {
        return requestWithHttpResponse(new HttpGet(uri), null, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public <T> Observable<Pair<T, HttpResponse>> put(final String uri,
                                 final @Nullable HttpEntity entity,
                                 final @Nullable List<NameValuePair> parameters,
                                 final @Nullable List<Header> addOrReplaceHeaders,
                                 final Class<T> clazz) {
        return requestWithHttpResponse(new HttpPut(uri), entity, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public <T> Observable<Pair<T, HttpResponse>> post(final String uri,
                                  final @Nullable HttpEntity entity,
                                  final @Nullable List<NameValuePair> parameters,
                                  final @Nullable List<Header> addOrReplaceHeaders,
                                  final Class<T> clazz) {
        return requestWithHttpResponse(new HttpPost(uri), entity, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public Observable<HttpResponse> delete(final String uri,
                                           final @Nullable List<NameValuePair> parameters,
                                           final @Nullable List<Header> addOrReplaceHeaders) {
        return requestWithHttpResponse(new HttpDelete(uri), null, parameters, addOrReplaceHeaders);
    }

    public <T> Observable<Pair<T, HttpResponse>> patch(final String uri,
                                   final @Nullable HttpEntity entity,
                                   final @Nullable List<NameValuePair> parameters,
                                   final @Nullable List<Header> addOrReplaceHeaders,
                                   final Class<T> clazz) {
        return requestWithHttpResponse(new HttpPatch(uri), entity, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public Observable<CloseableHttpResponse> executeReqAndCheckStatus(final HttpEntityEnclosingRequestBase req,
                                                                      final int validStatueCode,
                                                                      final List<NameValuePair> pairs) {
        return request(req, req.getEntity(), pairs, Arrays.asList(getDefaultHeaderGroup().getAllHeaders()))
                .doOnNext(
                        resp -> {
                            int statusCode = resp.getStatusLine().getStatusCode();
                            if (statusCode != validStatueCode) {
                                Exceptions.propagate(new UnknownServiceException(
                                        String.format("Exceute request with unexpected code %s and resp %s",
                                                statusCode, resp)));
                            }
                        }
                );
    }
}
