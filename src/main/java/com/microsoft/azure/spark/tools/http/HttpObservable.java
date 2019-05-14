// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.microsoft.azure.spark.tools.log.Logger;
import com.microsoft.azure.spark.tools.utils.JsonConverter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.exceptions.Exceptions;

import java.io.IOException;
import java.net.UnknownServiceException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.microsoft.azure.spark.tools.http.status.HttpErrorStatus.classifyHttpError;
import static rx.exceptions.Exceptions.propagate;

public class HttpObservable implements Logger {
    private RequestConfig defaultRequestConfig;

    private String userAgentPrefix;


    private @Nullable String userAgent;

    private HeaderGroup defaultHeaders;

    private CookieStore cookieStore;

    private HttpContext httpContext;

    private final CloseableHttpClient httpClient;

    private final List<NameValuePair> defaultParameters = new ArrayList<>();


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
        HeaderGroup headerGroup = new HeaderGroup();

//        String loadingClass = this.getClass().getClassLoader().getClass().getName().toLowerCase();
//        this.userAgentPrefix = loadingClass.contains("intellij")
//                ? "Azure Toolkit for IntelliJ"
//                : (loadingClass.contains("eclipse")
//                        ? "Azure Toolkit for Eclipse"
//                        : "Azure HDInsight SDK HTTP RxJava client");
        this.userAgentPrefix = "";
        this.userAgent = userAgentPrefix;

        // set default headers
        headerGroup.setHeaders(new Header[] {
                new BasicHeader("Content-Type", "application/json"),
                new BasicHeader("User-Agent", userAgent),
        });

        this.cookieStore = new BasicCookieStore();
        this.httpContext = new BasicHttpContext();
        this.httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

        // Create global request configuration
        this.defaultRequestConfig = RequestConfig.custom()
                .setCookieSpec(CookieSpecs.DEFAULT)
                .setTargetPreferredAuthSchemes(Arrays.asList(
                        AuthSchemes.KERBEROS, AuthSchemes.DIGEST, AuthSchemes.BASIC))
                .setProxyPreferredAuthSchemes(Collections.singletonList(AuthSchemes.BASIC))
                .build();

        HttpClientBuilder clientBuilder = HttpClients.custom()
                .useSystemProperties()
                .setDefaultCookieStore(this.cookieStore)
//                .setSSLSocketFactory(createSSLSocketFactory())
                .setDefaultRequestConfig(this.defaultRequestConfig);

        if (StringUtils.isNotBlank(username) && password != null) {
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
            headerGroup.addHeader(
                    new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(encodedAuth)));
        }

        this.httpClient = clientBuilder.build();
        this.defaultHeaders = headerGroup;
    }

    /*
     * Getter / Setter
     */

    public String getUserAgentPrefix() {
        return userAgentPrefix;
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
        return httpClient;
    }

//    public HttpObservable setHttpClient(final CloseableHttpClient client) {
//        this.httpClient = client;
//
//        return this;
//    }


    public @Nullable Header[] getDefaultHeaders() throws IOException {
        return getDefaultHeaderGroup().getAllHeaders();
    }

//    public HttpObservable setDefaultHeader(final @Nullable Header defaultHeader) {
//        this.getDefaultHeaderGroup().updateHeader(defaultHeader);
//        return this;
//    }

    public HeaderGroup getDefaultHeaderGroup()  {
        return defaultHeaders;
    }

//  public HttpObservable setDefaultHeaderGroup(final @Nullable HeaderGroup defaultHeaderGroup) {
//      this.getDefaultHeaderGroup().setHeaders(defaultHeaderGroup == null ? null : defaultHeaderGroup.getAllHeaders());
//
//      return this;
//  }

    public HttpObservable setContentType(final String type) {
        this.getDefaultHeaderGroup().updateHeader(new BasicHeader("Content-Type", type));
        return this;
    }

    public HttpContext getHttpContext() {
        return httpContext;
    }


    public @Nullable String getUserAgent() {
        return userAgent;
    }

//    public HttpObservable setUserAgent(final @Nullable String ua) {
//        this.userAgent = ua;
//
//        // Update the default headers
//        return setDefaultHeader(new BasicHeader("User-Agent", ua));
//    }


    public List<NameValuePair> getDefaultParameters() {
        return defaultParameters;
    }

    /*
     * Helper functions
     */

    public static boolean isSSLCertificateValidationDisabled() {
        return false;

        // try {
        //     return DefaultLoader.getIdeHelper()
        //                         .isApplicationPropertySet(CommonConst.DISABLE_SSL_CERTIFICATE_VALIDATION)
        //             && Boolean.valueOf(DefaultLoader.getIdeHelper().getApplicationProperty(
        //                     CommonConst.DISABLE_SSL_CERTIFICATE_VALIDATION));
        // } catch (Exception ex) {
        //     // To fix exception in unit test
        //     return false;
        // }
    }

//    private SSLConnectionSocketFactory createSSLSocketFactory() {
//        TrustStrategy ts = ServiceManager.getServiceProvider(TrustStrategy.class);
//        SSLConnectionSocketFactory sslSocketFactory = null;
//
//        if (ts != null) {
//            try {
//                SSLContext sslContext = new SSLContextBuilder()
//                        .loadTrustMaterial(ts)
//                        .build();
//
//                sslSocketFactory = new SSLConnectionSocketFactory(sslContext,
//                        HttpObservable.isSSLCertificateValidationDisabled()
//                                ? NoopHostnameVerifier.INSTANCE
//                                : new DefaultHostnameVerifier());
//            } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
//                log().error("Prepare SSL Context for HTTPS failure. " + ExceptionUtils.getStackTrace(e));
//            }
//        }
//        return sslSocketFactory;
//    }

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
     * @param resp HTTP response, consumed as String content
     * @param clazz the target type to convert
     * @param <T> the target type
     * @return the specified type class instance
     */
    public <T> T convertJsonResponseToObject(final HttpResponse resp, final Class<T> clazz) {
        try {
            String body = resp.getMessage();
            T obj = JsonConverter.of(clazz).parseFrom(body);
            if (obj == null) {
                throw new UnknownServiceException("Unknown HTTP server response: " + body);
            }

            return obj;
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

    public <T> Observable<T> get(final String uri,
                                 final @Nullable List<NameValuePair> parameters,
                                 final @Nullable List<Header> addOrReplaceHeaders,
                                 final Class<T> clazz) {
        return requestWithHttpResponse(new HttpGet(uri), null, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public <T> Observable<T> put(final String uri,
                                 final @Nullable HttpEntity entity,
                                 final @Nullable List<NameValuePair> parameters,
                                 final @Nullable List<Header> addOrReplaceHeaders,
                                 final Class<T> clazz) {
        return requestWithHttpResponse(new HttpPut(uri), entity, parameters, addOrReplaceHeaders)
                .map(resp -> this.convertJsonResponseToObject(resp, clazz));
    }

    public <T> Observable<T> post(final String uri,
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

    public <T> Observable<T> patch(final String uri,
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
