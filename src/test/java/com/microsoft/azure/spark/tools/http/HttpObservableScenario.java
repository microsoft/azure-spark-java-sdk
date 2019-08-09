// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;


import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.joda.time.DateTime;
import org.joda.time.Period;

import com.microsoft.azure.spark.tools.utils.Configurations;
import com.microsoft.azure.spark.tools.utils.MockHttpService;

import javax.net.ssl.SSLException;
import java.net.URI;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.microsoft.azure.spark.tools.utils.MockHttpService.WIREMOCK_SSL_CERT_PUBLIC_KEY;
import static com.microsoft.azure.spark.tools.utils.MockHttpService.WIREMOCK_SSL_CERT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpObservableScenario {
    private HttpObservable httpMock;
    private MockHttpService httpServerMock = null;
    private HttpRequestBase httpRequest;
    private CookieStore cookieStore;
    private MockHttpService httpsServerMock;
    private boolean isMockTSInvoked = false;

    @Before("@HttpObservableScenario")
    public void setUp() {
        this.httpServerMock = MockHttpService.create();
        this.httpsServerMock = MockHttpService.createHttps();
        this.cookieStore = new BasicCookieStore();
    }

    @After("@HttpObservableScenario")
    public void cleanUp() {
        this.httpServerMock.stop();
        this.httpsServerMock.stop();
    }

    @Given("^setup a basic Http mock service for '(.+)' request '(.*)' to return '(.*)'")
    public void setupMockService(String method, String path, String response) {
        this.httpServerMock.stub(method, path, 200, response);
    }

    @Given("^setup a basic Https mock service for '(.+)' request '(.*)' to return '(.*)'")
    public void setupMockHttpsService(String method, String path, String response) {
        this.httpsServerMock.stubHttps(method, path, 200, response);
    }

    @Given("^prepare Http '(.+)' request to '(.*)' with username '(.+)' and password '(.+)'")
    public void prepareRequest(String method, String path, String username, String password) throws Throwable {
        this.httpMock = new HttpObservable(username, password);

        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }
        };

        this.httpRequest.setURI(URI.create(httpServerMock.completeUrl(path)));
    }

    @Given("^prepare Https '(.+)' request to '(.*)' with username '(.+)' and password '(.+)'")
    public void prepareHttpsRequest(String method, String path, String username, String password) throws Throwable {
        this.httpMock = new HttpObservable(username, password);

        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }
        };

        this.httpRequest.setURI(URI.create(httpsServerMock.completeHttpsUrl(path)));
    }

    @Given("^add a cookie '(.*):(.*)' into cookie store")
    public void mockCookieStore(String name, String value) {
        BasicClientCookie cookie = new BasicClientCookie(name, value);
        cookie.setExpiryDate(DateTime.now().plus(Period.days(1)).toDate());
        cookie.setDomain("localhost");
        cookieStore.addCookie(cookie);

        this.httpMock.setCookieStore(this.cookieStore);
    }

    @Then("^send and check Http '(.+)' request to '(.*)' should contain headers$")
    public void checkRequestHeaderAndParameters(String method,
                                                String path,
                                                Map<String, String> expectHeaders) {
        this.httpMock.request(this.httpRequest, null, null, null )
                .toBlocking()
                .subscribe();

        RequestPatternBuilder builder = RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlPathEqualTo(path));

        expectHeaders.forEach((k, v) -> builder.withHeader(k, equalTo(v)));

        verify(builder);
    }

    @Then("^send and check Https '(.+)' request to '(.*)' should contain headers$")
    public void checkHttpsRequestHeaderAndParameters(String method,
                                                String path,
                                                Map<String, String> expectHeaders) {
        this.httpMock.request(this.httpRequest, null, null, null )
                .toBlocking()
                .subscribe();

        RequestPatternBuilder builder = RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlPathEqualTo(path));

        expectHeaders.forEach((k, v) -> builder.withHeader(k, equalTo(v)));

        verify(builder);
    }

    @Then("send and check Https {string} request to {string} should throw CertificateException {string}")
    public void checkHttpsRequestValidatorException(String method,
                                                    String path,
                                                    String expectErrorMessage) {
        AtomicReference<Throwable> caught = new AtomicReference<>();

        this.httpMock.request(this.httpRequest, null, null, null)
                .toBlocking()
                .subscribe(
                        data -> {},
                        caught::set
                );


        assertTrue(
                "The exception caught was not a CertificateException: " + ExceptionUtils.getMessage(caught.get()),
                caught.get().getCause() instanceof CertificateException);
        assertEquals(expectErrorMessage, caught.get().getCause().getMessage());
    }

    @Then("send and check Https {string} request to {string} should throw SSLException {string}")
    public void checkHttpsRequestSSLException(String method,
                                                    String path,
                                                    String expectErrorMessage) {
        AtomicReference<Throwable> caught = new AtomicReference<>();

        this.httpMock.request(this.httpRequest, null, null, null)
                .toBlocking()
                .subscribe(
                        data -> {},
                        caught::set
                );


        assertTrue(
                "The exception caught was not SSLException: " + ExceptionUtils.getMessage(caught.get()),
                caught.get() instanceof SSLException);
        assertEquals(expectErrorMessage, caught.get().getMessage());
    }

    @And("^set mocked Trust Strategy$")
    public void setMockedTrustStrategy() {
        TrustStrategy mockTS = (chain, authType) -> {
            assertEquals(WIREMOCK_SSL_CERT_PUBLIC_KEY, chain[0].getPublicKey().toString());
            assertEquals(WIREMOCK_SSL_CERT_TYPE, authType);

            this.isMockTSInvoked = true;
            return true;
        };

        this.httpMock.setTrustStrategy(mockTS);
    }

    @Then("^check the mocked Trust Strategy is invoked$")
    public void checkMockedTrustStrategyIsInvoked() {
        assertTrue("The mocked TrustStrategy is not invoked", this.isMockTSInvoked);
    }

    @And("^set Trust All Strategy is (enabled|disabled)$")
    public void setTrustAllStrategy(String isEnabled) {
        if (StringUtils.equalsIgnoreCase("enabled", isEnabled)) {
            System.setProperty(Configurations.TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY, "true");
        } else {
            System.setProperty(Configurations.TRUST_LIVY_SSL_ALL_HOST_STRATEGY_PROPERTY, "false");
        }
    }

    @And("^set SSL validation bypass is (enabled|disabled)$")
    public void setSSLValidationBypass(String isEnabled) {
        if (StringUtils.equalsIgnoreCase("enabled", isEnabled)) {
            System.setProperty(Configurations.BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY, "true");
        } else {
            System.setProperty(Configurations.BYPASS_LIVY_SSL_CERTIFICATE_VALIDATION_PROPERTY, "false");
        }
    }
}
