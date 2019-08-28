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
import org.apache.http.client.methods.HttpRequestBase;

import com.microsoft.azure.spark.tools.utils.MockHttpService;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OAuthTokenHttpObservableScenario {
    private HttpObservable httpMock;
    private MockHttpService httpServerMock;
    private HttpRequestBase httpRequest;

    @Before("@OAuthTokenHttpObservable")
    public void setUp() {
        this.httpServerMock = MockHttpService.create();
    }

    @After("@OAuthTokenHttpObservable")
    public void cleanUp() {
        this.httpServerMock.stop();
    }

    @Given("^setup a mock service with OAuth auth for '(.+)' request '(.*)' to return '(.*)'")
    public void setupMockService(String method, String path, String response) {
        this.httpServerMock.stub(method, path, 200, response);
    }

    @Given("^prepare OAuthTokenHttp '(.+)' request to '(.*)' with access token '(.*)'")
    public void prepareRequest(String method, String path, String accessToken) {
        this.httpMock = new OAuthTokenHttpObservable(accessToken);

        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }
        };

        this.httpRequest.setURI(URI.create(httpServerMock.completeUrl(path)));
    }

    @Then("^send and check OAuthTokenHttp '(.+)' request to '(.*)' should contains header '(.*):\\s*(.*)'$")
    public void checkRequestHeader(String method, String path, String expectHeaderKey, String expectHeaderValue) {
        this.httpMock.request(this.httpRequest, null, emptyList(), emptyList())
                .toBlocking()
                .subscribe();

        verify(RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlPathEqualTo(path))
            .withHeader(expectHeaderKey, equalTo(expectHeaderValue)));
    }

    @And("prepare OAuthTokenHttp {string} request to {string} with access token getting IOException {string}")
    public void prepareRequestWithGetAccessTokenError(String method, String path, String mockErrorMessage) {
        this.httpMock = new OAuthTokenHttpObservable(() -> { throw new IOException(mockErrorMessage); });
        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }

            @Override
            public URI getURI() {
                return URI.create(httpServerMock.completeUrl(path));
            }
        };
    }

    @And("prepare OAuthTokenHttp {string} request to {string} with access token getting RuntimeException {string}")
    public void prepareRequestWithGetAccessTokenRuntimeError(String method, String path, String mockErrorMessage) {
        this.httpMock = new OAuthTokenHttpObservable(() -> { throw new RuntimeException(mockErrorMessage); });
        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }

            @Override
            public URI getURI() {
                return URI.create(httpServerMock.completeUrl(path));
            }
        };
    }

    @Then("send and check OAuthTokenHttp {string} request to {string} should throw IOException with {string}")
    public void sendRequestAndCheckError(String method, String path, String expectedErrorMessage) {
        AtomicReference<Throwable> caught = new AtomicReference<>();

        this.httpMock.request(this.httpRequest, null, emptyList(), emptyList())
                .toBlocking()
                .subscribe(
                        data -> {},
                        caught::set
                    );

        assertTrue("The exception caught should be IOException", caught.get() instanceof IOException);
        assertEquals(expectedErrorMessage, caught.get().getMessage());

        // Request shouldn't be sent out
        verify(0, RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlEqualTo(path)));
    }

    @Then("send and check OAuthTokenHttp {string} request to {string} should throw RuntimeException with {string}")
    public void sendRequestAndCheckRuntimeError(String method, String path, String expectedErrorMessage) {
        AtomicReference<Throwable> caught = new AtomicReference<>();

        this.httpMock.request(this.httpRequest, null, emptyList(), emptyList())
                .toBlocking()
                .subscribe(
                        data -> {},
                        caught::set
                );

        assertTrue("The exception caught should be RuntimeException", caught.get() instanceof RuntimeException);
        assertEquals(expectedErrorMessage, caught.get().getMessage());

        // Request shouldn't be sent out
        verify(0, RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlEqualTo(path)));
    }

    @Then("check get OAuthTokenHttp default header should throw IOException with {string}")
    public void checkGetOAuthTokenHttpDefaultHeaderIOError(String expectErrorMessage) {
        Throwable caught = null;
        try {
            this.httpMock.getDefaultHeaders();
        } catch (Exception ex) {
            caught = ex;
        }

        assertTrue("The exception caught should be IOException", caught instanceof IOException);
        assertEquals(expectErrorMessage, caught.getMessage());

    }
}
