// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import cucumber.api.java.After;
import cucumber.api.java.Before;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import org.apache.http.client.methods.HttpRequestBase;
import org.mockito.Mockito;

import com.microsoft.azure.spark.tools.utils.MockHttpService;

import java.net.URI;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.when;

public class AzureHttpObservableScenario {
    private HttpObservable httpMock;
    private MockHttpService httpServerMock;
    private HttpRequestBase httpRequest;
    private String apiVersion;

    @Before("@AzureHttpObservableScenario")
    public void setUp() {
        this.httpServerMock = MockHttpService.create();
    }

    @After("@AzureHttpObservableScenario")
    public void cleanUp() {
        this.httpServerMock.stop();
    }

    @Given("^setup a mock service with Azure OAuth auth for '(.+)' request '(.*)' to return '(.*)'")
    public void setupMockService(String method, String path, String response) {
        this.httpServerMock.stub(method, path, 200, response);
    }

    @Given("^prepare AzureHttp '(.+)' request to '(.*)' with access token '(.+)' and API version '(.+)'")
    public void prepareRequest(String method, String path, String accessToken, String apiVersion) throws Throwable {
        AzureOAuthTokenFetcher tokenFetcher = Mockito.mock(AzureOAuthTokenFetcher.class);
        when(tokenFetcher.get()).thenReturn(accessToken);

        this.httpMock = new AzureHttpObservable(tokenFetcher, apiVersion);
        this.apiVersion = apiVersion;

        this.httpRequest = new HttpRequestBase() {
            @Override
            public String getMethod() {
                return method;
            }
        };

        this.httpRequest.setURI(URI.create(httpServerMock.completeUrl(path)));
    }

    @Then("^send and check AzureHttp '(.+)' request to '(.*)' should contains header '(.*):\\s*(.*)' and parameters$")
    public void checkRequestHeaderAndParameters(String method,
                                                String path,
                                                String expectHeaderKey,
                                                String expectHeaderValue,
                                                Map<String, String> expectParameters) {
        this.httpMock.request(this.httpRequest, null, emptyList(), emptyList())
                .toBlocking()
                .subscribe();

        RequestPatternBuilder builder = RequestPatternBuilder.newRequestPattern(RequestMethod.fromString(method), urlPathEqualTo(path))
                .withHeader(expectHeaderKey, equalTo(expectHeaderValue));

        expectParameters.forEach((k, v) -> builder.withQueryParam(k, equalTo(v)));

        verify(builder);
    }
}
