/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 */

package com.microsoft.azure.spark.tools.utils;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.common.SingleRootFileSource;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.recording.RecordingStatus;
import com.github.tomakehurst.wiremock.standalone.JsonFileMappingsSource;
import com.github.tomakehurst.wiremock.standalone.MappingsSource;
import groovy.text.SimpleTemplateEngine;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.github.tomakehurst.wiremock.core.WireMockApp.FILES_ROOT;
import static com.github.tomakehurst.wiremock.core.WireMockApp.MAPPINGS_ROOT;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class MockHttpService {
    private WireMockServer httpServerMock;

    private MockHttpService() {
    }

    public WireMockServer getServer() {
        return httpServerMock;
    }

    public int getPort() {
        return this.httpServerMock.port();
    }

    private Map<String, String> getTemplateProperties() {
        return new HashMap<String, String>() {{
            put("port", Integer.toString(getPort()));
        }};
    }

    public void stub(String action, String uri, int statusCode, String response) {
        WireMock.configureFor(getPort());
        WireMock.stubFor(WireMock.request(
                                action, WireMock.urlEqualTo(uri))
                        .willReturn(WireMock.aResponse()
                                .withStatus(statusCode).withBody(normalizeResponse(response))));
    }

    public void stubWithHeader(String action, String uri, int statusCode, String response, Map<String, String> header) {
        WireMock.configureFor(getPort());
        WireMock.stubFor(WireMock.request(
                action, WireMock.urlEqualTo(uri))
                .willReturn(WireMock.aResponse()
                        .withStatus(statusCode)
                        .withHeaders(new HttpHeaders(
                                header.entrySet()
                                      .stream()
                                      .map(it -> new HttpHeader(it.getKey(), it.getValue()))
                                      .collect(Collectors.toList())))
                        .withBody(normalizeResponse(response))));
    }

    public void stubWithBody(String action, String uri, String body, int statusCode, String response) {
        WireMock.configureFor(getPort());
        WireMock.stubFor(
            WireMock.request(action, WireMock.urlEqualTo(uri))
                .withRequestBody(WireMock.equalToJson(body))
                .willReturn(WireMock.aResponse()
                    .withStatus(statusCode)
                    .withBody(normalizeResponse(response))));
    }

    public String normalizeResponse(String rawResponse) {
        SimpleTemplateEngine engine = new SimpleTemplateEngine();
        try {
            return engine.createTemplate(rawResponse).make(getTemplateProperties()).toString();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String completeUrl(String absoluteUri) {
        return "http://localhost:" + getPort() + "/" + StringUtils.stripStart(absoluteUri, "/");
    }

    public static MockHttpService create() {
        MockHttpService mockHttpService = new MockHttpService();
        mockHttpService.httpServerMock = new WireMockServer(wireMockConfig().dynamicPort());

        mockHttpService.httpServerMock.start();

        return mockHttpService;
    }

    private static final String RECORDINGS_ROOT = "src/test/resources/recordings/";

    public static MockHttpService createFromSaving(String className) {
        MockHttpService mockHttpService = new MockHttpService();
        FileSource fileSource = new SingleRootFileSource(RECORDINGS_ROOT + className);
        MappingsSource mappingsFileSource = new JsonFileMappingsSource(fileSource.child(MAPPINGS_ROOT));

        mockHttpService.httpServerMock = new WireMockServer(wireMockConfig()
                .dynamicPort()
                .fileSource(fileSource)
                .mappingSource(mappingsFileSource)
                .notifier(new Slf4jNotifier(true))
        );

        mockHttpService.httpServerMock.start();

        return mockHttpService;
    }

    public static MockHttpService createForRecord(String className, String targetUrl) {
        MockHttpService mockHttpService = new MockHttpService();
        FileSource fileSource = new SingleRootFileSource(RECORDINGS_ROOT + className);
        FileSource filesFileSource = fileSource.child(FILES_ROOT);
        filesFileSource.createIfNecessary();
        FileSource mappingsFileSource = fileSource.child(MAPPINGS_ROOT);
        mappingsFileSource.createIfNecessary();

        WireMockServer mockServer = new WireMockServer(options()
                .fileSource(fileSource)
                .mappingSource(new JsonFileMappingsSource(mappingsFileSource))

                // uncomment for debugging with console output
                // .notifier(new ConsoleNotifier(true))

                // uncomment for debugging with local proxy
                // .proxyVia("localhost", 8888)
        );

        // Clean up all history recordings
        mockServer.resetMappings();

        mockServer.start();
        mockServer.startRecording(recordSpec()
                .forTarget(targetUrl)
                .captureHeader("Accept")
                .captureHeader("Content-Type", true)
                .captureHeader("X-Requested-By")
                .makeStubsPersistent(true)
                .build()
        );

        try {
            while (mockServer.getRecordingStatus().getStatus() != RecordingStatus.Recording) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        mockHttpService.httpServerMock = mockServer;

        return mockHttpService;
    }
}