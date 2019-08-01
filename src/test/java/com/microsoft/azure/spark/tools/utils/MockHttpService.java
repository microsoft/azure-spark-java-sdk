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
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
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
    public static String WIREMOCK_SSL_CERT_PUBLIC_KEY = "Sun RSA public key, 2048 bits\n" +
            "  modulus: 1616281159999743767002432523209340041847821214339609530132554918980559030678322788" +
            "308328436445345761589385826429423868823943077166929543729099618462943663813097850123395545232" +
            "985007928565879330303595925625352897604590688698640633331749303053422625328525772608635706869" +
            "720914691104497628607206687350939381619516420478302307873582835075271858922244224099641302297" +
            "236218479236997148862688517102979382188787197512248169095378456233047400728614720679541664549" +
            "045050751317489220182994070036742676715775907381776530891158250830220384186038820816709179936" +
            "9665611086691071592657494061212129654521104283864301320982025481461669\n" +
            "  public exponent: 65537";
    public static String WIREMOCK_SSL_CERT_TYPE = "ECDHE_RSA";

    private WireMockServer httpServerMock;

    private MockHttpService() {
    }

    public WireMockServer getServer() {
        return httpServerMock;
    }

    public int getPort() {
        return this.httpServerMock.port();
    }

    public int getHttpsPort() {
        return this.httpServerMock.httpsPort();
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

    public void stubHttps(String action, String path, int statusCode, String response) {
        WireMock.configureFor("https", "localhost", getHttpsPort());
        WireMock.stubFor(WireMock.request(
                action, WireMock.urlPathEqualTo(path))
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

    public String completeHttpsUrl(String absoluteUri) {
        return "https://localhost:" + getHttpsPort() + "/" + StringUtils.stripStart(absoluteUri, "/");
    }

    public static MockHttpService create() {
        MockHttpService mockHttpService = new MockHttpService();
        mockHttpService.httpServerMock = new WireMockServer(wireMockConfig().dynamicPort());

        mockHttpService.httpServerMock.start();

        return mockHttpService;
    }

    public static MockHttpService createHttps() {
        MockHttpService mockHttpService = new MockHttpService();

        WireMockConfiguration option = wireMockConfig().dynamicPort().dynamicHttpsPort();
        mockHttpService.httpServerMock = new WireMockServer(option);

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

    public static MockHttpService createForRecord(String className, String targetUrl, boolean isPersistent) {
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
        if (isPersistent) {
            mockServer.resetMappings();
        }

        mockServer.start();
        mockServer.startRecording(recordSpec()
                .forTarget(targetUrl)
                .captureHeader("Accept")
                .captureHeader("Content-Type", true)
                .captureHeader("X-Requested-By")
                .makeStubsPersistent(isPersistent)
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