/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 */

package com.microsoft.azure.spark.tools.utils

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.http.HttpHeader
import com.github.tomakehurst.wiremock.http.HttpHeaders
import groovy.text.SimpleTemplateEngine

class MockHttpService {
    val httpServerMock: WireMockServer = WireMockServer(wireMockConfig().dynamicPort())

    val port: Int
        get() = this.httpServerMock.port()

    val templateProperties: Map<String, String>
        get() = hashMapOf("port" to port.toString())

    fun stub(action: String, uri: String, statusCode: Int, response: String) {
        WireMock.configureFor(port)
        WireMock.stubFor(WireMock.request(
                                action, WireMock.urlEqualTo(uri))
                        .willReturn(WireMock.aResponse()
                                .withStatus(statusCode).withBody(normalizeResponse(response))))
    }

    fun stubWithHeader(action: String, uri: String, statusCode: Int, response: String, header: Map<String, String>) {
        WireMock.configureFor(port)
        WireMock.stubFor(WireMock.request(
                action, WireMock.urlEqualTo(uri))
                .willReturn(WireMock.aResponse()
                        .withStatus(statusCode)
                        .withHeaders(HttpHeaders(header.map { HttpHeader(it.key, it.value) }))
                        .withBody(normalizeResponse(response))))
    }

    fun stubWithBody(action: String, uri: String, body: String, statusCode: Int, response: String) {
        WireMock.configureFor(port)
        WireMock.stubFor(
            WireMock.request(action, WireMock.urlEqualTo(uri))
                .withRequestBody(WireMock.equalToJson(body))
                .willReturn(WireMock.aResponse()
                    .withStatus(statusCode)
                    .withBody(normalizeResponse(response))))
    }

    fun normalizeResponse(rawResponse: String): String {
        val engine = SimpleTemplateEngine()
        return engine.createTemplate(rawResponse).make(templateProperties).toString()
    }

    fun completeUrl(absoluteUri: String): String {
        return "http://localhost:$port/${absoluteUri.trimStart('/')}"
    }

    init {
        this.httpServerMock.start()
    }
}