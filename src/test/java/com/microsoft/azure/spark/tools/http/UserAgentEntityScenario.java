// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.http;

import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;

import java.util.Map;

import static org.junit.Assert.*;

public class UserAgentEntityScenario {
    private UserAgentEntity uaMock = null;
    private Exception exCaught = null;

    @Given("^User Agent Entity name `([^`]+)`")
    public void buildUAWithEntityName(String nameMock) {
        try {
            uaMock = new UserAgentEntity.Builder(nameMock).build();
        } catch (Exception ex) {
            exCaught = ex;
        }
    }

    @Then("check the exception type `(.*)` and message `(.*)` should be caught in User Agent build")
    public void checkExceptionCaughtInUABuild(String exTypeExpect, String exMessageExpect) {
        assertEquals(exCaught.getClass().getTypeName(), exTypeExpect);
        assertEquals(exCaught.getMessage(), exMessageExpect);
    }

    @Then("check the User Agent toString result should be `(.*)`")
    public void checkTheUserAgentToStringResultShouldBe(String expect) {
        assertEquals(expect, uaMock.toString());
    }

    @Given("User Agent Entity name `([^`]+)` and comment `([^`]*)`")
    public void userAgentEntityNameAndComment(String nameMock, String commentMock) {
        uaMock = new UserAgentEntity.Builder(nameMock).comment(commentMock).build();
    }

    @Given("User Agent Entity name `([^`]+)` and version `([^`]*)`")
    public void userAgentEntityNameAndVersion(String nameMock, String verMock) {
        uaMock = new UserAgentEntity.Builder(nameMock).version(verMock).build();
    }

    @Given("User Agent Entity name `([^`]+)` and version `([^`]*)` and comment `([^`]*)`")
    public void userAgentEntityNameAndVersionAndComment(String nameMock, String verMock, String commentMock) {
        uaMock = new UserAgentEntity.Builder(nameMock).version(verMock).comment(commentMock).build();
    }

    @Given("User Agent Entity name `([^`]+)` and version `([^`]*)` and comments:")
    public void userAgentEntityNameAndVersionAndComments(String nameMock, String verMock, Map<String, String> kvMocks) {
        try {
            UserAgentEntity.Builder builder = new UserAgentEntity.Builder(nameMock).version(verMock);

            kvMocks.forEach((key, value) -> {
                if (key.equals("<__blank__>")) {
                    builder.comment("  ", value);
                } else {
                    builder.comment(key, value);
                }
            });

            uaMock = builder.build();
        } catch (Exception ex) {
            exCaught = ex;
        }
    }
}
