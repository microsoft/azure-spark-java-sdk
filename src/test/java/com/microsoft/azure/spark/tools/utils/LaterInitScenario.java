// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

package com.microsoft.azure.spark.tools.utils;

import com.microsoft.azure.spark.tools.errors.InitializedException;
import com.microsoft.azure.spark.tools.errors.NotInitializedException;
import cucumber.api.java.en.And;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;
import rx.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class LaterInitScenario {
    private final static Pattern stringRegex = Pattern.compile("^\"(.*)\"$");

    private LaterInit laterInitValue;
    private Exception caught;
    private AtomicBoolean isComplete = new AtomicBoolean(false);
    private AtomicReference<Object> actureValue = new AtomicReference<>();
    private Object valueGot;

    @When("^init a String type LaterInit$")
    public void initLaterInitString() {
        laterInitValue = new LaterInit<String>();
    }

    @Then("^check LaterInit's value (.*) should be got$")
    public void checkLaterInitValue(String expect) {
        assertEquals(parseString(expect), laterInitValue.getWithNull());
    }

    @Then("^check LaterInit's value just got should be (.*)$")
    public void checkLaterInitValueGot(String expect) {
        assertEquals(parseString(expect), valueGot);
    }

    @When("^set value (.*) to LaterInit$")
    public void setLaterInitValue(String value) {
        try {
            laterInitValue.set(parseString(value));
        } catch (InitializedException e) {
            caught = e;
        }
    }

    @When("^get LaterInit's value$")
    public void getLaterInitValue() {
        try {
            valueGot = laterInitValue.get();
        } catch (NotInitializedException e) {
            caught = e;
        }
    }

    @And("^set value (.*) to LaterInit if it is not set$")
    public void setLaterInitValueIfNull(String value) {
        laterInitValue.setIfNull(parseString(value));
    }


    @And("^prepare subscription of LaterInit observable$")
    public void prepareLaterInitSubscription() {
        assertFalse(isComplete.get());

        laterInitValue.observable()
                .subscribe(
                        value -> actureValue.set(value),
                        err -> fail("Shouldn't get any error, but got " + err),
                        () -> isComplete.set(true)
                );
    }

    @Then("^check LaterInit's observable should be pending$")
    public void checkLaterInitObservablePending() throws InterruptedException {
        assertFalse(isComplete.get());

        Subscription sub = laterInitValue.observable()
                .doOnUnsubscribe(() -> isComplete.set(true))
                .subscribe(
                        value -> fail("Shouldn't get any value"),
                        err -> fail("Shouldn't get any error, but got " + err),
                        () -> fail("Shouldn't complete")
                );

        Thread.sleep(200);

        sub.unsubscribe();
        assertTrue(isComplete.get());
        assertNull(laterInitValue.getWithNull());
    }

    @Then("^check LaterInit's observable onNext (.*) should be got$")
    public void checkLaterInitObservableOnNext(String expect) {
        assertTrue(isComplete.get());
        assertEquals(parseString(expect), actureValue.get());
    }

    @Then("^check LaterInit initialized exception thrown$")
    public void checkLaterInitInitilizedExceptionThrown() {
        assertEquals(InitializedException.class, caught.getClass());
    }

    @Then("^check LaterInit not initialized exception thrown$")
    public void checkLaterInitNotInitilizedExceptionThrown() {
        assertEquals(NotInitializedException.class, caught.getClass());
        assertNull(valueGot);
    }

    private Object parseString(String input) {
        Matcher stringMatcher = stringRegex.matcher(input.trim());

        if (input.equalsIgnoreCase("null")) {
            return null;
        } else if (stringMatcher.matches()) {
            return stringMatcher.group(1);
        }

        throw new RuntimeException("Unsupported value type to check: " + input);
    }
}
