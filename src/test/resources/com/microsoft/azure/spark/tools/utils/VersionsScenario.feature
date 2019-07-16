Feature: Versions unit tests

  Scenario: Static functions return values check
    Then Versions should not be `UNKNOWN_VERSION`
    Then Production ID should be `azure-spark-java-sdk`
