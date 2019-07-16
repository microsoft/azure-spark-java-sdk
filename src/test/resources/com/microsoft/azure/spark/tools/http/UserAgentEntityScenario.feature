Feature: UserAgentEntity unit tests

  Scenario: Build UA product name only
    Given User Agent Entity name `prodMock`
    Then check the User Agent toString result should be `prodMock`

  Scenario: Build UA product blank negative case
    Given User Agent Entity name `  `
    Then check the exception type `java.lang.IllegalArgumentException` and message `Product can't be blank.` should be caught in User Agent build

  Scenario: Build UA product name and comment
    Given User Agent Entity name `prodMock` and comment `commentMock`
    Then check the User Agent toString result should be `prodMock (commentMock)`

  Scenario: Build UA product name with version
    Given User Agent Entity name `prodMock` and version `verMock`
    Then check the User Agent toString result should be `prodMock/verMock`

  Scenario: Build UA with version and a comment
    Given User Agent Entity name `prodMock` and version `verMock` and comment `commentMock`
    Then check the User Agent toString result should be `prodMock/verMock (commentMock)`

  Scenario: Build UA with version and key-value comments
    Given User Agent Entity name `prodMock` and version `verMock` and comments:
      | k1 | v1 |
      | k2 | v2 |
    Then check the User Agent toString result should be `prodMock/verMock (k1:v1; k2:v2)`

  Scenario: Build UA with version and blank key-value comments
    Given User Agent Entity name `prodMock` and version `verMock` and comments:
      | <__blank__> |    |
    Then check the exception type `java.lang.IllegalArgumentException` and message `key and value are not allowed to be blank.` should be caught in User Agent build

  Scenario: Build UA with version and blank key comments
    Given User Agent Entity name `prodMock` and version `verMock` and comments:
      | <__blank__> | v1 |
    Then check the exception type `java.lang.IllegalArgumentException` and message `key and value are not allowed to be blank.` should be caught in User Agent build

  Scenario: Build UA with version and blank value comments
    Given User Agent Entity name `prodMock` and version `verMock` and comments:
      | k1 |   |
    Then check the exception type `java.lang.IllegalArgumentException` and message `key and value are not allowed to be blank.` should be caught in User Agent build
