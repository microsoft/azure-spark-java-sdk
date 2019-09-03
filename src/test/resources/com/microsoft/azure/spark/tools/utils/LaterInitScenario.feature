Feature: LaterInit unit tests

  Scenario: Just initialized LaterInit value should get null value
    When init a String type LaterInit
    Then check LaterInit's value null should be got
    When get LaterInit's value
    Then check LaterInit not initialized exception thrown

  Scenario: Just initialized LaterInit value should get null value
    When init a String type LaterInit
    And set value null to LaterInit
    Then check LaterInit's observable should be pending

  Scenario: Just initialized LaterInit observable should be pending
    When init a String type LaterInit
    Then check LaterInit's observable should be pending

  Scenario: Set normal value firstly should be success
    When init a String type LaterInit
    And set value "hello" to LaterInit
    When get LaterInit's value
    And prepare subscription of LaterInit observable
    Then check LaterInit's value "hello" should be got
    Then check LaterInit's value just got should be "hello"
    Then check LaterInit's observable onNext "hello" should be got
    Then check LaterInit's later subscriber onNext "hello" should be got

  Scenario: Set normal value firstly after preparing observable should be success
    When init a String type LaterInit
    And prepare subscription of LaterInit observable
    And set value "hello" to LaterInit
    Then check LaterInit's value "hello" should be got
    Then check LaterInit's observable onNext "hello" should be got

  Scenario: Set normal value twice will cause exception
    When init a String type LaterInit
    And set value "hello" to LaterInit
    And set value "world" to LaterInit
    Then check LaterInit initialized exception thrown

  Scenario: Set value if null
    When init a String type LaterInit
    And set value "hello" to LaterInit if it is not set
    Then check LaterInit's value "hello" should be got
    And set value "world" to LaterInit if it is not set
    Then check LaterInit's value "hello" should be got
