@LivySparkBatchScenario
Feature: LivySparkBatch unit tests

  Scenario: getSparkJobApplicationId integration test with mocked Livy server
    Given setup a mock Livy service for GET request '/batch/9' to return '{"id":9,"state":"starting","appId":"application_1492415936046_0015","appInfo":{"driverLogUrl":"https://spkdbg.azurehdinsight.net/yarnui/10.0.0.15/node/containerlogs/container_e02_1492415936046_0015_01_000001/livy","sparkUiUrl":"https://spkdbg.azurehdinsight.net/yarnui/hn/proxy/application_1492415936046_0015/"},"log":["\\t ApplicationMaster RPC port: -1","\\t queue: default","\\t start time: 1492569369011","\\t final status: UNDEFINED","\\t tracking URL: https://spkdbg.azurehdinsight.net/yarnui/hn/proxy/application_1492415936046_0015/","\\t user: livy","17/04/19 02:36:09 INFO ShutdownHookManager: Shutdown hook called","17/04/19 02:36:09 INFO ShutdownHookManager: Deleting directory /tmp/spark-1984dc9d-acd4-4648-9104-398431590f8e","YARN Diagnostics:","AM container is launched, waiting for AM container to Register with RM"]}' with status code 200
    And mock Spark job connect URI to be 'http://localhost:$port/batch/'
    And mock Spark job batch id to 9
    Then getting spark job application id should be 'application_1492415936046_0015'

  Scenario: getSparkJobApplicationId negative integration test with broken Livy response
    Given setup a mock Livy service for GET request '/batch/9' to return '{"id":9,' with status code 200
    And mock Spark job connect URI to be 'http://localhost:$port/batch/'
    And mock Spark job batch id to 9
    Then getting spark job application id, '/batch/9' should be got with 3 times retried

  Scenario: getSparkJobApplicationId retries test with mocked Livy server
    Given setup a mock Livy service for GET request '/batch/9' to return '{}' with status code 404
    And mock Spark job connect URI to be 'http://localhost:$port/batch/'
    And mock Spark job batch id to 9
    Then getting spark job application id, '/batch/9' should be got with 3 times retried

  Scenario: await Spark job is done behavior
    Given setup a mock Livy service with the following scenario 'awaitJobIsDoneUT'
      | ACTION | URI      | RESPONSE_STATUS | RESPONSE_BODY                     | PREV_STATE | NEXT_STATE |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "not_started"} | Started    | starting_1 |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "starting"}    | starting_1 | starting_2 |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "starting"}    | starting_2 | starting_3 |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "running"}     | starting_3 | running_1  |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "running"}     | running_1  | running_2  |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "success"}     | running_2  | success_1  |
      | GET    | /batch/9 | 200             | {"id": 9, "state": "dead"}        | success_1  | end        |
    And mock Spark job connect URI to be 'http://localhost:$port/batch/'
    And mock Spark job batch id to 9
    Then await Livy Spark job done should get state 'success'

  Scenario: parseLivyLogs unit tests
    Given parse Livy Logs from the following
      | stdout:  |
      | one      |
      | two      |
      | \\nstderr: |
      | \\nYARN Diagnostics: |
    Then check parsed Livy logs stdout should be
      | stdout:  |
      | one      |
      | two      |
    Then check parsed Livy logs stderr should be
      | \\nstderr: |
    Then check parsed Livy logs yarn diagnostics should be
      | \\nYARN Diagnostics: |
    Given parse Livy Logs from the following
      | one      |
      | two      |
      | \\nstderr: |
      | err 1      |
      | \\nYARN Diagnostics: |
      | error log 1          |
      | error log 2          |
    Then check parsed Livy logs stdout should be
      | one      |
      | two      |
    Then check parsed Livy logs stderr should be
      | \\nstderr: |
      | err 1      |
    Then check parsed Livy logs yarn diagnostics should be
      | \\nYARN Diagnostics: |
      | error log 1          |
      | error log 2          |
