Feature: YarnSparkApplicationDriverLog unit tests
  Scenario: parsingAmHostHttpAddressHost unit tests
    Given prepare a Yarn cluster with Node Manager base URL https://127.0.0.1:$port/yarnui/hn/ and UI base URL http://127.0.0.1:$port/yarnui/
    And create a yarn application driver with id application_1492415936046_0015
    Then Parsing driver HTTP address 'host.domain.com:8042' should get host 'host.domain.com'
    Then Parsing driver HTTP address '10.0.0.15:30060' should get host '10.0.0.15'
    Then Parsing driver HTTP address '10.0.0.15:' should be null
    Then Parsing driver HTTP address ':1234' should be null

  Scenario: getSparkDriverHost integration test with responses
    Given prepare a Yarn cluster with Node Manager base URL http://127.0.0.1:$port/yarnui/ws/v1/cluster/apps/ and UI base URL http://127.0.0.1:$port/yarnui/
    And create a yarn application driver with id application_1492415936046_0015
    And setup a mock Yarn service for GET request '/yarnui/ws/v1/cluster/apps/application_1492415936046_0015' to return '{ "app": { "amNodeLabelExpression": "", "finishedTime": 1493100184345, "startedTime": 1493097873053, "priority": 0, "applicationTags": "livy-batch-15-bcixsxv0", "applicationType": "SPARK", "clusterId": 1492780173422, "diagnostics": "Application application_1492780173422_0003 failed 5 times due to ApplicationMaster for attempt appattempt_1492780173422_0003_000005 timed out. Failing the application.", "trackingUrl": "http://hn0-zhwe-s.uhunwunss5gupibv1jib3beicb.lx.internal.cloudapp.net:8088/cluster/app/application_1492780173422_0003", "id": "application_1492780173422_0003", "user": "livy", "name": "SparkCore_WasbIOTest", "queue": "default", "state": "ACCEPTED", "finalStatus": "ACCEPTED", "progress": 100, "trackingUI": "History", "elapsedTime": 2311292, "amContainerLogs": "http://10.0.0.15:30060/node/containerlogs/container_e03_1492780173422_0003_05_000001/livy", "amHostHttpAddress": "10.0.0.15:30060", "allocatedMB": -1, "allocatedVCores": -1, "runningContainers": -1, "memorySeconds": 3549035, "vcoreSeconds": 2308, "queueUsagePercentage": 0, "clusterUsagePercentage": 0, "preemptedResourceMB": 0, "preemptedResourceVCores": 0, "numNonAMContainerPreempted": 0, "numAMContainerPreempted": 0, "logAggregationStatus": "SUCCEEDED", "unmanagedApplication": false }}' with status code 200
    Then getting Spark driver host should be '10.0.0.15'

  Scenario: getSparkJobYarnCurrentAppAttempt integration test with response
    Given prepare a Yarn cluster with Node Manager base URL http://127.0.0.1:$port/yarnui/ws/v1/cluster/apps/ and UI base URL http://127.0.0.1:$port/yarnui/
    And create a yarn application driver with id application_1513565654634_0011
    And setup a mock Yarn service for GET request '/yarnui/ws/v1/cluster/apps/application_1513565654634_0011/appattempts' to return '{"appAttempts":{"appAttempt":[{"id":1,"startTime":1513673984219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000001","nodeHttpAddress":"10.0.0.6:30060","nodeId":"10.0.0.6:30050","logsLink":"http://10.0.0.6:30060/node/containerlogs/container_1513565654634_0011_01_000001/livy","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000001"},{"id":2,"startTime":1513673985219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000002","nodeHttpAddress":"10.0.0.7:30060","nodeId":"10.0.0.7:30050","logsLink":"http://10.0.0.7:30060/node/containerlogs/container_1513565654634_0011_01_000002/livy","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000002"}]}}' with status code 200
    Then getting current Yarn App attempt should be 'http://10.0.0.7:30060/node/containerlogs/container_1513565654634_0011_01_000002/livy'
    When setup a mock Yarn service for GET request '/yarnui/10.0.0.7/node/containerlogs/container_1513565654634_0011_01_000002/livy' to return '{}' with status code 200
    Then getting Spark Job driver log URL Observable should be 'http://127.0.0.1:$port/yarnui/10.0.0.7/node/containerlogs/container_1513565654634_0011_01_000002/livy'

  Scenario: getSparkJobDriverLogUrlObservable unit test with mode selection
    Given prepare a Yarn cluster with Node Manager base URL http://127.0.0.1:$port/yarnui/ws/v1/cluster/apps/ and UI base URL http://127.0.0.1:$port/yarnui/
    And create a yarn application driver with id application_1513565654634_0011
    And setup a mock Yarn service for GET request '/yarnui/ws/v1/cluster/apps/application_1513565654634_0011/appattempts' to return '{"appAttempts":{"appAttempt":[{"id":1,"startTime":1513673984219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000001","nodeHttpAddress":"10.0.0.6:30060","nodeId":"10.0.0.6:30050","logsLink":"http://10.0.0.6:30060/node/containerlogs/container_1513565654634_0011_01_000001/livy","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000001"},{"id":2,"startTime":1513673985219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000002","nodeHttpAddress":"10.0.0.7:30060","nodeId":"10.0.0.7:30050","logsLink":"http://10.0.0.7:30060/node/containerlogs/container_1513565654634_0011_01_000002/livy","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000002"}]}}' with status code 200
    Then getting current Yarn App attempt should be 'http://10.0.0.7:30060/node/containerlogs/container_1513565654634_0011_01_000002/livy'
    When setup a mock Yarn service for GET request '/yarnui/10.0.0.7/node/containerlogs/container_1513565654634_0011_01_000002/livy' to return '{}' with status code 404
    When setup a mock Yarn service for GET request '/yarnui/10.0.0.7/port/30060/node/containerlogs/container_1513565654634_0011_01_000002/livy' to return '{}' with status code 200
    Then getting Spark Job driver log URL Observable should be 'http://127.0.0.1:$port/yarnui/10.0.0.7/port/30060/node/containerlogs/container_1513565654634_0011_01_000002/livy'

  Scenario: getSparkJobDriverLogUrlObservable unit test for failure
    Given prepare a Yarn cluster with Node Manager base URL http://127.0.0.1:$port/yarnui/ws/v1/cluster/apps/ and UI base URL http://127.0.0.1:$port/yarnui/
    And create a yarn application driver with id application_1513565654634_0011
    And setup a mock Yarn service for GET request '/yarnui/ws/v1/cluster/apps/application_1513565654634_0011/appattempts' to return '{"appAttempts":{"appAttempt":[{"id":1,"startTime":1513673984219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000001","nodeHttpAddress":"10.0.0.6:30060","nodeId":"10.0.0.6:30050","logsLink":"http://10.0.0.6:30060/node/containerlogs/container_1513565654634_0011_01_000001/livy","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000001"},{"id":2,"startTime":1513673985219,"finishedTime":0,"containerId":"container_1513565654634_0011_01_000002","nodeHttpAddress":"10.0.0.7:30060","nodeId":"10.0.0.7:30050","logsLink":"","blacklistedNodes":"","appAttemptId":"appattempt_1513565654634_0011_000002"}]}}' with status code 200
    Then getting Spark Job driver log URL Observable should be empty
