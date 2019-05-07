Feature: PostBatches unit test

  Scenario: Private function getPostBatchesMap
    Given create PostBatches with the following job config
      | driverMemory | 2G |
    And mock file to fFilePath
    And mock className to fakeClassName
    And apply spark configs
      | spark.driver.extraJavaOptions | -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6006 |
    Then the parameter map should include key 'conf' with value '{spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6006}'

  Scenario: serializeToJson would serialize the conf map well
    Given mock args to farg1,farg2
    And mock file to fFilePath
    And mock reference files to ffile1,ffile2
    And mock reference jars to fjar1,fjar2
    And mock className to fakeClassName
    And mock application name to fakeAppName
    And create PostBatches with the following job config
      | driverMemory | 2G |
      | driverCores  | 2  |
      | executorMemory | 1G |
      | executorCores  | 2  |
      | numExecutors   | 7  |
    Then the serialized JSON should be '{"name": "fakeAppName", "driverCores": 2, "executorMemory": "1G", "executorCores": 2, "numExecutors": 7, "args":["farg1","farg2"],"file":"fFilePath","driverMemory":"2G","files":["ffile1","ffile2"],"jars":["fjar1","fjar2"],"className":"fakeClassName"}'

  Scenario: serializeToJson would serialize the conf map with driver extra Java options well
    Given create PostBatches with the following job config
      | driverMemory | 2G |
    And mock file to fFilePath
    And mock className to fakeClassName
    And apply spark configs
      | spark.driver.extraJavaOptions | -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6006 |
      | other                         | Other values                                                       |
    Then the serialized JSON should be '{"driverMemory":"2G","file":"fFilePath","className":"fakeClassName","conf":{"other":"Other values","spark.driver.extraJavaOptions":"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6006"}}'

  Scenario: serializeToJson would serialize the conf map with conf incomplete value well
    Given create PostBatches with the following job config
      | driverCores | 2 |
    And mock driver memory size to 1200 Megabytes
    And mock file to fFilePath
    And mock className to fakeClassName
    And mock executor memory size to 1.5 Gigabytes
    And apply spark configs
      | invalid | half close" |
    Then the serialized JSON should be '{"file":"fFilePath","driverMemory":"1200M", "executorMemory": "1.5G", "driverCores": 2, "className":"fakeClassName","conf":{"invalid":"half close\""}}'