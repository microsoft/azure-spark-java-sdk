@ArcadiaSparkBatchScenario
Feature: ArcadiaSparkBatch integration test

  Scenario: Submit a Arcadia Spark job with artifact in shared storage
    Given create PostBatches with the following job config for ArcadiaBatch
      | className | sample.JavaSparkPi |
      | name      | sample.JavaSparkPi |
      | driverMemory | 4G              |
      | driverCores  | 1               |
      | executorMemory | 4G            |
      | executorCores  | 1             |
      | numExecutors   | 5             |
      | file      | wasbs://rufancontainer@rufanblob072301.blob.core.windows.net/SparkSubmission/2019/08/07/0d826013-e06f-47b4-bcd0-5f11c715e82c/sample-1.0-SNAPSHOT.jar |
      | spark.hadoop.fs.azure.account.key.rufanblob072301.blob.core.windows.net | masked_blob_access_token |
    And mock the Arcadia HTTP OAuth token to 'masked_oauth_token'
    And mock the Arcadia workspace to 'zhwe-0801'
    And submit Arcadia Spark job
    Then no any error after submitting Arcadia Spark job
