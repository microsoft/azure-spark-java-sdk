Feature: SparkBatchJobRemoteProcess integration test

  Scenario: Submit a HDInsight Spark job with artifact in shared storage
    Given create PostBatches with the following job config for SparkBatchJobRemoteProcess
      | className | sample.LogQuery |
      | file      | adl://zhwespkwesteu.azuredatalakestore.net/clusters/zhwe-spk23-adlsgen1/SparkSubmission/2019/04/28/4c017893-dd57-4a01-8bf9-593f002cd391/default_artifact.jar |
    And submit HDInsight Spark job
    Then check the HDInsight Spark job stdout should be
      | (10.10.10.10,"FRED",GET http://images.com/2013/Generic.jpg HTTP/1.1)	bytes=621	n=2 |