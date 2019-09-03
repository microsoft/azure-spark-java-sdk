@SparkBatchJobRemoteProcessScenario
Feature: SparkBatchJobRemoteProcess integration test

  Scenario: Submit a HDInsight Spark job with artifact in shared storage
    Given create PostBatches with the following job config for SparkBatchJobRemoteProcess
      | className | sample.LogQuery |
      | file      | adl://zhwespkwesteu.azuredatalakestore.net/clusters/zhwe-spk23-adlsgen1/SparkSubmission/2019/04/28/4c017893-dd57-4a01-8bf9-593f002cd391/default_artifact.jar |
    And submit HDInsight Spark job
    Then check the HDInsight Spark job stdout should be
      | (10.10.10.10,"FRED",GET http://images.com/2013/Generic.jpg HTTP/1.1)	bytes=621	n=2 |
    Then check the HDInsight Spark job stderr should match the following line and log
      | 0   | SLF4J: Class path contains multiple SLF4J bindings. |
      | 18  | 19/09/03 02:56:38 INFO SparkContext: Running Spark version 2.3.2.2.6.5.3006-29 |
      | 328 | 19/09/03 02:56:46 INFO ShutdownHookManager: Shutdown hook called |
      | -1  | 19/09/03 02:56:46 INFO ShutdownHookManager: Deleting directory /mnt/resource/hadoop/yarn/local/usercache/livy/appcache/application_1555654226340_0207/spark-db11308a-da9e-4574-90c2-2a28aa89e4a1 |
