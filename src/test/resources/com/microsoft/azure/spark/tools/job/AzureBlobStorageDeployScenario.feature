Feature: AzureBlobStorageDeploy unit tests

  Scenario: Create an AzureBlobStorageDeploy with access key and fs root
    Given create an AzureBlobStorageDeploy with access key 'mockkey' and fs root 'wasbs://mycontainer@mockaccount.blob.core.windows.net/'
    Then check the AzureBlobStorageDeploy storage account credential is 'mockkey' and account is 'mockaccount'

  Scenario: Create an AzureBlobStorageDeploy with mocked storage account key and fs root
    Given create an AzureBlobStorageDeploy with mocked storage account and fs root 'wasbs://mycontainer@mockaccount.blob.core.windows.net/'
    And perform an Azure blob deploy operation for the local file '/tmp/mock_artifact.jar'
    Then check the destination URI should match regex 'wasbs://mycontainer@mockaccount.blob.core.windows.net/SparkSubmission/\d{4}/\d{2}/\d{2}/[^/]+/mock_artifact.jar'