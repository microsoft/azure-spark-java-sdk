Feature: AzureHttpObservable unit test

  Scenario: API Version parameter in request
    Given setup a mock service with Azure OAuth auth for 'GET' request '/api&api-version=2019-01-01' to return '{"messages": "good" }'
    And prepare AzureHttp 'GET' request to '/api' with access token 'MOCKTOKEN' and API version '2019-01-01'
    Then send and check AzureHttp 'GET' request to '/api' should contains header 'Authorization: Bearer MOCKTOKEN' and parameters
      | api-version | 2019-01-01 |
