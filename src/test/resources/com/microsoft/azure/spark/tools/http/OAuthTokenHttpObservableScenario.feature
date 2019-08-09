@OAuthTokenHttpObservable
Feature: OAuthTokenHttpObservable unit test

  Scenario: OAuth token in request header
    Given setup a mock service with OAuth auth for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare OAuthTokenHttp 'GET' request to '/api' with access token 'MOCKTOKEN'
    Then send and check OAuthTokenHttp 'GET' request to '/api' should contains header 'Authorization: Bearer MOCKTOKEN'

  Scenario: OAuth token get error when sending requests
    Given setup a mock service with OAuth auth for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare OAuthTokenHttp 'GET' request to '/api' with access token getting IOException 'mocked error'
    Then check get OAuthTokenHttp default header should throw IOException with 'mocked error'
    Then send and check OAuthTokenHttp 'GET' request to '/api' should throw IOException with 'mocked error'

  Scenario: OAuth token get runtime error when sending requests
    Given setup a mock service with OAuth auth for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare OAuthTokenHttp 'GET' request to '/api' with access token getting RuntimeException 'mocked error'
    Then send and check OAuthTokenHttp 'GET' request to '/api' should throw RuntimeException with 'mocked error'
