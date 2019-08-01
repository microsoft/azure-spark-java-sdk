Feature: HttpObservable unit tests

  Scenario: Default cookie store setter validation
    Given setup a basic Http mock service for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare Http 'GET' request to '/api' with username 'mockuser' and password 'mockpw'
    And add a cookie 'mck1:mcv1' into cookie store
    Then send and check Http 'GET' request to '/api' should contain headers
      | Authorization | Basic bW9ja3VzZXI6bW9ja3B3 |
      | Cookie        | mck1=mcv1                  |

  Scenario: Mock Trust Strategy only for HTTPS
    Given setup a basic Https mock service for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare Https 'GET' request to '/api' with username 'mockuser' and password 'mockpw'
    And set mocked Trust Strategy
    Then send and check Https 'GET' request to '/api' should throw SSLException "Certificate for <localhost> doesn't match any of the subject alternative names: []"
    Then check the mocked Trust Strategy is invoked

  Scenario: Mock Trust Strategy and Bypass SSL validation for HTTPS
    Given setup a basic Https mock service for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare Https 'GET' request to '/api' with username 'mockuser' and password 'mockpw'
    And set mocked Trust Strategy
    And set SSL validation bypass is enabled
    Then send and check Https 'GET' request to '/api' should contain headers
      | Authorization | Basic bW9ja3VzZXI6bW9ja3B3 |
    And set SSL validation bypass is disabled
    Then check the mocked Trust Strategy is invoked

  Scenario: Bypass SSL Validation for HTTPS
    Given setup a basic Https mock service for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare Https 'GET' request to '/api' with username 'mockuser' and password 'mockpw'
    And set Trust All Strategy is enabled
    And set SSL validation bypass is enabled
    Then send and check Https 'GET' request to '/api' should contain headers
      | Authorization | Basic bW9ja3VzZXI6bW9ja3B3 |
    And set SSL validation bypass is disabled
    And set Trust All Strategy is disabled

  Scenario: Negative case with SSL Validation failure for HTTPS
    Given setup a basic Https mock service for 'GET' request '/api' to return '{"messages": "good" }'
    And prepare Https 'GET' request to '/api' with username 'mockuser' and password 'mockpw'
    Then send and check Https 'GET' request to '/api' should throw CertificateException 'PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target'
