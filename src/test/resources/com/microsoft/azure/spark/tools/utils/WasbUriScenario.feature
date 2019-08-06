Feature: WasbUri unit tests

  Scenario: Parsing a WASB blob URI
    Then check parsing WASB URI table should be
      | rawUri | container | storageAccount | endpointSuffix | absolutePath |
      | wasb://mock-container-00@mockacc21.blob.core.windows.net          | mock-container-00 | mockacc21 | core.windows.net | /     |
      | wasb://mock-container-00@mockacc21.blob.core.windows.net/         | mock-container-00 | mockacc21 | core.windows.net | /     |
      | wasbs://mock-container-00@mockacc21.blob.core.windows.net         | mock-container-00 | mockacc21 | core.windows.net | /     |
      | wasbs://mock-container-00@mockacc21.blob.core.windows.net/        | mock-container-00 | mockacc21 | core.windows.net | /     |
      | wasb://mock-container-00@mockacc21.blob.core.windows.net/a/b      | mock-container-00 | mockacc21 | core.windows.net | /a/b  |
      | wasb://mock-container-00@mockacc21.blob.core.windows.net/a/b/     | mock-container-00 | mockacc21 | core.windows.net | /a/b/ |
      | wasb://mock-container-00@mockacc21.blob.core.windows.net/a/b?k=v  | mock-container-00 | mockacc21 | core.windows.net | /a/b  |
      | http://mockacc21.blob.core.windows.net/mock-container-00/         | mock-container-00 | mockacc21 | core.windows.net | /     |
      | https://mockacc21.blob.core.windows.net/mock-container-00/        | mock-container-00 | mockacc21 | core.windows.net | /     |
      | http://mockacc21.blob.core.windows.net/mock-container-00          | mock-container-00 | mockacc21 | core.windows.net | /     |
      | https://mockacc21.blob.core.windows.net/mock-container-00         | mock-container-00 | mockacc21 | core.windows.net | /     |
      | https://mockacc21.blob.core.windows.net/mock-container-00/a/b     | mock-container-00 | mockacc21 | core.windows.net | /a/b  |
      | https://mockacc21.blob.core.windows.net/mock-container-00/a/b/    | mock-container-00 | mockacc21 | core.windows.net | /a/b/ |
      | https://mockacc21.blob.core.windows.net/mock-container-00/a/b?k=v | mock-container-00 | mockacc21 | core.windows.net | /a/b |
