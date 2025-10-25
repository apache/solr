#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<#
  Pester tests for Solr assert command
  Ported from test/test_assert.bats
#>

BeforeAll {
  # Get the Solr installation directory from environment variable
  $script:SolrTip = $env:SOLR_TIP
  $script:SolrPort = $env:SOLR_PORT
  $script:SolrHome = $env:SOLR_HOME
  $script:TestFailureDir = $env:TEST_FAILURE_DIR

  if (-not $SolrTip) {
    throw "SOLR_TIP environment variable is not set"
  }

  if (-not $SolrPort) {
    throw "SOLR_PORT environment variable is not set"
  }

  # Determine the Solr executable based on OS
  $script:SolrCmd = Join-Path $SolrTip "bin\solr.cmd"

  if (-not (Test-Path $SolrCmd)) {
    throw "Solr executable not found at: $SolrCmd"
  }

  Write-Host "Using Solr installation at: $SolrTip"
  Write-Host "Using Solr port: $SolrPort"
  Write-Host "Using Solr home: $SolrHome"
}

AfterEach {
  # Stop all Solr instances after each test
  & $SolrCmd stop --all 2>&1 | Out-Null
  Start-Sleep -Seconds 2
}

Describe "Solr Assert Command" {
  Context "Assert for non-cloud mode" {
    It "assert --not-started before starting Solr" {
      $output = & $SolrCmd assert --not-started "http://localhost:$SolrPort" --timeout 5000 2>&1
      $output | Out-String | Should -Match "Solr is not running"
    }

    It "assert --started after starting Solr" {
      # Start Solr in user-managed mode (non-cloud)
      & $SolrCmd start --user-managed 2>&1 | Out-Null
      Start-Sleep -Seconds 5

      $output = & $SolrCmd assert --started "http://localhost:$SolrPort" --timeout 5000 2>&1
      $output | Out-String | Should -Match "Solr is running"
    }

    It "assert --not-cloud on standalone Solr instance" {
      # Start Solr in user-managed mode (non-cloud)
      & $SolrCmd start --user-managed 2>&1 | Out-Null
      Start-Sleep -Seconds 5

      $output = & $SolrCmd assert --not-cloud "http://localhost:$SolrPort/solr" 2>&1
      $output | Out-String | Should -Match "needn't include Solr's context-root"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "assert --cloud fails on standalone Solr instance" {
      # Start Solr in user-managed mode (non-cloud)
      & $SolrCmd start --user-managed 2>&1 | Out-Null
      Start-Sleep -Seconds 5

      $output = & $SolrCmd assert --cloud "http://localhost:$SolrPort" 2>&1
      $LASTEXITCODE | Should -Not -Be 0
      $output | Out-String | Should -Match "ERROR: Solr is not running in cloud mode"
    }
  }

  Context "Assert for cloud mode" {
    It "assert --cloud on cloud Solr instance" {
      # Start Solr in cloud mode (default)
      & $SolrCmd start 2>&1 | Out-Null
      Start-Sleep -Seconds 5

      $output = & $SolrCmd assert --started "http://localhost:$SolrPort" --timeout 5000 2>&1
      $output | Out-String | Should -Match "Solr is running"

      $output = & $SolrCmd assert --cloud "http://localhost:$SolrPort" 2>&1
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "assert --not-cloud fails on cloud Solr instance" {
      # Start Solr in cloud mode (default)
      & $SolrCmd start 2>&1 | Out-Null
      Start-Sleep -Seconds 5

      $output = & $SolrCmd assert --not-cloud "http://localhost:$SolrPort/solr" 2>&1
      $LASTEXITCODE | Should -Not -Be 0
      $output | Out-String | Should -Match "needn't include Solr's context-root"
      $output | Out-String | Should -Match "ERROR: Solr is not running in standalone mode"
    }
  }
}
