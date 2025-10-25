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
  Pester tests for Solr help command
  Ported from test/test_help.bats
#>

BeforeAll {
  # Get the Solr installation directory from environment variable
  $script:SolrTip = $env:SOLR_TIP

  if (-not $SolrTip) {
    throw "SOLR_TIP environment variable is not set"
  }

  # Determine the Solr executable based on OS
  $script:SolrCmd = Join-Path $SolrTip "bin\solr.cmd"

  if (-not (Test-Path $SolrCmd)) {
    throw "Solr executable not found at: $SolrCmd"
  }

  Write-Host "Using Solr installation at: $SolrTip"
  Write-Host "Using Solr command: $SolrCmd"
}

Describe "Solr Help Command" {
  Context "Main help commands" {
    It "solr --help flag prints help" {
      $output = & $SolrCmd --help 2>&1
      $output | Out-String | Should -Match "Usage: solr COMMAND OPTIONS"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "solr with no flags prints help" {
      $output = & $SolrCmd 2>&1
      $output | Out-String | Should -Match "Usage: solr COMMAND OPTIONS"
      $output | Out-String | Should -Not -Match "ERROR"
    }
  }

  Context "Command-specific help" {
    It "start --help flag prints help" {
      $output = & $SolrCmd start --help 2>&1
      $output | Out-String | Should -Match "Usage: solr start"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "start -h flag prints help" {
      $output = & $SolrCmd start -h 2>&1
      $output | Out-String | Should -Match "Usage: solr start"
      $output | Out-String | Should -Not -Match "ERROR: Hostname is required when using the -h option!"
    }

    It "stop --help flag prints help" {
      $output = & $SolrCmd stop --help 2>&1
      $output | Out-String | Should -Match "Usage: solr stop"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "restart --help flag prints help" {
      $output = & $SolrCmd restart --help 2>&1
      $output | Out-String | Should -Match "Usage: solr restart"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "status --help flag prints help" {
      $output = & $SolrCmd status --help 2>&1
      $output | Out-String | Should -Match "usage: bin/solr status"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "healthcheck --help flag prints help" {
      $output = & $SolrCmd healthcheck --help 2>&1
      $output | Out-String | Should -Match "usage: bin/solr healthcheck"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "create --help flag prints help" {
      $output = & $SolrCmd create --help 2>&1
      $output | Out-String | Should -Match "usage: bin/solr create"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "delete -h flag prints help" {
      $output = & $SolrCmd delete -h 2>&1
      $output | Out-String | Should -Match "usage: bin/solr delete"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "zk --help flag prints help" {
      $output = & $SolrCmd zk --help 2>&1
      $output | Out-String | Should -Match "usage:"
      $output | Out-String | Should -Match "bin/solr zk ls"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "auth --help flag prints help" {
      $output = & $SolrCmd auth --help 2>&1
      $output | Out-String | Should -Match "bin/solr auth enable"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "assert --help flag prints help" {
      $output = & $SolrCmd assert --help 2>&1
      $output | Out-String | Should -Match "usage: bin/solr assert"
      $output | Out-String | Should -Not -Match "ERROR"
    }

    It "post --help flag prints help" {
      $output = & $SolrCmd post --help 2>&1
      $output | Out-String | Should -Match "usage: bin/solr post"
      $output | Out-String | Should -Not -Match "ERROR"
    }
  }
}
