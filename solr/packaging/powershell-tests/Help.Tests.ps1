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

  function Test-HelpOutput {
    param(
      [string[]]$Arguments,
      [string]$ExpectedPattern,
      [string]$TestName
    )

    Write-Host "Testing help: $TestName"
    Write-Host "Running: $SolrCmd $($Arguments -join ' ')"

    $output = & $SolrCmd @Arguments 2>&1
    $outputStr = $output | Out-String

    Write-Host "Exit Code: $LASTEXITCODE"
    if ($outputStr.Length -gt 0) {
      Write-Host "Output (first 500 chars): $($outputStr.Substring(0, [Math]::Min(500, $outputStr.Length)))"
    } else {
      Write-Host "WARNING: Output is empty!"
    }

    return $outputStr
  }
}

Describe "Solr Help Command" {
  Context "Main help commands" {
    It "solr --help flag prints help" {
      $output = Test-HelpOutput @("--help") "Usage: solr COMMAND OPTIONS" "solr --help"
      $output | Should -Match "Usage: solr COMMAND OPTIONS"
      $output | Should -Not -cmatch "ERROR"
    }

    It "solr with no flags prints help" {
      $output = Test-HelpOutput @() "Usage: solr COMMAND OPTIONS" "solr (no flags)"
      $output | Should -Match "Usage: solr COMMAND OPTIONS"
      $output | Should -Not -cmatch "ERROR"
    }
  }

  Context "Command-specific help" {
    It "start --help flag prints help" {
      $output = Test-HelpOutput @("start", "--help") "Usage: solr start" "start --help"
      $output | Should -Match "Usage: solr start"
      $output | Should -Not -cmatch "ERROR"
    }

    It "start -h flag prints help" {
      $output = Test-HelpOutput @("start", "-h") "Usage: solr start" "start -h"
      $output | Should -Match "Usage: solr start"
      $output | Should -Not -Match "ERROR: Hostname is required when using the -h option!"
    }

    It "stop --help flag prints help" {
      $output = Test-HelpOutput @("stop", "--help") "Usage: solr stop" "stop --help"
      $output | Should -Match "Usage: solr stop"
      $output | Should -Not -cmatch "ERROR"
    }

    It "restart --help flag prints help" {
      $output = Test-HelpOutput @("restart", "--help") "Usage: solr restart" "restart --help"
      $output | Should -Match "Usage: solr restart"
      $output | Should -Not -cmatch "ERROR"
    }

    It "status --help flag prints help" {
      $output = Test-HelpOutput @("status", "--help") "usage: bin/solr status" "status --help"
      $output | Should -Match "usage: bin/solr status"
      $output | Should -Not -cmatch "ERROR"
    }

    It "healthcheck --help flag prints help" {
      $output = Test-HelpOutput @("healthcheck", "--help") "usage: bin/solr healthcheck" "healthcheck --help"
      $output | Should -Match "usage: bin/solr healthcheck"
      $output | Should -Not -cmatch "ERROR"
    }

    It "create --help flag prints help" {
      $output = Test-HelpOutput @("create", "--help") "usage: bin/solr create" "create --help"
      $output | Should -Match "usage: bin/solr create"
      $output | Should -Not -cmatch "ERROR"
    }

    It "delete -h flag prints help" {
      $output = Test-HelpOutput @("delete", "-h") "usage: bin/solr delete" "delete -h"
      $output | Should -Match "usage: bin/solr delete"
      $output | Should -Not -cmatch "ERROR"
    }

    It "zk --help flag prints help" {
      $output = Test-HelpOutput @("zk", "--help") "usage:" "zk --help"
      $output | Should -Match "usage:"
      $output | Should -Match "bin/solr zk ls"
      $output | Should -Not -cmatch "ERROR"
    }

    It "auth --help flag prints help" {
      $output = Test-HelpOutput @("auth", "--help") "bin/solr auth enable" "auth --help"
      $output | Should -Match "bin/solr auth enable"
      $output | Should -Not -cmatch "ERROR"
    }

    It "assert --help flag prints help" {
      $output = Test-HelpOutput @("assert", "--help") "usage: bin/solr assert" "assert --help"
      $output | Should -Match "usage: bin/solr assert"
      $output | Should -Not -cmatch "ERROR"
    }

    It "post --help flag prints help" {
      $output = Test-HelpOutput @("post", "--help") "usage: bin/solr post" "post --help"
      $output | Should -Match "usage: bin/solr post"
      $output | Should -Not -cmatch "ERROR"
    }
  }
}
