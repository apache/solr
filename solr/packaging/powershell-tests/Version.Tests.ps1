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
  Pester tests for Solr version command
  Ported from test/test_version.bats
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

Describe "Solr Version Command" {
  Context "When using --version flag" {
    It "--version returns Solr version" {
      $output = & $SolrCmd --version 2>&1
      $output | Should -Contain "Solr version is:"
    }
  }

  Context "When using version as direct command" {
    It "version as direct tool call still runs" {
      $output = & $SolrCmd version 2>&1
      $output | Should -Contain "Solr version is:"
    }
  }
}
