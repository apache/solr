# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run against an assembled Solr distribution on Windows:
# pwsh -File test_help_windows.ps1 -SolrDir C:\path\to\solr
param([Parameter(Mandatory = $true)][string]$SolrDir)

$ErrorActionPreference = 'Stop'
$launcher = Join-Path $SolrDir 'bin\solr.cmd'
if (-not (Test-Path -LiteralPath $launcher)) { throw "Missing launcher: $launcher" }

function Check-Command([int]$expected, [string]$marker, [string[]]$cliArgs) {
  $ErrorActionPreference = 'Continue'
  $output = (& $launcher @cliArgs 2>&1 | Out-String)
  $actual = $LASTEXITCODE
  if ($actual -ne $expected -or $output -notmatch [regex]::Escape($marker)) {
    throw "solr ${cliArgs}: expected status $expected and '$marker', got $actual`n$output"
  }
}

$previous = @{}
foreach ($name in @('SOLR_PICOCLI', 'SOLR_TOOL_OPTS', 'SOLR_INCLUDE')) {
  $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
try {
  $env:SOLR_INCLUDE = Join-Path $SolrDir 'nonexistent-test-include.cmd'
  Remove-Item Env:SOLR_TOOL_OPTS -ErrorAction SilentlyContinue
  foreach ($parser in @('true', 'false')) {
    $env:SOLR_PICOCLI = $parser
    Check-Command 2 'solr' @()
    foreach ($flag in @('--help', '-h')) {
      Check-Command 0 'solr' @($flag)
    }
    foreach ($parent in @('zk', 'snapshot', 'package')) {
      Check-Command 2 $parent @($parent)
      foreach ($flag in @('--help', '-h')) {
        Check-Command 0 $parent @($parent, $flag)
      }
    }
    $env:SOLR_TOOL_OPTS = '-XX:SolrInvalidOptionForTest'
    Check-Command 1 'Unrecognized VM option' @('--help')
    Remove-Item Env:SOLR_TOOL_OPTS
  }
  Write-Output 'Windows launcher help checks passed for both parsers.'
} finally {
  foreach ($name in $previous.Keys) {
    [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
  }
}
