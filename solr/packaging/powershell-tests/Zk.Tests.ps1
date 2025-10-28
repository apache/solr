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
  Pester tests for Solr zk command
  Ported from test/test_zk.bats
#>

BeforeAll {
  # Get the Solr installation directory from environment variable
  $script:SolrTip = $env:SOLR_TIP

  # Note: $SolrTip cannot be used directly in -Skip statements in tests, use $env:SOLR_TIP instead
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

  # Get ZooKeeper port from environment or default
  $script:ZK_PORT = if ($env:ZK_PORT) { $env:ZK_PORT } else { "9983" }
  $script:SOLR_PORT = if ($env:SOLR_PORT) { $env:SOLR_PORT } else { "8983" }

  function Test-CommandOutput {
    param(
      [string[]]$Arguments,
      [string]$TestName
    )

    Write-Host "Testing: $TestName"
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

Describe "Solr Zk Command" {
  Context "Help commands" {
    It "short help" {
      $output = Test-CommandOutput @("zk", "ls", "-h") "zk ls -h"
      $output | Should -Match "usage: bin/solr zk"
    }

    It "short help is inferred" {
      $output = Test-CommandOutput @("zk", "ls") "zk ls"
      $output | Should -Match "usage: bin/solr zk"
    }

    It "long help" {
      $output = Test-CommandOutput @("zk", "-h") "zk -h"
      $output | Should -Match "bin/solr zk ls"
      $output | Should -Match "bin/solr zk updateacls"
      $output | Should -Match "Pass --help or -h after any COMMAND"
    }

    It "running subcommands with zk is prevented" {
      $output = Test-CommandOutput @("ls", "/", "-z", "localhost:$ZK_PORT") "ls / -z localhost:$ZK_PORT"
      $output | Should -Match "You must invoke this subcommand using the zk command"
    }
  }

  Context "Zk operations (requires running Solr with ZooKeeper)" -Skip:(-not (Test-Path (Join-Path $env:SOLR_TIP "server\solr"))) {
    BeforeAll {
      # Check if Solr is running by trying to connect to the port
      $solrRunning = $false
      try {
        $response = Invoke-WebRequest -Uri "http://localhost:$SOLR_PORT/solr/" -UseBasicParsing -ErrorAction SilentlyContinue
        if ($response.StatusCode -eq 200) {
          $solrRunning = $true
        }
      } catch {
        $solrRunning = $false
      }

      $script:SolrRunning = $solrRunning
      if (-not $solrRunning) {
        Write-Host "WARNING: Solr does not appear to be running on port $SOLR_PORT"
      }
    }

    It "listing out files" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-z", "localhost:$ZK_PORT", "--recursive") "zk ls / -z localhost:$ZK_PORT --recursive"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via solr-url" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "--solr-url", "http://localhost:$SOLR_PORT") "zk ls / --solr-url http://localhost:$SOLR_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via -s flag" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-s", "http://localhost:$SOLR_PORT") "zk ls / -s http://localhost:$SOLR_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via default (localhost)" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/") "zk ls /"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via -z flag" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-z", "localhost:$ZK_PORT") "zk ls / -z localhost:$ZK_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via --zk-host flag" -Skip:(-not $SolrRunning) {
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "--zk-host", "localhost:$ZK_PORT") "zk ls / --zk-host localhost:$ZK_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "copying files around" -Skip:(-not $SolrRunning) {
      $testFile = "testfile_$([System.IO.Path]::GetRandomFileName()).txt"
      try {
        # Create test file
        "test content" | Out-File -FilePath $testFile -Encoding UTF8

        # Copy to ZK
        $output = Test-CommandOutput @("zk", "cp", $testFile, "zk:/$testFile", "-z", "localhost:$ZK_PORT") "zk cp $testFile to ZK"
        $output | Should -Match "Copying from"

        Start-Sleep -Seconds 1

        # Verify file is in ZK
        $listOutput = Test-CommandOutput @("zk", "ls", "/", "-z", "localhost:$ZK_PORT") "zk ls / to verify copy"
        $listOutput | Should -Match $testFile
      } finally {
        # Cleanup
        if (Test-Path $testFile) {
          Remove-Item $testFile -Force
        }
      }
    }

    It "upconfig" -Skip:(-not $SolrRunning) {
      $sourceConfigsetDir = Join-Path $SolrTip "server\solr\configsets\sample_techproducts_configs"
      if (Test-Path $sourceConfigsetDir) {
        $output = Test-CommandOutput @("zk", "upconfig", "-d", $sourceConfigsetDir, "-n", "techproducts_ps_test", "-z", "localhost:$ZK_PORT") "zk upconfig"
        $output | Should -Match "Uploading"
        $output | Should -Not -Match "ERROR"
      } else {
        Set-ItResult -Skipped -Because "sample_techproducts_configs not found"
      }
    }

    It "downconfig" -Skip:(-not $SolrRunning) {
      $tempDir = [System.IO.Path]::GetTempPath()
      $downloadDir = Join-Path $tempDir "downconfig_$([System.IO.Path]::GetRandomFileName())"
      New-Item -ItemType Directory -Path $downloadDir -Force | Out-Null

      try {
        $output = Test-CommandOutput @("zk", "downconfig", "-z", "localhost:$ZK_PORT", "-n", "_default", "-d", $downloadDir) "zk downconfig"
        $output | Should -Match "Downloading"
        $output | Should -Not -Match "ERROR"
      } finally {
        # Cleanup
        if (Test-Path $downloadDir) {
          Remove-Item $downloadDir -Recurse -Force
        }
      }
    }
  }
}
