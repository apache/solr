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

  # Check if Solr is already running
  $solrAlreadyRunning = $false
  try {
    $response = Invoke-WebRequest -Uri "http://localhost:$SOLR_PORT/solr/" -UseBasicParsing -TimeoutSec 2 -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 200) {
      $solrAlreadyRunning = $true
      Write-Host "Solr is already running on port $SOLR_PORT"
    }
  } catch {
    $solrAlreadyRunning = $false
  }

  $script:SolrStartedByTests = $false

  # Start Solr if it's not already running
  if (-not $solrAlreadyRunning) {
    Write-Host "Starting Solr in cloud mode..."

    # Start Solr with embedded ZooKeeper using Start-Process to run asynchronously
    $startArgs = @("start", "-p", "$SOLR_PORT")
    Write-Host "Running: $SolrCmd $($startArgs -join ' ')"

    # Use Start-Job with cmd.exe to properly parse arguments and maintain process hierarchy
    $solrJob = Start-Job -ScriptBlock {
        param($cmd, $argString)
        cmd /c "`"$cmd`" $argString" 2>&1
    } -ArgumentList $SolrCmd, $startArgs

    # Wait a bit for Solr to start
    Start-Sleep -Seconds 5

    Write-Host "Solr starting on port $SOLR_PORT (Job ID: $($solrJob.Id))"

    # Check if there were any immediate errors
    $jobOutput = Receive-Job -Job $solrJob -Keep
    if ($jobOutput) {
        Write-Host "Solr output: $jobOutput"
    }

    $script:SolrStartedByTests = $true

    # Wait for Solr to be ready (max 60 seconds)
    Write-Host "Waiting for Solr to be ready..."
    $maxWaitTime = 60
    $waitInterval = 2
    $elapsed = 0
    $solrReady = $false

    while ($elapsed -lt $maxWaitTime) {
      Start-Sleep -Seconds $waitInterval
      $elapsed += $waitInterval

      try {
        $response = Invoke-WebRequest -Uri "http://localhost:$SOLR_PORT/solr/" -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop

        if ($response.StatusCode -eq 200) {
          $solrReady = $true
          Write-Host "Solr is ready! (took $elapsed seconds)"
          break
        }
      } catch {
        Write-Host "Still waiting... ($elapsed seconds elapsed) - Error: $($_.Exception.Message)"
      }
    }

    if (-not $solrReady) {
      throw "Solr did not become ready within $maxWaitTime seconds"
    }

    # Give it a bit more time to fully initialize ZooKeeper
    Start-Sleep -Seconds 3
  }

  $script:SolrRunning = $true

  function Test-CommandOutput {
    param(
      [string[]]$Arguments,
      [string]$TestName
    )

    Write-Host "Testing: $TestName"
    Write-Host "Running: $SolrCmd $($Arguments -join ' ')"

    # Note that CLI warnings are interpreted as NativeCommandError, since logged to std.err, and therefore may match
    # strings like "Error"
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

AfterAll {
  # Stop Solr only if we started it
  if ($script:SolrStartedByTests) {
    Write-Host "Stopping Solr..."
    $stopArgs = @("stop", "-p", $script:SOLR_PORT)
    Write-Host "Running: $script:SolrCmd $($stopArgs -join ' ')"

    $stopOutput = & $script:SolrCmd @stopArgs 2>&1
    $stopOutputStr = $stopOutput | Out-String
    Write-Host $stopOutputStr

    if ($LASTEXITCODE -ne 0) {
      Write-Warning "Failed to stop Solr cleanly. Exit code: $LASTEXITCODE"
    } else {
      Write-Host "Solr stopped successfully"
    }
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

    It "listing out files" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-z", "localhost:$ZK_PORT", "--recursive") "zk ls / -z localhost:$ZK_PORT --recursive"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via solr-url" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "--solr-url", "http://localhost:$SOLR_PORT") "zk ls / --solr-url http://localhost:$SOLR_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via -s flag" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-s", "http://localhost:$SOLR_PORT") "zk ls / -s http://localhost:$SOLR_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via default (localhost)" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/") "zk ls /"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via -z flag" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "-z", "localhost:$ZK_PORT") "zk ls / -z localhost:$ZK_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "connecting via --zk-host flag" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      Start-Sleep -Seconds 1
      $output = Test-CommandOutput @("zk", "ls", "/", "--zk-host", "localhost:$ZK_PORT") "zk ls / --zk-host localhost:$ZK_PORT"
      $output | Should -Match "aliases\.json"
    }

    It "copying files around" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
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

    It "upconfig" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      $sourceConfigsetDir = Join-Path $SolrTip "server\solr\configsets\sample_techproducts_configs"
      if (Test-Path $sourceConfigsetDir) {
        $output = Test-CommandOutput @("zk", "upconfig", "-d", $sourceConfigsetDir, "-n", "techproducts_ps_test", "-z", "localhost:$ZK_PORT") "zk upconfig"
        $output | Should -Match "Uploading"
        $LASTEXITCODE | Should -Match 0
      } else {
        Set-ItResult -Skipped -Because "sample_techproducts_configs not found"
      }
    }

    It "downconfig" {
      if (-not $script:SolrRunning) { Set-ItResult -Skipped -Because "Solr is not running"; return }
      $tempDir = [System.IO.Path]::GetTempPath()
      $downloadDir = Join-Path $tempDir "downconfig_$([System.IO.Path]::GetRandomFileName())"
      New-Item -ItemType Directory -Path $downloadDir -Force | Out-Null

      try {
        $output = Test-CommandOutput @("zk", "downconfig", "-z", "localhost:$ZK_PORT", "-n", "_default", "-d", $downloadDir) "zk downconfig"
        $output | Should -Match "Downloading"
        $LASTEXITCODE | Should -Match 0
      } finally {
        # Cleanup
        if (Test-Path $downloadDir) {
          Remove-Item $downloadDir -Recurse -Force
        }
      }
    }
  }
}
