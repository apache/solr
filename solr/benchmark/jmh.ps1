# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

param(
    [string]$SysProps = ""
)

# Get all arguments passed after the script name (remaining args go to JMH)
$JmhArgs = $args

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$gradlewDir = Join-Path $scriptDir "..\..\"
$gradlew = Join-Path $gradlewDir "gradlew.bat"

# Check if lib directory exists
$libDir = Join-Path $scriptDir "lib"
if (Test-Path $libDir) {
    Write-Host "Using lib directory for classpath..."
    $classpath = "$libDir\*;$scriptDir\build\classes\java\main"
} else {
    Write-Host "Getting classpath from gradle..."

    # Build the jars first
    Push-Location $gradlewDir
    try {
        & $gradlew -q jar
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Gradle build failed"
            exit 1
        }
        Write-Host "Gradle build done"
    } finally {
        Pop-Location
    }

    # Get classpath from gradle
    Push-Location $scriptDir
    try {
        $classpath = & $gradlew -q echoCp
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to get classpath from gradle"
            exit 1
        }
    } finally {
        Pop-Location
    }
}

Write-Host "Running JMH with args: $JmhArgs"

# JVM Arguments
$jvmArgs = @(
    "-jvmArgs", "-Djmh.shutdownTimeout=5",
    "-jvmArgs", "-Djmh.shutdownTimeout.step=3",
    "-jvmArgs", "-Djava.security.egd=file:/dev/./urandom",
    "-jvmArgs", "-XX:+UnlockDiagnosticVMOptions",
    "-jvmArgs", "-XX:+DebugNonSafepoints",
    "-jvmArgs", "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
)

# GC Arguments
$gcArgs = @(
    "-jvmArgs", "-XX:+UseG1GC",
    "-jvmArgs", "-XX:+ParallelRefProcEnabled"
)

# Logging Arguments
$loggingArgs = @(
    "-jvmArgs", "-Dlog4jConfigurationFile=./log4j2-bench.xml",
    "-jvmArgs", "-Dlog4j2.is.webapp=false",
    "-jvmArgs", "-Dlog4j2.garbagefreeThreadContextMap=true",
    "-jvmArgs", "-Dlog4j2.enableDirectEncoders=true",
    "-jvmArgs", "-Dlog4j2.enable.threadlocals=true"
)

# User-provided system properties
$userSysPropsArgs = @()
if ($SysProps -ne "") {
    # Split on whitespace, handling -D properties
    $props = $SysProps -split '\s+(?=-D)' | Where-Object { $_ -ne "" }
    foreach ($prop in $props) {
        $userSysPropsArgs += "-jvmArgs", $prop.Trim()
    }
    Write-Host "User system properties: $($props -join ', ')"
}

# Build the full argument list
$allArgs = @(
    "-cp", $classpath,
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "-Djdk.module.illegalAccess.silent=true",
    "org.openjdk.jmh.Main"
) + $jvmArgs + $loggingArgs + $gcArgs + $userSysPropsArgs + $JmhArgs

# Run JMH
Write-Host "Executing: java $($allArgs -join ' ')"
& java $allArgs

$exitCode = $LASTEXITCODE
Write-Host "JMH benchmarks done (exit code: $exitCode)"
exit $exitCode

