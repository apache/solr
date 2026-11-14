@echo off
@REM Licensed to the Apache Software Foundation (ASF) under one or more
@REM contributor license agreements.  See the NOTICE file distributed with
@REM this work for additional information regarding copyright ownership.
@REM The ASF licenses this file to You under the Apache License, Version 2.0
@REM (the "License"); you may not use this file except in compliance with
@REM the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@REM The cmd.exe twin of mytool.sh: the same modes, printing the same thing, so a test
@REM drives one spelling on either shell.
@REM
@REM Usage: mytool echo <text...>     echoes its arguments, one space between them
@REM        mytool sleep <seconds>    stays alive for that long
@REM        mytool work <seconds> <status>
@REM                                  prints "starting", stays alive for that long, prints
@REM                                  "finished", exits with that status
@REM        mytool listen <port>      holds a TCP port open until killed

setlocal
set "mode=%~1"
if /i "%mode%"=="echo" goto echoargs
if /i "%mode%"=="sleep" goto dosleep
if /i "%mode%"=="work" goto dowork
if /i "%mode%"=="listen" goto dolisten
echo usage: mytool echo ^<text...^> ^| sleep ^<s^> ^| work ^<s^> ^<status^> ^| listen ^<port^> 1>&2
exit /b 2

:echoargs
@REM %* ignores shift, so the arguments after the mode are collected one at a time.
set "text="
shift
:nextarg
if "%~1"=="" goto printtext
if defined text (set "text=%text% %~1") else (set "text=%~1")
shift
goto nextarg
:printtext
echo %text%
exit /b 0

:dosleep
powershell -NoProfile -Command "Start-Sleep -Seconds %~2"
exit /b 0

:dowork
echo starting
powershell -NoProfile -Command "Start-Sleep -Seconds %~2"
echo finished
if "%~3"=="" exit /b 0
exit /b %~3

:dolisten
echo listening on %~2
powershell -NoProfile -Command "$l = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, %~2); $l.Start(); while ($true) { $l.AcceptTcpClient().Close() }"
exit /b 0
