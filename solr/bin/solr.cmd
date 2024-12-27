@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off

@REM Make sure to keep line endings as CRLF for .cmd files

IF "%OS%"=="Windows_NT" setlocal enabledelayedexpansion enableextensions

@REM What version of Java is required to run this version of Solr.
set REQUIRED_JAVA_VERSION=21
set "PASS_TO_RUN_EXAMPLE="

REM Determine top-level Solr directory
set SDIR=%~dp0
IF "%SDIR:~-1%"=="\" set SDIR=%SDIR:~0,-1%
set SOLR_TIP=%SDIR%\..
pushd %SOLR_TIP%
set SOLR_TIP=%CD%
popd

REM Used to report errors before exiting the script
set SCRIPT_ERROR=
set NO_USER_PROMPT=0

REM Allow user to import vars from an include file
REM vars set in the include file can be overridden with
REM command line args
IF "%SOLR_INCLUDE%"=="" set "SOLR_INCLUDE=%SOLR_TIP%\bin\solr.in.cmd"
IF EXIST "%SOLR_INCLUDE%" CALL "%SOLR_INCLUDE%"

set "DEFAULT_SERVER_DIR=%SOLR_TIP%\server"


REM Verify Java is available
IF DEFINED SOLR_JAVA_HOME set "JAVA_HOME=%SOLR_JAVA_HOME%"
REM Try to detect JAVA_HOME from the registry
IF NOT DEFINED JAVA_HOME (
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment" /v CurrentVersion') DO set CurVer=%%B
  FOR /F "skip=2 tokens=2*" %%A IN ('REG QUERY "HKLM\Software\JavaSoft\Java Runtime Environment\!CurVer!" /v JavaHome') DO (
    set "JAVA_HOME=%%B"
  )
)
IF NOT DEFINED JAVA_HOME (
  REM Need java home
  @echo "Please set the JAVA_HOME environment variable to the path where you installed Java !REQUIRED_JAVA_VERSION!+"
  goto done
)
set JAVA_HOME=%JAVA_HOME:"=%
IF %JAVA_HOME:~-1%==\ SET JAVA_HOME=%JAVA_HOME:~0,-1%
IF NOT EXIST "%JAVA_HOME%\bin\java.exe" (
  set "SCRIPT_ERROR=java.exe not found in %JAVA_HOME%\bin. Please set JAVA_HOME to a valid JRE / JDK directory."
  goto err
)
set "JAVA=%JAVA_HOME%\bin\java"
CALL :resolve_java_info
IF !JAVA_MAJOR_VERSION! LSS !REQUIRED_JAVA_VERSION! (
  set "SCRIPT_ERROR=Java !REQUIRED_JAVA_VERSION! or later is required to run Solr. Current Java version is: !JAVA_VERSION_INFO! (detected major: !JAVA_MAJOR_VERSION!)"
  goto err
)


REM Select HTTP OR HTTPS related configurations
set SOLR_URL_SCHEME=http
set "SOLR_JETTY_CONFIG=--module=http"
set "SOLR_SSL_OPTS= "

IF DEFINED SOLR_HADOOP_CREDENTIAL_PROVIDER_PATH (
  set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dhadoop.security.credential.provider.path=%SOLR_HADOOP_CREDENTIAL_PROVIDER_PATH%"
)

IF NOT DEFINED SOLR_SSL_ENABLED (
  IF DEFINED SOLR_SSL_KEY_STORE (
    set "SOLR_SSL_ENABLED=true"
  ) ELSE (
    set "SOLR_SSL_ENABLED=false"
  )
)

IF NOT DEFINED SOLR_SSL_RELOAD_ENABLED (
  set "SOLR_SSL_RELOAD_ENABLED=true"
)

REM Enable java security manager by default (limiting filesystem access and other things)
IF NOT DEFINED SOLR_SECURITY_MANAGER_ENABLED (
  set SOLR_SECURITY_MANAGER_ENABLED=true
)

IF "%SOLR_SSL_ENABLED%"=="true" (
  set "SOLR_JETTY_CONFIG=--module=https --lib="%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*""
  set SOLR_URL_SCHEME=https
  IF "%SOLR_SSL_RELOAD_ENABLED%"=="true" (
    set "SOLR_JETTY_CONFIG=!SOLR_JETTY_CONFIG! --module=ssl-reload"
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.keyStoreReload.enabled=true"
  )
  IF DEFINED SOLR_SSL_KEY_STORE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.keystore=%SOLR_SSL_KEY_STORE%"
    IF "%SOLR_SSL_RELOAD_ENABLED%"=="true" (
      IF "%SOLR_SECURITY_MANAGER_ENABLED%"=="true" (
        set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.keystoreParentPath=%SOLR_SSL_KEY_STORE%/.."
      )
    )
  )

  IF DEFINED SOLR_SSL_KEY_STORE_TYPE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.keystore.type=%SOLR_SSL_KEY_STORE_TYPE%"
  )

  IF DEFINED SOLR_SSL_TRUST_STORE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.truststore=%SOLR_SSL_TRUST_STORE%"
  )
  IF DEFINED SOLR_SSL_TRUST_STORE_TYPE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.truststore.type=%SOLR_SSL_TRUST_STORE_TYPE%"
  )

  IF NOT DEFINED SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION (
    set SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=true
  )
  IF "%SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION%"=="true" (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.ssl.verifyClientHostName=HTTPS"
  )

  IF DEFINED SOLR_SSL_NEED_CLIENT_AUTH (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.ssl.needClientAuth=%SOLR_SSL_NEED_CLIENT_AUTH%"
  )
  IF DEFINED SOLR_SSL_WANT_CLIENT_AUTH (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.jetty.ssl.wantClientAuth=%SOLR_SSL_WANT_CLIENT_AUTH%"
  )

  IF DEFINED SOLR_SSL_CLIENT_KEY_STORE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.keyStore=%SOLR_SSL_CLIENT_KEY_STORE%"

    IF DEFINED SOLR_SSL_CLIENT_KEY_STORE_TYPE (
      set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.keyStoreType=%SOLR_SSL_CLIENT_KEY_STORE_TYPE%"
    )
    IF "%SOLR_SSL_RELOAD_ENABLED%"=="true" (
      IF "%SOLR_SECURITY_MANAGER_ENABLED%"=="true" (
        set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.keyStoreParentPath=%SOLR_SSL_CLIENT_KEY_STORE_TYPE%/.."
      )
    )
  ) ELSE (
    IF DEFINED SOLR_SSL_KEY_STORE (
      set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.keyStore=%SOLR_SSL_KEY_STORE%"
    )
    IF DEFINED SOLR_SSL_KEY_STORE_TYPE (
      set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.keyStoreType=%SOLR_SSL_KEY_STORE_TYPE%"
    )
  )

  IF DEFINED SOLR_SSL_CLIENT_TRUST_STORE (
    set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.trustStore=%SOLR_SSL_CLIENT_TRUST_STORE%"

    IF DEFINED SOLR_SSL_CLIENT_TRUST_STORE_TYPE (
      set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.trustStoreType=%SOLR_SSL_CLIENT_TRUST_STORE_TYPE%"
    )
  ) ELSE (
    IF DEFINED SOLR_SSL_TRUST_STORE (
     set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.trustStore=%SOLR_SSL_TRUST_STORE%"
    )
    IF DEFINED SOLR_SSL_TRUST_STORE_TYPE (
     set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Djavax.net.ssl.trustStoreType=%SOLR_SSL_TRUST_STORE_TYPE%"
    )
  )
  IF DEFINED SOLR_SSL_CHECK_PEER_NAME (
   set "SOLR_SSL_OPTS=!SOLR_SSL_OPTS! -Dsolr.ssl.checkPeerName=%SOLR_SSL_CHECK_PEER_NAME% -Dsolr.jetty.ssl.sniHostCheck=%SOLR_SSL_CHECK_PEER_NAME%"
  )
) ELSE (
  set SOLR_SSL_OPTS=
)

REM Requestlog options
IF NOT DEFINED SOLR_REQUESTLOG_ENABLED (
  set SOLR_REQUESTLOG_ENABLED=true
)
IF "%SOLR_REQUESTLOG_ENABLED%"=="true" (
  set "SOLR_JETTY_CONFIG=!SOLR_JETTY_CONFIG! --module=requestlog"
)

REM Jetty gzip module enabled by default
IF NOT DEFINED SOLR_GZIP_ENABLED (
  set "SOLR_GZIP_ENABLED=true"
)
IF "%SOLR_GZIP_ENABLED%"=="true" (
  set "SOLR_JETTY_CONFIG=!SOLR_JETTY_CONFIG! --module=gzip"
)

REM Authentication options

IF NOT DEFINED SOLR_AUTH_TYPE (
  IF DEFINED SOLR_AUTHENTICATION_OPTS (
    echo WARNING: SOLR_AUTHENTICATION_OPTS variable configured without associated SOLR_AUTH_TYPE variable
    echo          Please configure SOLR_AUTH_TYPE variable with the authentication type to be used.
    echo          Currently supported authentication types are [basic]
  )
)

IF DEFINED SOLR_AUTH_TYPE (
  IF DEFINED SOLR_AUTHENTICATION_CLIENT_BUILDER (
    echo WARNING: SOLR_AUTHENTICATION_CLIENT_BUILDER and SOLR_AUTH_TYPE variables are configured together
    echo          Use SOLR_AUTH_TYPE variable to configure authentication type to be used
    echo          Currently supported authentication types are [basic]
    echo          The value of SOLR_AUTHENTICATION_CLIENT_BUILDER configuration variable will be ignored
  )
)

IF DEFINED SOLR_AUTH_TYPE (
  IF /I "%SOLR_AUTH_TYPE%" == "basic" (
    set SOLR_AUTHENTICATION_CLIENT_BUILDER="org.apache.solr.client.solrj.impl.PreemptiveBasicAuthClientBuilderFactory"
  ) ELSE (
    echo ERROR: Value specified for SOLR_AUTH_TYPE configuration variable is invalid.
    goto err
  )
)

IF DEFINED SOLR_AUTHENTICATION_CLIENT_CONFIGURER (
  echo WARNING: Found unsupported configuration variable SOLR_AUTHENTICATION_CLIENT_CONFIGURER
  echo          Please start using SOLR_AUTH_TYPE instead
)
IF DEFINED SOLR_AUTHENTICATION_CLIENT_BUILDER (
  set AUTHC_CLIENT_BUILDER_ARG="-Dsolr.httpclient.builder.factory=%SOLR_AUTHENTICATION_CLIENT_BUILDER%"
)
set "AUTHC_OPTS=%AUTHC_CLIENT_BUILDER_ARG% %SOLR_AUTHENTICATION_OPTS%"

REM Set the SOLR_TOOL_HOST variable for use when connecting to a running Solr instance
IF NOT "%SOLR_HOST%"=="" (
  set "SOLR_TOOL_HOST=%SOLR_HOST%"
) ELSE (
  set "SOLR_TOOL_HOST=localhost"
)
IF "%SOLR_JETTY_HOST%"=="" (
  set "SOLR_JETTY_HOST=127.0.0.1"
)


REM Handle special commands
IF "%1"=="start" goto set_script_cmd
IF "%1"=="stop" goto set_script_cmd
IF "%1"=="restart" goto set_script_cmd
IF "%1"=="auth" goto set_script_cmd

REM Handle all other commands by simply running SolrCLI
"%JAVA%" %SOLR_SSL_OPTS% %AUTHC_OPTS% %SOLR_ZK_CREDS_AND_ACLS% %SOLR_TOOL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" ^
  -Dlog4j.configurationFile="file:///%DEFAULT_SERVER_DIR%\resources\log4j2-console.xml" ^
  -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
  org.apache.solr.cli.SolrCLI %*
if errorlevel 1 (
   exit /b 1
)
goto done

:set_script_cmd
set SCRIPT_CMD=%1
SHIFT
IF "%SCRIPT_CMD%"=="auth" goto run_auth
goto parse_args

:usage
IF NOT "%SCRIPT_ERROR%"=="" ECHO %SCRIPT_ERROR%
IF "%SCRIPT_CMD%"=="start" goto start_usage
IF "%SCRIPT_CMD%"=="restart" goto start_usage
IF "%SCRIPT_CMD%"=="stop" goto stop_usage
REM Should not be reachable, but just in case
goto err

:start_usage
@echo.
@echo Usage: solr %SCRIPT_CMD% [-f] [--user-managed] [--host hostname] [-p port] [--server-dir directory] [-z zkHost] [-m memory] [-e example] [--solr-home solr.solr.home] [--data-home solr.data.home] [--jvm-opts "jvm-opts"] [--verbose]
@echo.
@echo   -f/--foreground Start Solr in foreground; default starts Solr in the background
@echo                   and sends stdout / stderr to solr-PORT-console.log
@echo.
@echo   --user-managed Start Solr in user managed aka standalone mode"
@echo                   See the Ref Guide for more details: https://solr.apache.org/guide/solr/latest/deployment-guide/cluster-types.html
@echo.
@echo   --host host   Specify the hostname for this Solr instance
@echo.
@echo   -p/--port port Specify the port to start the Solr HTTP listener on; default is 8983
@echo                   The specified port (SOLR_PORT) will also be used to determine the stop port
@echo                   STOP_PORT=(%%SOLR_PORT%%-1000) and JMX RMI listen port RMI_PORT=(%%SOLR_PORT%%+10000).
@echo                   For instance, if you set -p 8985, then the STOP_PORT=7985 and RMI_PORT=18985
@echo.
@echo   --server-dir dir Specify the Solr server directory; defaults to server
@echo.
@echo   -z zkHost     Zookeeper connection string; only used when running in SolrCloud mode using -c
@echo                   If neither ZK_HOST is defined in solr.in.cmd nor the -z parameter is specified,
@echo                   an embedded ZooKeeper instance will be launched.
@echo                   Set the ZK_CREATE_CHROOT environment variable to true if your ZK host has a chroot path, and you want to create it automatically."
@echo.
@echo   -m/--memory memory Sets the min (-Xms) and max (-Xmx) heap size for the JVM, such as: -m 4g
@echo                   results in: -Xms4g -Xmx4g; by default, this script sets the heap size to 512m
@echo.
@echo   --solr-home dir  Sets the solr.solr.home system property; Solr will create core directories under
@echo                   this directory. This allows you to run multiple Solr instances on the same host
@echo                   while reusing the same server directory set using the --server-dir parameter. If set, the
@echo                   specified directory should contain a solr.xml file, unless solr.xml exists in Zookeeper.
@echo                   This parameter is ignored when running examples (-e), as the solr.solr.home depends
@echo                   on which example is run. The default value is server/solr. If passed a relative dir
@echo                   validation with the current dir will be done before trying the default server/^<dir^>
@echo.
@echo   --data-home dir Sets the solr.data.home system property, where Solr will store index data in ^<instance_dir^>/data subdirectories.
@echo                   If not set, Solr uses solr.solr.home for both config and data.
@echo.
@echo   -e/--example name Name of the example to run; available examples:
@echo       cloud:          SolrCloud example
@echo       techproducts:   Comprehensive example illustrating many of Solr's core capabilities
@echo       schemaless:     Schema-less example (schema is inferred from data during indexing)
@echo       films:          Example of starting with _default configset and defining explicit fields dynamically
@echo.
@echo   --jvm-opts opts Additional parameters to pass to the JVM when starting Solr, such as to setup
@echo                 Java debug options. For example, to enable a Java debugger to attach to the Solr JVM
@echo                 you could pass: --jvm-opts "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=18983"
@echo                 In most cases, you should wrap the additional parameters in double quotes.
@echo.
@echo   -j opts       Additional parameters to pass to Jetty when starting Solr.
@echo                 For example, to add configuration folder that jetty should read
@echo                 you could pass: -j "--include-jetty-dir=/etc/jetty/custom/server/"
@echo                 In most cases, you should wrap the additional parameters in double quotes.
@echo.
@echo   -y/--no-prompt Don't prompt for input; accept all defaults when running examples that accept user input
@echo.
@echo   --verbose and -q/--quiet Verbose or quiet logging. Sets default log level to DEBUG or WARN instead of INFO
@echo.
goto done

:stop_usage
@echo.
@echo Usage: solr stop [-k key] [-p port] [--verbose]
@echo.
@echo  -k key         Stop key; default is solrrocks
@echo.
@echo  -p port        Specify the port the Solr HTTP listener is bound to
@echo.
@echo  --all          Find and stop all running Solr servers on this host
@echo.
@echo  --verbose      Verbose messages from this script
@echo.
@echo  NOTE: To see if any Solr servers are running, do: solr status
@echo.
goto done

REM Parse arguments for special commands (start, stop, restart)
:parse_args

set "arg=%~1"
set "firstTwo=%arg:~0,2%"

REM In case no arguments left, run special command
IF [%1]==[] goto run_special_command

REM Skip start / restart arguments if stop command
IF "%SCRIPT_CMD%"=="stop" goto parse_stop_args

:parse_start_args
IF "%1"=="-f" goto set_foreground_mode
IF "%1"=="--foreground" goto set_foreground_mode
IF "%1"=="--verbose" goto set_verbose
IF "%1"=="-q" goto set_warn
IF "%1"=="--quiet" goto set_warn
IF "%1"=="--user-managed" goto set_user_managed_mode
IF "%1"=="--server-dir" goto set_server_dir
IF "%1"=="--solr-home" goto set_solr_home_dir
IF "%1"=="--data-home" goto set_solr_data_dir
IF "%1"=="-e" goto set_example
IF "%1"=="--example" goto set_example
IF "%1"=="--host" goto set_host
IF "%1"=="-m" goto set_memory
IF "%1"=="--memory" goto set_memory
IF "%1"=="-z" goto set_zookeeper
IF "%1"=="--zk-host" goto set_zookeeper
IF "%1"=="-s" goto set_solr_url
IF "%1"=="--solr-url" goto set_solr_url
IF "%1"=="--jvm-opts" goto set_jvm_opts
IF "%1"=="-j" goto set_addl_jetty_config
IF "%1"=="--jettyconfig" goto set_addl_jetty_config
IF "%1"=="-y" goto set_noprompt
IF "%1"=="--no-prompt" goto set_noprompt

REM Skip stop arg parsing if not stop command
IF NOT "%SCRIPT_CMD%"=="stop" goto parse_general_args

:parse_stop_args
IF "%1"=="-k" goto set_stop_key
IF "%1"=="--key" goto set_stop_key
IF "%1"=="--all" goto set_stop_all

:parse_general_args

REM Print usage of command in case help option included
IF "%1"=="--help" goto usage
IF "%1"=="-h" goto usage

REM other args supported by all special commands
IF "%1"=="-p" goto set_port
IF "%1"=="--port" goto set_port
IF "%firstTwo%"=="-D" goto set_passthru

REM Argument not supported / found
IF NOT "%1"=="" goto invalid_cmd_line
REM Not reachable, but just in case
goto invalid_cmd_line

:set_foreground_mode
set FG=1
SHIFT
goto parse_args

:set_verbose
set verbose=1
set SOLR_LOG_LEVEL=DEBUG
set "PASS_TO_RUN_EXAMPLE=--verbose !PASS_TO_RUN_EXAMPLE!"
SHIFT
goto parse_args

:set_warn
set SOLR_LOG_LEVEL=WARN
SHIFT
goto parse_args

:set_user_managed_mode
set SOLR_MODE=user-managed
SHIFT
goto parse_args

:set_server_dir

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Directory name is required!
  goto invalid_cmd_line
)
set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected directory but found %2 instead!
  goto invalid_cmd_line
)

REM See if they are using a short-hand name relative from the Solr tip directory
IF EXIST "%SOLR_TIP%\%~2" (
  set "SOLR_SERVER_DIR=%SOLR_TIP%\%~2"
) ELSE (
  set "SOLR_SERVER_DIR=%~2"
)
SHIFT
SHIFT
goto parse_args

:set_solr_home_dir

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Directory name is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected directory but found %2 instead!
  goto invalid_cmd_line
)
set "SOLR_HOME=%~2"
SHIFT
SHIFT
goto parse_args

:set_solr_data_dir

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Directory name is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected directory but found %2 instead!
  goto invalid_cmd_line
)
set "SOLR_DATA_HOME=%~2"
SHIFT
SHIFT
goto parse_args

:set_example

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Example name is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected example name but found %2 instead!
  goto invalid_cmd_line
)

set EXAMPLE=%~2
SHIFT
SHIFT
goto parse_args

:set_memory

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Memory setting is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected memory setting but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_HEAP=%~2
set "PASS_TO_RUN_EXAMPLE=-m %~2 !PASS_TO_RUN_EXAMPLE!"
SHIFT
SHIFT
goto parse_args

:set_host
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Hostname is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected hostname but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_HOST=%~2
set "PASS_TO_RUN_EXAMPLE=--host %~2 !PASS_TO_RUN_EXAMPLE!"
SHIFT
SHIFT
goto parse_args

:set_port
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Port is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected port but found %2 instead!
  goto invalid_cmd_line
)

set SOLR_PORT=%~2
set "PASS_TO_RUN_EXAMPLE=-p %~2 !PASS_TO_RUN_EXAMPLE!"
SHIFT
SHIFT
goto parse_args

:set_stop_key
set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Stop key is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected stop key but found %2 instead!
  goto invalid_cmd_line
)
set STOP_KEY=%~2
SHIFT
SHIFT
goto parse_args

:set_stop_all
set STOP_ALL=1
SHIFT
goto parse_args

:set_zookeeper

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Zookeeper connection string is required!
  goto invalid_cmd_line
)

set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  set SCRIPT_ERROR=Expected Zookeeper connection string but found %2 instead!
  goto invalid_cmd_line
)

set "ZK_HOST=%~2"
set "PASS_TO_RUN_EXAMPLE=-z %~2 !PASS_TO_RUN_EXAMPLE!"
SHIFT
SHIFT
goto parse_args

:set_solr_url

set "arg=%~2"
IF "%arg%"=="" (
  set SCRIPT_ERROR=Solr url string is required!
  goto invalid_cmd_line
)

set "ZK_SOLR_URL=%~2"
SHIFT
SHIFT
goto parse_args

:set_jvm_opts
set "arg=%~2"
set "SOLR_ADDL_ARGS=%~2"
IF "%SOLR_ADDL_ARGS%"=="" (
  set "EMPTY_ADDL_JVM_ARGS=true"
)
SHIFT
SHIFT
goto parse_args

:set_addl_jetty_config
set "arg=%~2"
set "SOLR_JETTY_ADDL_CONFIG=%~2"
SHIFT
SHIFT
goto parse_args

:set_passthru
set "PASSTHRU_KEY=%~1"
set "PASSTHRU_VALUES="

SHIFT
:repeat_passthru
set "arg=%~1"
if "%arg%"=="" goto end_passthru
set firstChar=%arg:~0,1%
IF "%firstChar%"=="-" (
  goto end_passthru
)

if defined PASSTHRU_VALUES (
    set "PASSTHRU_VALUES=%PASSTHRU_VALUES%,%arg%"
) else (
    set "PASSTHRU_VALUES=%arg%"
)
SHIFT
goto repeat_passthru

:end_passthru
set "PASSTHRU=%PASSTHRU_KEY%=%PASSTHRU_VALUES%"

IF NOT "%SOLR_OPTS%"=="" (
  set "SOLR_OPTS=%SOLR_OPTS% %PASSTHRU%"
) ELSE (
  set "SOLR_OPTS=%PASSTHRU%"
)
set "PASS_TO_RUN_EXAMPLE=%PASSTHRU% !PASS_TO_RUN_EXAMPLE!"

goto parse_args

:set_noprompt
set NO_USER_PROMPT=1
set "PASS_TO_RUN_EXAMPLE=--no-prompt !PASS_TO_RUN_EXAMPLE!"

SHIFT
goto parse_args

REM Handle invalid arguments passed to special commands (start, stop, restart)
:invalid_cmd_line
@echo.
IF "!SCRIPT_ERROR!"=="" (
  @echo Invalid command-line option: %1
) ELSE (
  @echo ERROR: !SCRIPT_ERROR!
)
@echo.
IF "%SCRIPT_CMD%"=="start" (
  goto start_usage
) ELSE IF "%SCRIPT_CMD%"=="restart" (
  goto start_usage
) ELSE IF "%SCRIPT_CMD%"=="stop" (
  goto stop_usage
)
REM Not reachable, but just in case
goto err

REM Process special commands (start, stop, restart)
:run_special_command

IF "%verbose%"=="1" (
  CALL :safe_echo "Using Solr root directory: %SOLR_TIP%"
  CALL :safe_echo "Using Java: %JAVA%"
  "%JAVA%" -version
  @echo.
)

IF NOT "%SOLR_HOST%"=="" (
  set SOLR_HOST_ARG=-Dhost=%SOLR_HOST%
) ELSE IF "%SOLR_JETTY_HOST%"=="" (
  set "SOLR_HOST_ARG=-Dhost=localhost"
) ELSE IF "%SOLR_JETTY_HOST%"=="127.0.0.1" (
  set "SOLR_HOST_ARG=-Dhost=localhost"
) ELSE (
  set SOLR_HOST_ARG=
)

set SCRIPT_SOLR_OPTS=

REM Default placement plugin
IF DEFINED SOLR_PLACEMENTPLUGIN_DEFAULT (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.placementplugin.default=%SOLR_PLACEMENTPLUGIN_DEFAULT%"
)

REM Remote streaming and stream body
IF "%SOLR_ENABLE_REMOTE_STREAMING%"=="true" (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.enableRemoteStreaming=true"
)
IF "%SOLR_ENABLE_STREAM_BODY%"=="true" (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.enableStreamBody=true"
)

IF "%SOLR_SERVER_DIR%"=="" set "SOLR_SERVER_DIR=%DEFAULT_SERVER_DIR%"

IF NOT EXIST "%SOLR_SERVER_DIR%" (
  set "SCRIPT_ERROR=Solr server directory %SOLR_SERVER_DIR% not found!"
  goto err
)

IF "%STOP_KEY%"=="" set STOP_KEY=solrrocks

IF NOT "%EXAMPLE%"=="" (
  REM Run the requested example

  "%JAVA%" %SOLR_SSL_OPTS% %AUTHC_OPTS% %SOLR_ZK_CREDS_AND_ACLS% %SOLR_TOOL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" ^
    -Dlog4j.configurationFile="file:///%DEFAULT_SERVER_DIR%\resources\log4j2-console.xml" ^
    -Dsolr.install.symDir="%SOLR_TIP%" ^
    -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
    org.apache.solr.cli.SolrCLI run_example --script "%SDIR%\solr.cmd" -e %EXAMPLE% --server-dir "%SOLR_SERVER_DIR%" ^
    --url-scheme !SOLR_URL_SCHEME! !PASS_TO_RUN_EXAMPLE!

  REM End of run_example
  goto done
)

set IS_RESTART=0
IF "%SCRIPT_CMD%"=="restart" (
  IF "%SOLR_PORT%"=="" (
    set "SCRIPT_ERROR=Must specify the port when trying to restart Solr."
    goto err
  )
  set SCRIPT_CMD=stop
  set IS_RESTART=1
)

REM Skipt to start if not stop or restart (that executes stop first)
IF "%SCRIPT_CMD%"=="start" goto start_solr

@REM stop logic here
:stop_solr
IF "%SOLR_STOP_WAIT%"=="" (
  set SOLR_STOP_WAIT=180
)
IF "%SOLR_PORT%"=="" (
  IF "%STOP_ALL%"=="1" (
    REM Stop all running Solr instances
    set found_it=0
    for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
      set SOME_SOLR_PORT=
      For /F "delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
      if NOT "!SOME_SOLR_PORT!"=="" (
        for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":0 " ^| find ":!SOME_SOLR_PORT! "') do (
          @REM j is the ip:port and k is the pid
          IF NOT "%%k"=="0" (
            IF "%%j"=="%SOLR_JETTY_HOST%:!SOME_SOLR_PORT!" (
              set found_it=1
              @echo Stopping Solr process %%k running on port !SOME_SOLR_PORT!
              IF "%STOP_PORT%"=="" (
                set /A LOCAL_STOP_PORT=!SOME_SOLR_PORT! - 1000
              ) else (
                set LOCAL_STOP_PORT=%STOP_PORT%
              )
              "%JAVA%" %SOLR_SSL_OPTS% -Djetty.home="%SOLR_SERVER_DIR%" -jar "%SOLR_SERVER_DIR%\start.jar" STOP.PORT=!LOCAL_STOP_PORT! STOP.KEY=%STOP_KEY% --stop
              del "%SOLR_TIP%"\bin\solr-!SOME_SOLR_PORT!.port
              REM wait for the process to terminate
              CALL :wait_for_process_exit %%k !SOLR_STOP_WAIT!
              REM Kill it if it is still running after the graceful shutdown
              IF EXIST "%JAVA_HOME%\bin\jstack.exe" (
                qprocess "%%k" >nul 2>nul && "%JAVA_HOME%\bin\jstack.exe" %%k && taskkill /f /PID %%k
              ) else (
                qprocess "%%k" >nul 2>nul && taskkill /f /PID %%k
              )
            )
          )
        )
      )
    )
    if "!found_it!"=="0" echo No Solr nodes found to stop.
  ) ELSE (
    set "SCRIPT_ERROR=Must specify the port when trying to stop Solr, or use --all to stop all running nodes on this host."
    goto err
  )
) ELSE (
  REM Stop Solr running on specific port
  set found_it=0
  For /f "tokens=2,5" %%M in ('netstat -nao ^| find "TCP " ^| find ":0 " ^| find ":%SOLR_PORT% "') do (
    IF NOT "%%N"=="0" (
      IF "%%M"=="%SOLR_JETTY_HOST%:%SOLR_PORT%" (
        set found_it=1
        @echo Stopping Solr process %%N running on port %SOLR_PORT%
        IF "%STOP_PORT%"=="" set /A STOP_PORT=%SOLR_PORT% - 1000
        "%JAVA%" %SOLR_SSL_OPTS% %SOLR_TOOL_OPTS% -Djetty.home="%SOLR_SERVER_DIR%" -jar "%SOLR_SERVER_DIR%\start.jar" %SOLR_JETTY_CONFIG% STOP.PORT=!STOP_PORT! STOP.KEY=%STOP_KEY% --stop
        del "%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port
        REM wait for the process to terminate
        CALL :wait_for_process_exit %%N !SOLR_STOP_WAIT!
        REM Kill it if it is still running after the graceful shutdown
        IF EXIST "%JAVA_HOME%\bin\jstack.exe" (
          qprocess "%%N" >nul 2>nul && "%JAVA_HOME%\bin\jstack.exe" %%N && taskkill /f /PID %%N
        ) else (
          qprocess "%%N" >nul 2>nul && taskkill /f /PID %%N
        )
      )
    )
  )
  if "!found_it!"=="0" echo No Solr found running on port %SOLR_PORT%
)

IF "!IS_RESTART!"=="0" goto done

REM Clean state here, continue with starting (start or restart)
set SCRIPT_CMD=start

:start_solr
REM Prepare for starting Solr
IF "%SOLR_HOME%"=="" set "SOLR_HOME=%SOLR_SERVER_DIR%\solr"
IF EXIST "%cd%\%SOLR_HOME%" set "SOLR_HOME=%cd%\%SOLR_HOME%"

IF NOT EXIST "%SOLR_HOME%\" (
  IF EXIST "%SOLR_SERVER_DIR%\%SOLR_HOME%" (
    set "SOLR_HOME=%SOLR_SERVER_DIR%\%SOLR_HOME%"
  ) ELSE (
    set "SCRIPT_ERROR=Solr home directory %SOLR_HOME% not found!"
    goto err
  )
)

@REM Handle overriding where logs are written to
IF [%SOLR_LOGS_DIR%] == [] (
  set "SOLR_LOGS_DIR=%SOLR_SERVER_DIR%\logs"
) ELSE (
  set SOLR_LOGS_DIR=%SOLR_LOGS_DIR:"=%
)

set "EXAMPLE_DIR=%SOLR_TIP%\example"
set TMP_SOLR_HOME=!SOLR_HOME:%EXAMPLE_DIR%=!
IF NOT "%TMP_SOLR_HOME%"=="%SOLR_HOME%" (
  set "SOLR_LOGS_DIR=%SOLR_HOME%\..\logs"
  set "LOG4J_CONFIG=%SOLR_SERVER_DIR%\resources\log4j2.xml"
)


IF "%SOLR_PORT%"=="" set SOLR_PORT=8983
IF "%STOP_PORT%"=="" set /A STOP_PORT=%SOLR_PORT% - 1000

IF DEFINED SOLR_PORT_ADVERTISE (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.port.advertise=%SOLR_PORT_ADVERTISE%"
)

IF DEFINED SOLR_JETTY_HOST (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.jetty.host=%SOLR_JETTY_HOST%"
)

IF DEFINED SOLR_ZK_EMBEDDED_HOST (
  set "SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% -Dsolr.zk.embedded.host=%SOLR_ZK_EMBEDDED_HOST%"
)

REM Make sure Solr is not running using netstat
For /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":0 " ^| find ":%SOLR_PORT% "') do (
  IF NOT "%%k"=="0" (
    IF "%%j"=="%SOLR_JETTY_HOST%:%SOLR_PORT%" (
      set "SCRIPT_ERROR=Process %%k is already listening on port %SOLR_PORT%. If this is Solr, please stop it first before starting (or use restart). If this is not Solr, then please choose a different port using -p PORT"
      goto err
    )
  )
)

IF "%EMPTY_ADDL_JVM_ARGS%"=="true" (
  set "SCRIPT_ERROR=JVM options are required when using the -a or --jvm-opts option!"
  goto err
)

@REM determine if -server flag is supported by current JVM
"%JAVA%" -server -version > nul 2>&1
IF ERRORLEVEL 1 (
  set IS_JDK=false
  set "SERVEROPT="
  @echo WARNING: You are using a JRE without support for -server option. Please upgrade to latest JDK for best performance
  @echo.
) ELSE (
  set IS_JDK=true
  set "SERVEROPT=-server"
)

IF NOT "%ZK_HOST%"=="" set SOLR_MODE=solrcloud
IF "%SOLR_MODE%"=="" set SOLR_MODE=solrcloud

IF "%SOLR_MODE%"=="solrcloud" (
  IF "%ZK_CLIENT_TIMEOUT%"=="" set "ZK_CLIENT_TIMEOUT=30000"

  set "CLOUD_MODE_OPTS=-DzkClientTimeout=!ZK_CLIENT_TIMEOUT!"

  IF NOT "%SOLR_WAIT_FOR_ZK%"=="" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DwaitForZk=%SOLR_WAIT_FOR_ZK%"
  )

  IF NOT "%SOLR_DELETE_UNKNOWN_CORES%"=="" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -Dsolr.deleteUnknownCores=%SOLR_DELETE_UNKNOWN_CORES%"
  )

  IF NOT "%ZK_HOST%"=="" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DzkHost=%ZK_HOST%"
  ) ELSE (
    IF %SOLR_PORT% GTR 64535 (
      set "SCRIPT_ERROR=ZK_HOST is not set and Solr port is %SOLR_PORT%, which would result in an invalid embedded Zookeeper port!"
      goto err
    )
    IF "%verbose%"=="1" echo Configuring SolrCloud to launch an embedded Zookeeper using -DzkRun
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DzkRun"
  )

  IF NOT "%ZK_CREATE_CHROOT%"=="" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -DcreateZkChroot=%ZK_CREATE_CHROOT%"
  )

  IF "%SOLR_SOLRXML_REQUIRED%"=="true" (
    set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -Dsolr.solrxml.required=true"
  )

  IF EXIST "%SOLR_HOME%\collection1\core.properties" set "CLOUD_MODE_OPTS=!CLOUD_MODE_OPTS! -Dbootstrap_confdir=./solr/collection1/conf -Dcollection.configName=myconf -DnumShards=1"
) ELSE (
  REM change Cloud mode to User Managed mode with flag
  set "CLOUD_MODE_OPTS="
  IF NOT EXIST "%SOLR_HOME%\solr.xml" (
    IF "%SOLR_SOLRXML_REQUIRED%"=="true" (
      set "SCRIPT_ERROR=Solr home directory %SOLR_HOME% must contain solr.xml!"
      goto err
    )
  )
)

REM Exit if old syntax found
IF DEFINED SOLR_IP_BLACKLIST (
  set SCRIPT_ERROR=SOLR_IP_BLACKLIST and SOLR_IP_WHITELIST are no longer supported. Please use SOLR_IP_ALLOWLIST and SOLR_IP_DENYLIST instead.
  goto err
)
IF DEFINED SOLR_IP_WHITELIST (
  set SCRIPT_ERROR=SOLR_IP_BLACKLIST and SOLR_IP_WHITELIST are no longer supported. Please use SOLR_IP_ALLOWLIST and SOLR_IP_DENYLIST instead.
  goto err
)

REM IP-based access control
set IP_ACL_OPTS=-Dsolr.jetty.inetaccess.includes="%SOLR_IP_ALLOWLIST%" ^
-Dsolr.jetty.inetaccess.excludes="%SOLR_IP_DENYLIST%"

REM These are useful for attaching remove profilers like VisualVM/JConsole
IF "%ENABLE_REMOTE_JMX_OPTS%"=="true" (
  IF "!RMI_PORT!"=="" (
    set /A RMI_PORT=%SOLR_PORT%+10000
    IF !RMI_PORT! GTR 65535 (
      set "SCRIPT_ERROR=RMI_PORT is !RMI_PORT!, which is invalid!"
      goto err
    )
  )
  set REMOTE_JMX_OPTS=-Dcom.sun.management.jmxremote ^
-Dcom.sun.management.jmxremote.local.only=false ^
-Dcom.sun.management.jmxremote.ssl=false ^
-Dcom.sun.management.jmxremote.authenticate=false ^
-Dcom.sun.management.jmxremote.port=!RMI_PORT! ^
-Dcom.sun.management.jmxremote.rmi.port=!RMI_PORT!

  IF NOT "%SOLR_HOST%"=="" set REMOTE_JMX_OPTS=!REMOTE_JMX_OPTS! -Djava.rmi.server.hostname=%SOLR_HOST%
) ELSE (
  set REMOTE_JMX_OPTS=
)

IF "%SOLR_SECURITY_MANAGER_ENABLED%"=="true" (
  set SECURITY_MANAGER_OPTS=-Djava.security.manager ^
-Djava.security.policy="%SOLR_SERVER_DIR%\etc\security.policy" ^
-Djava.security.properties="%SOLR_SERVER_DIR%\etc\security.properties" ^
-Dsolr.internal.network.permission=*
)

REM Enable ADMIN UI by default, and give the option for users to disable it
IF "%SOLR_ADMIN_UI_DISABLED%"=="true" (
  set DISABLE_ADMIN_UI="true"
) else (
  set DISABLE_ADMIN_UI="false"
)

IF NOT "%SOLR_HEAP%"=="" set SOLR_JAVA_MEM=-Xms%SOLR_HEAP% -Xmx%SOLR_HEAP%
IF "%SOLR_JAVA_MEM%"=="" set SOLR_JAVA_MEM=-Xms512m -Xmx512m
IF "%SOLR_JAVA_STACK_SIZE%"=="" set SOLR_JAVA_STACK_SIZE=-Xss256k
set SCRIPT_SOLR_OPTS=%SOLR_JAVA_STACK_SIZE% %SCRIPT_SOLR_OPTS%
IF "%SOLR_TIMEZONE%"=="" set SOLR_TIMEZONE=UTC

IF "%GC_TUNE%"=="" (
  set GC_TUNE=-XX:+UseG1GC ^
    -XX:+PerfDisableSharedMem ^
    -XX:+ParallelRefProcEnabled ^
    -XX:MaxGCPauseMillis=250 ^
    -XX:+UseLargePages ^
    -XX:+AlwaysPreTouch ^
    -XX:+ExplicitGCInvokesConcurrent
)

REM Add vector optimizations module
set SCRIPT_SOLR_OPTS=%SCRIPT_SOLR_OPTS% --add-modules jdk.incubator.vector

IF "%GC_LOG_OPTS%"=="" (
  set GC_LOG_OPTS="-Xlog:gc*"
)

IF "%verbose%"=="1" (
  @echo Starting Solr using the following settings:
  CALL :safe_echo "    JAVA              = %JAVA%"
  CALL :safe_echo "    SOLR_SERVER_DIR   = %SOLR_SERVER_DIR%"
  CALL :safe_echo "    SOLR_HOME         = %SOLR_HOME%"
  @echo     SOLR_HOST         = %SOLR_HOST%
  @echo     SOLR_PORT         = %SOLR_PORT%
  @echo     STOP_PORT         = %STOP_PORT%
  @echo     SOLR_JAVA_MEM     = %SOLR_JAVA_MEM%
  @echo     GC_TUNE           = !GC_TUNE!
  @echo     GC_LOG_OPTS       = %GC_LOG_OPTS%
  @echo     SOLR_TIMEZONE     = %SOLR_TIMEZONE%

  IF "%SOLR_MODE%"=="solrcloud" (
    @echo     CLOUD_MODE_OPTS   = %CLOUD_MODE_OPTS%
  )

  IF NOT "%SOLR_OPTS%"=="" (
    CALL :safe_echo "    SOLR_OPTS (USER)   = %SOLR_OPTS%"
  )

  IF NOT "%SCRIPT_SOLR_OPTS%"=="" (
    CALL :safe_echo "    SOLR_OPTS (SCRIPT) = %SCRIPT_SOLR_OPTS%"
  )

  IF NOT "%SOLR_ADDL_ARGS%"=="" (
    CALL :safe_echo "    SOLR_ADDL_ARGS     = %SOLR_ADDL_ARGS%"
  )

  IF NOT "%SOLR_JETTY_ADDL_CONFIG%"=="" (
    CALL :safe_echo "    SOLR_JETTY_ADDL_CONFIG = %SOLR_JETTY_ADDL_CONFIG%"
  )

  IF "%ENABLE_REMOTE_JMX_OPTS%"=="true" (
    @echo     RMI_PORT          = !RMI_PORT!
    @echo     REMOTE_JMX_OPTS   = %REMOTE_JMX_OPTS%
  )

  IF NOT "%SOLR_LOG_LEVEL%"=="" (
    @echo     SOLR_LOG_LEVEL    = !SOLR_LOG_LEVEL!
  )

  IF NOT "%SOLR_DATA_HOME%"=="" (
    @echo     SOLR_DATA_HOME    = !SOLR_DATA_HOME!
  )

  @echo.
)

set START_OPTS=-Duser.timezone=%SOLR_TIMEZONE%
REM '-OmitStackTraceInFastThrow' ensures stack traces in errors,
REM users who don't care about useful error msgs can override in SOLR_OPTS with +OmitStackTraceInFastThrow
set START_OPTS=%START_OPTS% -XX:-OmitStackTraceInFastThrow
REM '+CrashOnOutOfMemoryError' ensures that Solr crashes whenever
REM OOME is thrown. Program operation after OOME is unpredictable.
set START_OPTS=%START_OPTS% -XX:+CrashOnOutOfMemoryError
set START_OPTS=%START_OPTS% -XX:ErrorFile="%SOLR_LOGS_DIR%\jvm_crash_%%p.log"
set START_OPTS=%START_OPTS% !GC_TUNE! %GC_LOG_OPTS%
set START_OPTS=%START_OPTS% -DdisableAdminUI=%DISABLE_ADMIN_UI%
IF NOT "!CLOUD_MODE_OPTS!"=="" set "START_OPTS=%START_OPTS% !CLOUD_MODE_OPTS!"
IF NOT "!IP_ACL_OPTS!"=="" set "START_OPTS=%START_OPTS% !IP_ACL_OPTS!"
IF NOT "!REMOTE_JMX_OPTS!"=="" set "START_OPTS=%START_OPTS% !REMOTE_JMX_OPTS!"
IF NOT "%SOLR_ADDL_ARGS%"=="" set "START_OPTS=%START_OPTS% %SOLR_ADDL_ARGS%"
IF NOT "%SOLR_HOST_ARG%"=="" set "START_OPTS=%START_OPTS% %SOLR_HOST_ARG%"
IF NOT "%SCRIPT_SOLR_OPTS%"=="" set "START_OPTS=%START_OPTS% %SCRIPT_SOLR_OPTS%"
IF NOT "%SOLR_OPTS_INTERNAL%"=="" set "START_OPTS=%START_OPTS% %SOLR_OPTS_INTERNAL%"
IF NOT "!SECURITY_MANAGER_OPTS!"=="" set "START_OPTS=%START_OPTS% !SECURITY_MANAGER_OPTS!"
IF "%SOLR_SSL_ENABLED%"=="true" (
  set "SSL_PORT_PROP=-Dsolr.jetty.https.port=%SOLR_PORT%"
  set "START_OPTS=%START_OPTS% %SOLR_SSL_OPTS% !SSL_PORT_PROP!"
)

set SOLR_LOGS_DIR_QUOTED="%SOLR_LOGS_DIR%"
set SOLR_DATA_HOME_QUOTED="%SOLR_DATA_HOME%"

set "START_OPTS=%START_OPTS% -Dsolr.log.dir=%SOLR_LOGS_DIR_QUOTED%"
IF NOT "%SOLR_DATA_HOME%"=="" set "START_OPTS=%START_OPTS% -Dsolr.data.home=%SOLR_DATA_HOME_QUOTED%"
IF NOT DEFINED LOG4J_CONFIG set "LOG4J_CONFIG=%SOLR_SERVER_DIR%\resources\log4j2.xml"

REM This should be the last thing added to START_OPTS, so that users can override as much as possible
IF NOT "%SOLR_OPTS%"=="" set "START_OPTS=%START_OPTS% %SOLR_OPTS%"

cd /d "%SOLR_SERVER_DIR%"

IF NOT EXIST "%SOLR_LOGS_DIR%" (
  mkdir "%SOLR_LOGS_DIR%"
)
copy /Y NUL "%SOLR_LOGS_DIR%\.writable" > NUL 2>&1 && set WRITEOK=1
IF DEFINED WRITEOK (
  del "%SOLR_LOGS_DIR%\.writable"
) else (
  echo "ERROR: Logs directory %SOLR_LOGS_DIR% is not writable or could not be created. Exiting"
  GOTO :eof
)
echo " contexts etc lib modules resources scripts solr solr-webapp " > "%TEMP%\solr-pattern.txt"
findstr /i /C:" %SOLR_LOGS_DIR% " "%TEMP%\solr-pattern.txt" 1>nul
if %ERRORLEVEL% == 0 (
  echo "ERROR: Logs directory %SOLR_LOGS_DIR% is invalid. Reserved for the system. Exiting"
  GOTO :eof
)

IF NOT EXIST "%SOLR_SERVER_DIR%\tmp" (
  mkdir "%SOLR_SERVER_DIR%\tmp"
)

IF "%DEFAULT_CONFDIR%"=="" set "DEFAULT_CONFDIR=%SOLR_SERVER_DIR%\solr\configsets\_default\conf"

IF "%FG%"=="1" (
  REM run solr in the foreground
  title "Solr-%SOLR_PORT%"
  echo %SOLR_PORT%>"%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port
  "%JAVA%" %SERVEROPT% %SOLR_JAVA_MEM% %START_OPTS% ^
    -Dlog4j.configurationFile="%LOG4J_CONFIG%" -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -Dsolr.install.symDir="%SOLR_TIP%" -Dsolr.default.confdir="%DEFAULT_CONFDIR%" ^
    -Djetty.port=%SOLR_PORT% -Djetty.home="%SOLR_SERVER_DIR%" ^
    -Djava.io.tmpdir="%SOLR_SERVER_DIR%\tmp" -jar start.jar %SOLR_JETTY_CONFIG% "%SOLR_JETTY_ADDL_CONFIG%"
) ELSE (
  START /B "Solr-%SOLR_PORT%" /D "%SOLR_SERVER_DIR%" ^
    "%JAVA%" %SERVEROPT% %SOLR_JAVA_MEM% %START_OPTS% ^
    -Dlog4j.configurationFile="%LOG4J_CONFIG%" -DSTOP.PORT=!STOP_PORT! -DSTOP.KEY=%STOP_KEY% ^
    -Dsolr.log.muteconsole ^
    -Dsolr.solr.home="%SOLR_HOME%" -Dsolr.install.dir="%SOLR_TIP%" -Dsolr.install.symDir="%SOLR_TIP%" -Dsolr.default.confdir="%DEFAULT_CONFDIR%" ^
    -Djetty.port=%SOLR_PORT% -Djetty.home="%SOLR_SERVER_DIR%" ^
    -Djava.io.tmpdir="%SOLR_SERVER_DIR%\tmp" -jar start.jar %SOLR_JETTY_CONFIG% "%SOLR_JETTY_ADDL_CONFIG%" > "!SOLR_LOGS_DIR!\solr-%SOLR_PORT%-console.log"
  echo %SOLR_PORT%>"%SOLR_TIP%"\bin\solr-%SOLR_PORT%.port

  IF "!SOLR_START_WAIT!"=="" (
    set SOLR_START_WAIT=180
  )
  REM now wait to see Solr come online ...
  "%JAVA%" %SOLR_SSL_OPTS% %AUTHC_OPTS% %SOLR_ZK_CREDS_AND_ACLS% %SOLR_TOOL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" -Dsolr.default.confdir="%DEFAULT_CONFDIR%"^
    -Dlog4j.configurationFile="file:///%DEFAULT_SERVER_DIR%\resources\log4j2-console.xml" ^
    -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
    org.apache.solr.cli.SolrCLI status --max-wait-secs !SOLR_START_WAIT! --solr-url !SOLR_URL_SCHEME!://%SOLR_TOOL_HOST%:%SOLR_PORT%
  IF NOT "!ERRORLEVEL!"=="0" (
      set "SCRIPT_ERROR=Solr did not start or was not reachable. Check the logs for errors."
      goto err
  )
)

goto done

:run_auth
REM Options parsing.
REM Note: With the following technique of parsing, it is not possible
REM       to have an option without a value.
set "AUTH_PARAMS=%1"
set "option="
for %%a in (%*) do (
   if not defined option (
      set arg=%%a
      if "!arg:~0,1!" equ "-" set "option=!arg!"
   ) else (
      set "option!option!=%%a"
      if "!option!" equ "--solr-home" set "SOLR_HOME=%%a"
      if "!option!" equ "--server-dir" set "SOLR_SERVER_DIR=%%a"    
      if not "!option!" equ "--solr-home" if not "!option!" equ "--server-dir" (
        set "AUTH_PARAMS=!AUTH_PARAMS! !option! %%a"
      )
      set "option="
   )
)
IF "%SOLR_SERVER_DIR%"=="" set "SOLR_SERVER_DIR=%DEFAULT_SERVER_DIR%"
IF NOT EXIST "%SOLR_SERVER_DIR%" (
  set "SCRIPT_ERROR=Solr server directory %SOLR_SERVER_DIR% not found!"
  goto err
)
IF "%SOLR_HOME%"=="" set "SOLR_HOME=%SOLR_SERVER_DIR%\solr"
IF EXIST "%cd%\%SOLR_HOME%" set "SOLR_HOME=%cd%\%SOLR_HOME%"
IF NOT EXIST "%SOLR_HOME%\" (
  IF EXIST "%SOLR_SERVER_DIR%\%SOLR_HOME%" (
    set "SOLR_HOME=%SOLR_SERVER_DIR%\%SOLR_HOME%"
  ) ELSE (
    set "SCRIPT_ERROR=Solr home directory %SOLR_HOME% not found!"
    goto err
  )
)

if "!AUTH_PORT!"=="" (
  for /f "usebackq" %%i in (`dir /b "%SOLR_TIP%\bin" ^| findstr /i "^solr-.*\.port$"`) do (
    set SOME_SOLR_PORT=
    For /F "Delims=" %%J In ('type "%SOLR_TIP%\bin\%%i"') do set SOME_SOLR_PORT=%%~J
    if NOT "!SOME_SOLR_PORT!"=="" (
      for /f "tokens=2,5" %%j in ('netstat -aon ^| find "TCP " ^| find ":0 " ^| find ":!SOME_SOLR_PORT! "') do (
        IF NOT "%%k"=="0" set AUTH_PORT=!SOME_SOLR_PORT!
      )
    )
  )
)
"%JAVA%" %SOLR_SSL_OPTS% %AUTHC_OPTS% %SOLR_ZK_CREDS_AND_ACLS% %SOLR_TOOL_OPTS% -Dsolr.install.dir="%SOLR_TIP%" ^
    -Dlog4j.configurationFile="file:///%DEFAULT_SERVER_DIR%\resources\log4j2-console.xml" ^
    -classpath "%DEFAULT_SERVER_DIR%\solr-webapp\webapp\WEB-INF\lib\*;%DEFAULT_SERVER_DIR%\lib\ext\*" ^
    org.apache.solr.cli.SolrCLI auth %AUTH_PARAMS% --solr-include-file "%SOLR_INCLUDE%" --auth-conf-dir "%SOLR_HOME%" ^
    --solr-url !SOLR_URL_SCHEME!://%SOLR_TOOL_HOST%:!AUTH_PORT!
goto done

:err
@echo.
@echo ERROR: !SCRIPT_ERROR!
@echo.
exit /b 1

REM Tests what Java we have and sets some global variables
:resolve_java_info

CALL :resolve_java_vendor

set JAVA_MAJOR_VERSION=0
set JAVA_VERSION_INFO=

FOR /f "usebackq tokens=3" %%a IN (`^""%JAVA%" -version 2^>^&1 ^| findstr "version"^"`) do (
  set JAVA_VERSION_INFO=%%a
  REM Remove surrounding quotes
  set JAVA_VERSION_INFO=!JAVA_VERSION_INFO:"=!

  REM Extract the major Java version, e.g. 7, 8, 9, 10 ...
  for /f "tokens=1,2 delims=._-" %%a in ("!JAVA_VERSION_INFO!") do (
    if %%a GEQ 9 (
      set JAVA_MAJOR_VERSION=%%a
    ) else (
      set JAVA_MAJOR_VERSION=%%b
    )
  )
)
GOTO :eof

REM Set which JVM vendor we have
:resolve_java_vendor
REM OpenJ9 was previously known as IBM J9, this will match both
"%JAVA%" -version 2>&1 | findstr /i /C:"IBM J9" /C:"OpenJ9" > nul
if %ERRORLEVEL% == 1 ( set "JAVA_VENDOR=Oracle" ) else ( set "JAVA_VENDOR=OpenJ9" )

set JAVA_VENDOR_OUT=
GOTO :eof

REM Safe echo which does not mess with () in strings
:safe_echo
set "eout=%1"
set eout=%eout:"=%
echo !eout!
GOTO :eof

:wait_for_process_exit
  REM Wait for a process to stop within a configured period.
  REM The timeout is approximate, since there is no standard
  REM timing mechanism on windows that works in all situations.
  REM ping can be used to provide a good approximation.
  set WAIT_FOR_PID=%1
  set WAIT_TIME=%2

  REM Check every N seconds
  set INTERVAL=1
  set ELAPSED=0

  REM ping timeout needs N + 1 for N seconds elapsed
  set /A PING_INTERVAL=INTERVAL+1

  echo Waiting up to %WAIT_TIME% seconds for process %WAIT_FOR_PID% to exit
  IF %WAIT_TIME% GEQ 30 (
      REM Reduce WAIT_TIME by 5% to roughly account for the drift in time for the untimed actions
      set /A WAIT_TIME=WAIT_TIME*95
      set /A WAIT_TIME=WAIT_TIME/100
  )

  :isalive
  REM Check if process is alive
  qprocess "%WAIT_FOR_PID%" >nul 2>nul
  IF %ERRORLEVEL% EQU 0 (
      ping 127.0.0.1 -n %PING_INTERVAL% > NUL
      set /A ELAPSED=ELAPSED+INTERVAL
      IF %ELAPSED% LEQ %WAIT_TIME% (
        goto isalive
      )
  )
GOTO :eof

:done
ENDLOCAL
exit /b 0
