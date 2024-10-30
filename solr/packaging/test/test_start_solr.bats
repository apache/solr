#!/usr/bin/env bats

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

load bats_helper

setup() {
  common_clean_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  solr stop -all >/dev/null 2>&1
}

@test "SOLR-11740 check 'solr stop' connection" {
  solr start
  solr start -p ${SOLR2_PORT}
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT}/solr --timeout 5000
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}

@test "stop command for single port" {

  solr start
  solr start -p ${SOLR2_PORT}
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT}/solr --timeout 5000

  run solr stop -p ${SOLR2_PORT}
  solr assert --not-started http://localhost:${SOLR2_PORT}/solr --timeout 5000
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000

}

@test "start warns about SolrCloud mode if no mode specified" {
  run solr start
  # Default behavior should be user-managed mode in Solr 9
  solr assert --not-cloud http://localhost:${SOLR_PORT} --timeout 5000
  assert_output --partial 'Solr will start in SolrCloud mode by default in version 10'
  assert_output --partial 'you will have to provide --user-managed'
  solr stop
}

@test "start warns about SolrCloud mode if --cloud used" {
  run solr start --cloud
  solr assert --cloud http://localhost:${SOLR_PORT} --timeout 5000
  assert_output --partial 'Solr will start in SolrCloud mode by default in version 10'
  assert_output --partial 'you will no longer need to pass in -c or --cloud flag'
  solr stop
}

@test "start doesn't warn about SolrCloud mode if --user-managed used" {
  run solr start --user-managed
  solr assert --not-cloud http://localhost:${SOLR_PORT} --timeout 5000
  refute_output --partial 'Solr will start in SolrCloud mode by default in version 10'
  solr stop
}

@test "check stop command doesn't hang" {
  # for start/stop/restart we parse the args separate from picking the command
  # which means you don't get an error message for passing a start arg, like --jvm-opts to a stop commmand.

  # Set a timeout duration (in seconds)
  TIMEOUT_DURATION=2

  # make sure that passing a non flag option (i.e --jvm-opts "blah") doesn't hang the stop command.
  run timeout $TIMEOUT_DURATION solr stop --jvm-opts

  assert_output --partial "ERROR: JVM options are required when using the --jvm-opts option!"

}

@test "SOLR-16976 solr starts with remote JMX enabled" {
  export ENABLE_REMOTE_JMX_OPTS=true
  export RMI_PORT=65535 # need to make sure we don't exceed port range so hard code it

  solr start
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

  run cat ${SOLR_LOGS_DIR}/solr-${SOLR_PORT}-console.log
  refute_output --partial 'Exception'
}
