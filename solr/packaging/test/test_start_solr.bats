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

  solr stop --all >/dev/null 2>&1
}

@test "SOLR-11740 check 'solr stop' connection" {
  solr start
  solr start -p ${SOLR2_PORT}
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000
  run bash -c 'solr stop --all 2>&1'
  refute_output --partial 'forcefully killing'
}

@test "stop command for single port" {

  solr start
  solr start -p ${SOLR2_PORT}
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000

  run solr stop -p ${SOLR2_PORT}
  solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

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
