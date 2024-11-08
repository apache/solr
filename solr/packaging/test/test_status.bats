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

@test "status detects locally running solr" {
  run solr status
  assert_output --partial "No Solr nodes are running."
  solr start
  run solr status
  assert_output --partial "running on port ${SOLR_PORT}"
  solr stop
  run solr status
  assert_output --partial "No Solr nodes are running."
}

@test "status with --solr-url from user" {
  solr start
  run solr status --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "\"solr_home\":"
  solr stop
}

@test "status with --port from user" {
  solr start
  run solr status --port ${SOLR_PORT}
  assert_output --partial "running on port ${SOLR_PORT}"
  solr stop
}

@test "multiple connection options are prevented" {
  run solr status --port ${SOLR_PORT} --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "The option 's' was specified but an option from this group has already been selected: 'p'"
}

@test "status with invalid --solr-url from user" {
  run solr status --solr-url http://invalidhost:${SOLR_PORT}
  assert_output --partial "Solr at http://invalidhost:${SOLR_PORT} not online"
}

@test "status with --short format" {
  solr start
  run solr status --port ${SOLR_PORT} --short
  assert_output --partial "http://localhost:${SOLR_PORT}/solr"
  solr stop
}
