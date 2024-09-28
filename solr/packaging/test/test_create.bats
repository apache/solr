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

@test "create for non cloud mode" {
  run solr start
  run solr create -c COLL_NAME
  assert_output --partial "Created new core 'COLL_NAME'"
}

@test "create_core works" {
  run solr start
  run solr create_core -c COLL_NAME
  assert_output --partial "Created new core 'COLL_NAME'"
}

@test "create for cloud mode" {
  run solr start -c
  run solr create -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "ensure -p port parameter is supported" {
  solr start -p ${SOLR2_PORT}
  solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000
  
  run solr create -c COLL_NAME -p ${SOLR2_PORT}
  assert_output --partial "Created new core 'COLL_NAME'"
}

@test "ensure -V verbose parameter is supported" {
  solr start 
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000
  
  run solr create -c COLL_NAME -V
  assert_output --partial "Creating new core 'COLL_NAME' using CoreAdminRequest"
}
