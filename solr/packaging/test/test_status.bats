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

@test "status detects locally running solr" {
  run solr status
  assert_output --partial "No Solr nodes are running."
  solr start
  run solr status
  assert_output --partial "Found 1 Solr nodes:"
  solr stop
  run solr status
  assert_output --partial "No Solr nodes are running."

}

@test "status shell script ignores passed in -solrUrl cli parameter from user" {
  solr start
  run solr status -solrUrl http://localhost:9999/solr
  assert_output --partial "needn't include Solr's context-root"
  assert_output --partial "Found 1 Solr nodes:"
  assert_output --partial "running on port ${SOLR_PORT}"
}

@test "status help flag outputs message highlighting not to use solrUrl." {
  run solr status --help
  assert_output --partial 'usage: status'
  refute_output --partial 'ERROR'
}
