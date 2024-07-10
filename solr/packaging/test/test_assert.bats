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

@test "assert for non cloud mode" {
  run solr start

  run solr assert --not-cloud http://localhost:${SOLR_PORT}/solr
  assert_output --partial "needn't include Solr's context-root"
  refute_output --partial "ERROR"

  run solr assert --cloud http://localhost:${SOLR_PORT}
  assert_output --partial "ERROR: Solr is not running in cloud mode"

  run ! solr assert --cloud http://localhost:${SOLR_PORT} -e
}

@test "assert for cloud mode" {
  run solr start -c

  run solr assert --cloud http://localhost:${SOLR_PORT}
  refute_output --partial "ERROR"

  run solr assert --not-cloud http://localhost:${SOLR_PORT}/solr
  assert_output --partial "needn't include Solr's context-root"
  assert_output --partial "ERROR: Solr is not running in standalone mode"

  run ! solr assert --not-cloud http://localhost:${SOLR_PORT} -e
}
