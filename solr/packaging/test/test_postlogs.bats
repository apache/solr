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

setup_file() {
  common_clean_setup
  solr start -c
}

teardown_file() {
  common_setup
  solr stop -all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  delete_all_collections
}

@test "post solr log into solr via script" {
  run solr create_collection -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"

  run postlogs http://localhost:${SOLR_PORT}/solr/COLL_NAME ${SOLR_LOGS_DIR}/solr.log
  assert_output --partial 'Sending last batch'
  assert_output --partial 'Committed'

  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*"
  refute_output --partial '"numFound":0'
}

@test "post solr log into solr via cli" {
  run solr create_collection -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"

  run solr postlogs -url http://localhost:${SOLR_PORT}/solr/COLL_NAME -rootdir ${SOLR_LOGS_DIR}/solr.log
  assert_output --partial 'Sending last batch'
  assert_output --partial 'Committed'

  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*"
  refute_output --partial '"numFound":0'
}
