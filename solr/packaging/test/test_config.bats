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
  solr start
}

teardown_file() {
  common_setup
  solr stop --all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  delete_all_collections
}

@test "setting property" {
  solr create -c COLL_NAME

  run solr config -c COLL_NAME --action set-property --property updateHandler.autoCommit.maxDocs --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "'value' is a required option."
  
  run solr config -c COLL_NAME --action set-property --property updateHandler.autoCommit.maxDocs --value 100 --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"
  
  run solr config -c COLL_NAME --action unset-property --property updateHandler.autoCommit.maxDocs --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully unset-property updateHandler.autoCommit.maxDocs"
}

@test "short form of setting property" {
  solr create -c COLL_NAME

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"
  assert_output --partial "Deprecated for removal since 9.8: Use --value instead"
  assert_output --partial "assuming solr url is http://localhost:${SOLR_PORT}."
}

# This test is to validate the deprecated and non deprecated options for connecting to Solr
@test "connecting to solr via various solr urls and zk hosts" {
  solr create -c COLL_NAME

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100 -solrUrl http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"
  assert_output --partial "Deprecated for removal since 9.7: Use --solr-url instead"

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100 --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100 -zkHost localhost:${ZK_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"
  assert_output --partial "Deprecated for removal since 9.7: Use --zk-host instead"

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100 -z localhost:${ZK_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"

  run solr config -c COLL_NAME --property updateHandler.autoCommit.maxDocs -v 100 --zk-host localhost:${ZK_PORT}
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"

}
