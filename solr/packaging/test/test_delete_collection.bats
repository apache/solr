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

@test "can delete collections" {
  solr create -c "COLL_NAME"
  assert collection_exists "COLL_NAME"

  solr delete -c "COLL_NAME"
  refute collection_exists "COLL_NAME"
}

@test "can delete collections with solrUrl" {
  solr create -c "COLL_NAME"
  assert collection_exists "COLL_NAME"

  solr delete -c "COLL_NAME" -solrUrl http://localhost:${SOLR_PORT}
  refute collection_exists "COLL_NAME"
}

@test "can delete collections with -p and --host" {
  solr create -c "COLL_NAME"
  assert collection_exists "COLL_NAME"

  tmp_port=${SOLR_PORT}
  export SOLR_PORT=
  export SOLR_HOST=
  export SOLR_TOOL_HOST=

  solr delete -c "COLL_NAME" -p ${tmp_port} --host localhost
  refute collection_exists "COLL_NAME"
}

@test "can delete collections with port scanning" {
  solr create -c "COLL_NAME"
  assert collection_exists "COLL_NAME"

  export SOLR_PORT=

  solr delete -c "COLL_NAME"
  refute collection_exists "COLL_NAME"
}

@test "collection delete also deletes zk config" {
  solr create -c "COLL_NAME"
  assert config_exists "COLL_NAME"

  solr delete -c "COLL_NAME"
  refute config_exists "COLL_NAME"
}

@test "deletes accompanying zk config with nondefault name" {
  solr create -c "COLL_NAME" -n "NONDEFAULT_CONFIG_NAME"
  assert config_exists "NONDEFAULT_CONFIG_NAME"

  solr delete -c "COLL_NAME"
  refute config_exists "NONDEFAULT_CONFIG_NAME"
}

@test "deleteConfig option can opt to leave config in zk" {
  solr create -c "COLL_NAME"
  assert config_exists "COLL_NAME"

  solr delete -c "COLL_NAME" -deleteConfig false
  assert config_exists "COLL_NAME"
}
