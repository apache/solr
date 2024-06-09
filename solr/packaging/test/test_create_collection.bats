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

@test "create collection" {
  run solr create -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "assuming solr url is http://localhost:${SOLR_PORT}"
}

@test "create collection using solrUrl" {
  run solr create -c COLL_NAME --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Created collection 'COLL_NAME'"
  refute_output --partial "assuming solr url is http://localhost:${SOLR_PORT}"
}

@test "create collection using legacy solrUrl" {
  run solr create -c COLL_NAME --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "needn't include Solr's context-root"
  refute_output --partial "assuming solr url is http://localhost:${SOLR_PORT}"
}

@test "create collection using Zookeeper" {
  run solr create -c COLL_NAME -z localhost:${ZK_PORT}
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "reject d option with invalid config dir" {
  run ! solr create -c COLL_NAME -d /asdf  --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Specified configuration directory /asdf not found!"
}

@test "accept d option with builtin config" {
  run solr create -c COLL_NAME -d sample_techproducts_configs --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "accept d option with explicit path to config" {
  local source_configset_dir="${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs"
  local dest_configset_dir="${BATS_TEST_TMPDIR}/config"
  test -d $source_configset_dir
  cp -r "${source_configset_dir}" "${dest_configset_dir}"

  run solr create -c COLL_NAME -d "${dest_configset_dir}" --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "accept n option as config name" {
  run solr create -c COLL_NAME -n other_conf_name --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "config-set 'other_conf_name'"
}

@test "allow config reuse when n option specifies same config" {
  run -0 solr create -c COLL_NAME_1 -n shared_config --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME_1'"
  assert_output --partial "config-set 'shared_config'"

  run -0 solr create -c COLL_NAME_2 -n shared_config --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME_2'"
  assert_output --partial "config-set 'shared_config'"
}

@test "create multisharded collections when s provided" {
  run -0 solr create -c COLL_NAME -s 2 --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 shard(s)"
}

@test "create replicated collections when rf provided" {
  run -0 solr create -c COLL_NAME -rf 2 --solr-url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 replica(s)"
}
