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
  common_setup
  run solr start -c

  local source_configset_dir="$SOLR_HOME/server/solr/configsets/sample_techproducts_configs"
  cp -r $source_configset_dir "$BATS_TMPDIR/config"
}

teardown_file() {
  common_setup
  run solr stop -all
}

setup() {
  common_setup
}

teardown() {
  delete_all_collections
}

@test "create collection" {
  run solr create_collection -c COLL_NAME
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "reject -d option with invalid config dir" {
  run ! solr create_collection -c COLL_NAME -d /asdf
  assert_output --partial "Specified configuration directory /asdf not found!"
}

@test "accept -d option with builtin config" {
  run solr create_collection -c COLL_NAME -d sample_techproducts_configs
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "accept -d option with explicit path to config" {
  run solr create_collection -c COLL_NAME -d "$BATS_TMPDIR/config"
  assert_output --partial "Created collection 'COLL_NAME'"
}

@test "accept -n option as config name" {
  run solr create_collection -c COLL_NAME -n other_conf_name
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "config-set 'other_conf_name'"
}

@test "allow config reuse when -n option specifies same config" {
  run -0 solr create_collection -c COLL_NAME_1 -n shared_config
  assert_output --partial "Created collection 'COLL_NAME_1'"
  assert_output --partial "config-set 'shared_config'"

  run -0 solr create_collection -c COLL_NAME_2 -n shared_config
  assert_output --partial "Created collection 'COLL_NAME_2'"
  assert_output --partial "config-set 'shared_config'"
}

@test "create multisharded collections when -s provided" {
  run -0 solr create_collection -c COLL_NAME -s 2
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 shard(s)"
}

@test "create replicated collections when -rf provided" {
  run -0 solr create_collection -c COLL_NAME -rf 2
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 replica(s)"
}