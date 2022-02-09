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

setup_file() {
  solr start -c > /dev/null 2>&1
}

teardown_file() {
  solr stop -all > /dev/null 2>&1
}

setup() {
  common_setup
}

teardown() {
  delete_all_collections
}

@test "can delete collections" {
  solr create_collection -c COLL_NAME
  # assert_collection_exists "COLL_NAME" || return 1

  solr delete -c "COLL_NAME"
  # assert_collection_doesnt_exist "COLL_NAME" || return 1
}

@test "collection delete also deletes zk config" {
  solr create_collection -c "COLL_NAME"
  # assert_config_exists "COLL_NAME" || return 1

  solr delete -c "COLL_NAME"
  # assert_config_doesnt_exist "COLL_NAME" || return 1
}

@test "deletes accompanying zk config with nondefault name" {
  solr create_collection -c "COLL_NAME" -n "NONDEFAULT_CONFIG_NAME"
  # assert_config_exists "NONDEFAULT_CONFIG_NAME" || return 1

  solr delete -c "COLL_NAME"
  # assert_config_doesnt_exist "NONDEFAULT_CONFIG_NAME"
}

@test "deleteConfig option can opt to leave config in zk" {
  solr create_collection -c "COLL_NAME"
  # assert_config_exists "COLL_NAME"

  solr delete -c "COLL_NAME" -deleteConfig false
  # assert_config_exists "COLL_NAME"
}
