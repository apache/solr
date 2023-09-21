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

  delete_all_collections
  solr stop -all >/dev/null 2>&1
}

@test "Affinity placement plugin using sysprop" {
  run solr start -c -Dsolr.placementplugin.default=affinity
  solr assert -c http://localhost:${SOLR_PORT}/solr -t 3000
  run solr create_collection -c COLL_NAME
  collection_exists COLL_NAME
  assert_file_contains "${SOLR_LOGS_DIR}/solr.log" 'Default replica placement plugin set in solr\.placementplugin\.default to affinity'
}

@test "Random placement plugin using ENV" {
  export SOLR_PLACEMENTPLUGIN_DEFAULT=random
  run solr start -c
  solr assert -c http://localhost:${SOLR_PORT}/solr -t 3000
  run solr create_collection -c COLL_NAME
  collection_exists COLL_NAME
  assert_file_contains "${SOLR_LOGS_DIR}/solr.log" 'Default replica placement plugin set in solr\.placementplugin\.default to random'
}
