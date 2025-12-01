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


@test "create replicated collections when core remnants exist" {
  run -0 solr create -c COLL_NAME -rf 2 --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 replica(s)"

  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n1 ]
  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n2 ]


  solr delete -c "COLL_NAME"

  # Simulate a core remnant still exists.
  mkdir -p ${SOLR_HOME}/COLL_NAME_shard1_replica_n2
  touch ${SOLR_HOME}/COLL_NAME_shard1_replica_n2/core.properties

  # Create a new collection that currently fails.
  # This passes with the CoreContainer deleting the remnant instance dir if it exists.
  run curl  "http://localhost:${SOLR_PORT}/solr/admin/collections?action=CREATE&collection.configName=_default&name=COLL_NAME&numShards=1&replicationFactor=3&wt=json"
  assert_output --partial '"status":0'

}
