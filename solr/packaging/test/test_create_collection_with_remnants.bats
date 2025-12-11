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

# Tests what happens when remnant core directories and and files still are on disk.
# Flip solr.delete.unknown.cores=false to see the out of box behavior and the failures.

load bats_helper

setup_file() {
  common_clean_setup
  solr start -Dsolr.delete.unknown.cores=true
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

  # Create a new collection, a process that normally fails due to the remnants
  run curl  "http://localhost:${SOLR_PORT}/solr/admin/collections?action=CREATE&collection.configName=_default&name=COLL_NAME&numShards=1&replicationFactor=3&wt=json"
  assert_output --partial '"status":0'

  # confirm new data directory created when creating collection
  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n2/data ]

}

@test "add replica when core remnants exist" {
  run -0 solr create -c COLL_NAME -rf 2 --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Created collection 'COLL_NAME'"
  assert_output --partial "2 replica(s)"

  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n1 ]
  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n2 ]

  # Simulate a core remnant still exists.
  mkdir -p ${SOLR_HOME}/COLL_NAME_shard1_replica_n5
  touch ${SOLR_HOME}/COLL_NAME_shard1_replica_n5/core.properties

  # Add a new replica, a process that normally fails due to the remnants
  run curl -X POST http://localhost:${SOLR_PORT}/api/collections/COLL_NAME/shards/shard1/replicas -H 'Content-Type: application/json' -d "
      {
        \"node\":\"localhost:${SOLR_PORT}_solr\"
      }
    "
  assert_output --partial '"status":0'

  # confirm new data directory created by adding replica
  assert [ -e ${SOLR_HOME}/COLL_NAME_shard1_replica_n5/data ]

}
