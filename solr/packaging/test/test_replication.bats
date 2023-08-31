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

  solr stop -all >/dev/null 2>&1
}

@test "user managed index replication without zookeeper" {
  skip

  # This test is fragile.  I think we are creating files etc in solr/packaging/build/solr-10.0.0-SNAPSHOT/example/techproducts
  # which means you may need to run gradle clean between runs or we can't start up Solr.
  # I think we can't run both bats test either, one after the other ;-( 
  
  mkdir -p test_data_dir_8983
  mkdir -p test_data_dir_7574
  
  solr start -e techproducts -p 8983 -t test_data_dir_8983 -V 
  solr start -e techproducts -p 7574 -t test_data_dir_7574 -V -Dsolr.disable.allowUrls=true
  solr assert --started http://localhost:8983 --timeout 5000
  solr assert --started http://localhost:7574 --timeout 5000
  
  run curl 'http://localhost:8983/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32'
  
  # Not totally sure why this didn't load it's data, but it works for our needs!
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":0'  
  
  run curl 'http://localhost:8983/solr/techproducts/replication?command=enablereplication'
  assert_output --partial '"OK"'
  run curl 'http://localhost:7574/solr/techproducts/replication?command=fetchindex&leaderUrl=http://localhost:8983/solr/techproducts'
  assert_output --partial '"OK"'
  
  # Wish we could block on fetchindex..   Does checking details help?
  sleep 5
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32' 
  
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}

@test "user managed index replication WITH Zookeeper" {
  # This test is fragile.  I think we are creating files etc in solr/packaging/build/solr-10.0.0-SNAPSHOT/example/techproducts
  # which means you may need to run gradle clean between runs or we can't start up Solr.
  # I think we can't run both bats test either, one after the other ;-( 
  
  mkdir -p test_data_dir_8983
  
  solr start -c -e techproducts -p 8983 -t test_data_dir_8983 -V
  solr start -c -p 7574 -V -Dsolr.disable.allowUrls=true
  solr assert --started http://localhost:8983 --timeout 5000
  solr assert --started http://localhost:7574 --timeout 5000
  
  run curl 'http://localhost:8983/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32'
  run curl 'http://localhost:8983/solr/techproducts_shard1_replica_n1/select?q=*:*'
  assert_output --partial '"numFound":32'
  
  # setup the follower
  local source_configset_dir="${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs"
  test -d $source_configset_dir

  run solr zk upconfig -d ${source_configset_dir} -n techproducts -z localhost:8574
  assert_output --partial "Uploading"
  run solr create -c techproducts -d sample_techproducts_configs -solrUrl http://localhost:7574
  assert_output --partial "Created collection 'techproducts'"
    
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":0'  
  
  run curl -X POST -H 'Content-type:application/json' -d '{"add-requesthandler": {"name": "/replication","class": "solr.ReplicationHandler","follower": { "leaderUrl": "http://localhost:8983/solr/techproducts_shard1_replica_n1" ,"pollInterval": "00:00:20" }}}' http://localhost:7574/api/collections/techproducts/config
  assert_output --partial '"status":0' 
  
  sleep 30
  
  
  run curl 'http://localhost:8983/solr/techproducts_shard1_replica_n1/replication?command=details'
  assert_output --partial '"replicationEnabled":"true"'
  #run curl 'http://localhost:7574/solr/techproducts_shard1_replica_n1/replication?command=fetchindex&leaderUrl=http://localhost:8983/solr/techproducts_shard1_replica_n1'
  #assert_output --partial '"OK"'
  
  # Wish we could block on fetchindex..   Does checking details help?
  sleep 5
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32' 
  
  # Create a doc on the leader and check the follower
  run solr post -url http://localhost:8983/solr/techproducts/update -mode args -out -commit "{'add': {doc:{'id': 'newproduct'}}}"
  assert_output --partial '"status":0'
  
  sleep 25
  
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":33' 
  
  
  
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}
