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

@test "user managed index replication" {

  # This test is fragile.  I think we are creating files etc in solr/packaging/build/solr-10.0.0-SNAPSHOT/example/techproducts
  # which means you may need to run gradle clean between runs or we can't start up Solr.
  # I think we can't run both bats test either, one after the other ;-( 
  
  # wish we had zkServerDataDir as a EnvUtil instead of system property!
  
  mkdir -p cloud_5000
  mkdir -p cloud_5100
  mkdir -p cloud_5200
  
  # Get our three seperate independent Solr nodes running.
  bin/solr start -f -c -p 5000 -Dsolr.disable.allowUrls=true -s $(pwd)/cloud_5000 -DzkServerDataDir=$(pwd)/cloud_5000/zoo_data -v -V 
  bin/solr start -f -c -p 5100 -Dsolr.disable.allowUrls=true -s $(pwd)/cloud_5100 -DzkServerDataDir=$(pwd)/cloud_5100/zoo_data -v -V 
  bin/solr start -f -c -p 5200 -Dsolr.disable.allowUrls=true -s $(pwd)/cloud_5200 -DzkServerDataDir=$(pwd)/cloud_5200/zoo_data -v -V 
  
  solr assert --started http://localhost:5000 --timeout 5000
  solr assert --started http://localhost:5100 --timeout 5000
  solr assert --started http://localhost:5200 --timeout 5000
  
  solr assert -cloud http://localhost:5000
  solr assert -cloud http://localhost:5100
  solr assert -cloud http://localhost:5200  
  
  # Wish I loaded configset seperately...
  bin/solr create -c techproducts -d ./server/solr/configsets/sample_techproducts_configs -solrUrl http://localhost:5000
  bin/solr create -c techproducts -d ./server/solr/configsets/sample_techproducts_configs -solrUrl http://localhost:5100
  bin/solr create -c techproducts -d ./server/solr/configsets/sample_techproducts_configs -solrUrl http://localhost:5200
  
  # Verify no data
  run curl 'http://localhost:5000/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":0'
  run curl 'http://localhost:5100/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":0'
  run curl 'http://localhost:5200/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":0'  
  
  # Load XML formatted data into the leader
  bin/solr post -type application/xml -commit -url http://localhost:5000/solr/techproducts/update ./example/exampledocs/*.xml
  run curl 'http://localhost:5000/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":32'
  
  # Confirm no replication
  run curl 'http://localhost:5100/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":0'  
  run curl 'http://localhost:5200/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":0'  
  
  # Setup the Leader for replication
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-requesthandler": {
      "name": "/replication",
      "class": "solr.ReplicationHandler",
      "leader":{ "replicateAfter": "commit", "backupAfter":"commit", "confFiles":""},
      "maxNumberOfBackups":2            
    }
  }' "http://localhost:5000/solr/techproducts/config"
  assert_output --partial '"status":0'
  
  run curl 'http://localhost:5000/solr/techproducts/replication?command=details'
  assert_output --partial '"replicationEnabled":"true"'
  
  
  # Setup the Repeater for replication
  
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-requesthandler": {
      "name": "/replication",
      "class": "solr.ReplicationHandler",
      "follower":{ "leaderUrl": "http://localhost:5000/solr/techproducts/replication", "pollInterval":"00:00:02"},
      "leader":{ "replicateAfter": "commit", "backupAfter":"commit", "confFiles":""},
      "maxNumberOfBackups":2            
    }
  }' "http://localhost:5100/solr/techproducts/config"
  assert_output --partial '"status":0'
  
  run curl 'http://localhost:5100/solr/techproducts/replication?command=details'
  assert_output --partial '"isPollingDisabled":"false"'

  
  # Wish we could block on fetchindex..   Does checking details help?
  sleep 5
  run curl 'http://localhost:5100/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":32' 
  
  # Testing adding new data by adding JSON formatted data into the leader
  bin/solr post -type application/json -commit -url http://localhost:5000/solr/techproducts/update ./example/exampledocs/*.json
  run curl 'http://localhost:5000/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":36'
  
  sleep 5
  
  # Testing adding new data by adding CSV formatted data into the leader
  bin/solr post -commit -url http://localhost:5000/solr/techproducts/update ./example/exampledocs/*.csv
  run curl 'http://localhost:5000/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":46'
  
  sleep 5
  
  run curl 'http://localhost:5100/solr/techproducts/select?q=*:*&rows=0'
  assert_output --partial '"numFound":46'
  
  
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}
