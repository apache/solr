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

@test "user managed index replication with a twist" {  
  # This demonstrates traditional user managed cluster working as defined in
  # https://solr.apache.org/guide/solr/latest/deployment-guide/cluster-types.html 
  #
  # We demonstrate starting up three independent Solr nodes in the Leader/Repeater/Follower pattern.
  # Then we create three seperate 'techproducts' collections, uploading the same configset three seperate times
  # to demonstrate that there is no interconnection or shard config between them.
  # We then index some XML data on the Leader, and then check that it flows through the Repeater to the Follower.
  # This is repeated for some more documents.
  # Lastly, we shutdown the Repeater and demonstrate that the Follower still has all of it's documents available for querying.
  # We delete the data on the Leader, and then subsequantly bring back up the Repeater.
  # The Repeater perseves all fo the configuration that was done during the setup process after restarting, and immediatley copies
  # over the now empty 'techproducts' index and we then see the Follower picks up that empty collection as well.
  
  
  export SOLR_SECURITY_MANAGER_ENABLED=false
  
  # should cloud_5000 be "leader" and cloud_5100 be "repeater" etc?
  
  export clusters_dir="${BATS_TEST_TMPDIR}/clusters"
  
  mkdir -p ${clusters_dir}/cluster_5000
  mkdir -p ${clusters_dir}/cluster_5100
  mkdir -p ${clusters_dir}/cluster_5200
  
  # Get our three seperate independent Solr nodes running.
  solr start -c -p ${SOLR_PORT} -Dsolr.disable.allowUrls=true -s "${clusters_dir}"/cluster_5000 -DzkServerDataDir="${clusters_dir}"/cluster_5000/zoo_data -v -V 
  solr start -c -p ${SOLR2_PORT} -Dsolr.disable.allowUrls=true -s "${clusters_dir}"/cluster_5100 -DzkServerDataDir="${clusters_dir}"/cluster_5100/zoo_data -v -V 
  solr start -c -p ${SOLR3_PORT} -Dsolr.disable.allowUrls=true -s "${clusters_dir}"/cluster_5200 -DzkServerDataDir="${clusters_dir}"/cluster_5200/zoo_data -v -V 
  
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000
  solr assert --started http://localhost:${SOLR3_PORT} --timeout 5000
  
  solr assert -cloud http://localhost:${SOLR_PORT}
  solr assert -cloud http://localhost:${SOLR2_PORT}
  solr assert -cloud http://localhost:${SOLR3_PORT}  
  
  # Wish I loaded configset seperately...
  local source_configset_dir="${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs"
  solr create -c techproducts -d "${source_configset_dir}" -solrUrl http://localhost:${SOLR_PORT}
  solr create -c techproducts -d "${source_configset_dir}" -solrUrl http://localhost:${SOLR2_PORT}
  solr create -c techproducts -d "${source_configset_dir}" -solrUrl http://localhost:${SOLR3_PORT}
  
  # Verify empty state of all the nodes
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'  
  
  # Load XML formatted data into the leader
  solr post -type application/xml -commit -url http://localhost:${SOLR_PORT}/solr/techproducts/update "${SOLR_TIP}"/example/exampledocs/*.xml
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":32'
  
  # Confirm no replication
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'  
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'  
  
  # Setup the Leader for replication
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-requesthandler": {
      "name": "/replication",
      "class": "solr.ReplicationHandler",
      "leader":{ "replicateAfter": "commit", "backupAfter":"commit", "confFiles":""},
      "maxNumberOfBackups":2            
    }
  }' "http://localhost:${SOLR_PORT}/solr/techproducts/config"
  assert_output --partial '"status":0'
  
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/replication?command=details"
  assert_output --partial '"replicationEnabled":"true"'
  
  # Setup the Repeater for replication  
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-requesthandler": {
      "name": "/replication",
      "class": "solr.ReplicationHandler",
      "follower":{ "leaderUrl": "http://localhost:'"${SOLR_PORT}"'/solr/techproducts/replication", "pollInterval":"00:00:02"},
      "leader":{ "replicateAfter": "commit", "backupAfter":"commit", "confFiles":""},
      "maxNumberOfBackups":2            
    }
  }' "http://localhost:${SOLR2_PORT}/solr/techproducts/config"
  assert_output --partial '"status":0'
  
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/replication?command=details"
  assert_output --partial '"isPollingDisabled":"false"'
  
  # Setup the Follower for replication  
  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-requesthandler": {
      "name": "/replication",
      "class": "solr.ReplicationHandler",
      "follower":{ "leaderUrl": "http://localhost:'"${SOLR2_PORT}"'/solr/techproducts/replication", "pollInterval":"00:00:02"}         
    }
  }' "http://localhost:${SOLR3_PORT}/solr/techproducts/config"
  assert_output --partial '"status":0'
  
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/replication?command=details"
  assert_output --partial '"isPollingDisabled":"false"'  
 
  # How can we know when a replication has happened and then check?
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/update?optimize=true"
  assert_output --partial '"status":0'
  sleep 10
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":32' 
  
  # Testing adding new data by adding JSON formatted data into the leader
  solr post -type application/json -commit -url http://localhost:${SOLR_PORT}/solr/techproducts/update "${SOLR_TIP}"/example/exampledocs/*.json
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":36'
  sleep 5
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":36' 
  
  # Testing adding new data by adding CSV formatted data into the leader
  solr post -commit -url http://localhost:${SOLR_PORT}/solr/techproducts/update "${SOLR_TIP}"/example/exampledocs/*.csv
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":46'  
  sleep 5  
  echo "Waiting to see Solr2 on ${SOLR2_PORT} update"
  sleep 20
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":46'  
  
  # Now lets go check our Follower and make sure it's picks up all the changes too!
  sleep 5  
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":46' 
  
  # Now lets stop our replicator
  solr stop -p ${SOLR2_PORT}
  
  solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000
  
  # Delete data on the leader.
  solr post -url http://localhost:${SOLR_PORT}/solr/techproducts/update -mode args -out -commit "{'delete': {'query': '*:*'}}"
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0' 

  # check our follower is still up and responding
  sleep 5  
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":46' 
  
  # Bring back our Repeater
  solr start -c -p ${SOLR2_PORT} -Dsolr.disable.allowUrls=true -s "${clusters_dir}"/cluster_5100 -DzkServerDataDir="${clusters_dir}"/cluster_5100/zoo_data -v -V 
  solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000
  
  # check our Repeater is picking up the deleted documents from the Leader
  sleep 5  
  run curl "http://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0' 
  
  # And now check our follower has no documents as well.
  sleep 5  
  run curl "http://localhost:${SOLR3_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0' 
  
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}
