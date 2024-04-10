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

@test "running subcommands with zk is prevented" {

  # this demonstrates the use of three Solr nodes with the embedded
  # ZooKeeper processes all working together.
  
  export SOLR_SECURITY_MANAGER_ENABLED=false
  
  export LH="localhost"
  
  export SOLR2_PORT=$((SOLR_PORT + 1))
  export SOLR3_PORT=$((SOLR_PORT + 2))
  export ZK2_PORT=$((ZK_PORT + 1))
  export ZK3_PORT=$((ZK_PORT + 2))
  
  solr start -p $SOLR_PORT -z $LH:$ZK_PORT,$LH:$ZK2_PORT,$LH:$ZK3_PORT -DzkQuorumRun
  solr start -p $SOLR2_PORT -z $LH:$ZK_PORT,$LH:$ZK2_PORT,$LH:$ZK3_PORT -DzkQuorumRun
  solr start -p $SOLR3_PORT -z $LH:$ZK_PORT,$LH:$ZK2_PORT,$LH:$ZK3_PORT -DzkQuorumRun
  
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 15000
  solr assert --started http://localhost:${SOLR2_PORT}/solr --timeout 15000
  solr assert --started http://localhost:${SOLR3_PORT}/solr --timeout 15000
  
  run curl "http://localhost:${SOLR3_PORT}/solr/admin/zookeeper/status?wt=json"  
  assert_output --partial '"mode":"ensemble"'
  assert_output --partial '"status":"green"'
  
  solr stop -p $SOLR2_PORT
  solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000
  
  run curl "http://localhost:${SOLR3_PORT}/solr/admin/zookeeper/status?wt=json"
  assert_output --partial '"status":"yellow"'
  
  solr stop -p $SOLR_PORT
  solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000
  
  run curl "http://localhost:${SOLR3_PORT}/solr/admin/zookeeper/status?wt=json"
  assert_output --partial '"status":"red"'
  
  solr start -p $SOLR_PORT -z $LH:$ZK_PORT,$LH:$ZK2_PORT,$LH:$ZK3_PORT -DzkQuorumRun
  solr start -p $SOLR2_PORT -z $LH:$ZK_PORT,$LH:$ZK2_PORT,$LH:$ZK3_PORT -DzkQuorumRun
  
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 15000
  solr assert --started http://localhost:${SOLR2_PORT}/solr --timeout 15000

  run curl "http://localhost:${SOLR3_PORT}/solr/admin/zookeeper/status?wt=json"  
  assert_output --partial '"status":"green"'
}
