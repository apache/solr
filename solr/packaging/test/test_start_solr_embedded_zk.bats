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

  solr stop --all >/dev/null 2>&1
}

@test "Embedded ZK in Quorum Mode" {

  export SOLR_SECURITY_MANAGER_ENABLED=false
  export SOLR_SECURITY_MANAGER_ENABLED=false
  
  export SOLR_PORT=8983
  export SOLR2_PORT=8984
  export SOLR3_PORT=8985
  
  export ZK_PORT=9983
  export ZK2_PORT=9984
  export ZK3_PORT=9985

  export nodes_dir="${BATS_TEST_TMPDIR}/nodes"
  mkdir -p ${nodes_dir}/solr1
  mkdir -p ${nodes_dir}/solr2
  mkdir -p ${nodes_dir}/solr3
  
  echo "HERE WE GO"
  echo $ZK2_PORT
echo $ZK3_PORT  
  run solr start -p ${SOLR_PORT} --solr-home ${nodes_dir}/solr1 -z localhost:${ZK_PORT} -Dsolr.node.roles=data:on,overseer:allowed,zookeeper_quorum:on
  #run solr start -p ${SOLR_PORT} --solr-home ${nodes_dir}/solr1 -z localhost:${ZK_PORT},localhost:${ZK2_PORT},localhost:${ZK3_PORT} -Dsolr.node.roles=data:on,overseer:allowed,zookeeper_quorum:on
  #run solr start -p ${SOLR2_PORT} --solr-home ${nodes_dir}/solr2 -z localhost:${ZK_PORT},localhost:${ZK2_PORT},localhost:${ZK3_PORT} -Dsolr.node.roles=data:on,overseer:allowed,zookeeper_quorum:on
  #run solr start -p ${SOLR3_PORT} --solr-home ${nodes_dir}/solr3 -z localhost:${ZK_PORT},localhost:${ZK2_PORT},localhost:${ZK3_PORT} -Dsolr.node.roles=data:on,overseer:allowed,zookeeper_quorum:on
  
  solr assert --started http://localhost:${SOLR_PORT} --timeout 20000
  #solr assert --started http://localhost:${SOLR2_PORT} --timeout 20000
  #solr assert --started http://localhost:${SOLR3_PORT} --timeout 20000
  
  solr assert --cloud http://localhost:${SOLR_PORT}
  solr assert --cloud http://localhost:${SOLR2_PORT}
  solr assert --cloud http://localhost:${SOLR3_PORT}  
  
  local source_configset_dir="${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs"
  #solr create -c techproducts --conf-dir "${source_configset_dir}" --solr-url http://localhost:${SOLR_PORT}
  run curl -X POST http://localhost:${SOLR_PORT}/api/collections -H 'Content-Type: application/json' -d '
    {
      "name": "techproducts",
      "config": "techproducts",
      "numShards": 1,
      "numReplicas": 2,
      "nodeSet": ["localhost:${SOLR_PORT}_solr", "localhost:${SOLR_PORT2}_solr"]
    }
  '
  assert_output --partial '"numFound":4'
  
  solr post --type application/json --solr-url http://localhost:${SOLR_PORT} -c techproducts "${SOLR_TIP}"/example/exampledocs/*.json
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":4'
  
  
  solr stop -p ${REPEATER_PORT}
  solr assert --not-started http://localhost:${REPEATER_PORT} --timeout 5000
  
}
