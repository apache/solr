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
  
  echo "Starting Solr"
  solr start -c -Denable.packages=true
  
  # The auth command exports some system variables that are injected as basic auth username and password, 
  # however that defeats our test so fake that out via -solrIncludeFile param specifing a bogus path.
  solr auth enable -type basicAuth -credentials name:password -solrIncludeFile /force/credentials/to/be/supplied
  
  solr assert -credentials name:password --cloud http://localhost:${SOLR_PORT} --timeout 5000
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  run solr auth disable -z localhost:${ZK_PORT}
  solr stop -all >/dev/null 2>&1
}

# Remaining commands that should support basic auth:
# package, export


@test "create, config, api, and delete with basic auth" {

  # Test create
  run solr create -u name:password -c COLL_NAME 
  assert_output --partial "Created collection 'COLL_NAME'"
  
  # Test config
  run solr config -u name:password -c COLL_NAME -action set-property -property updateHandler.autoCommit.maxDocs -value 100 -solrUrl http://localhost:${SOLR_PORT}/solr
  assert_output --partial "Successfully set-property updateHandler.autoCommit.maxDocs to 100"
  
  # Test api
  run solr api -u name:password --get "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*" -verbose
  assert_output --partial '"numFound":0'
  
  # Test delete
  run solr delete --credentials name:password -c COLL_NAME -zkHost localhost:${ZK_PORT} -verbose
  assert_output --partial "Deleted collection 'COLL_NAME'"
  refute collection_exists "COLL_NAME"
  
}

@test "post, postlogs and export with basic auth" {
run solr create -c COLL_NAME
  run solr create -u name:password -c COLL_NAME 
  assert_output --partial "Created collection 'COLL_NAME'"

  # Test post
  run solr post -u name:password -type application/xml -url http://localhost:${SOLR_PORT}/solr/monitors/update ${SOLR_TIP}/example/exampledocs/monitor.xml
  assert_output --partial '1 files indexed.'

  # Test postlogs
  run solr postlogs -u name:password -url http://localhost:${SOLR_PORT}/solr/COLL_NAME -rootdir ${SOLR_LOGS_DIR}/solr.log
  assert_output --partial 'Committed'
  
  # Test export
  #run solr export -u name:password -url "http://localhost:${SOLR_PORT}/solr/COLL_NAME" -query "*:*" -out "${BATS_TEST_TMPDIR}/output"
  #assert_output --partial 'Export complete'
  
}

@test "package with basic auth" {
  
  run solr package deploy PACKAGE_NAME -credentials name:password -collections foo-1.2
  # verify that package tool is communicating with Solr via basic auth
  assert_output --partial "Collection(s) doesn't exist: [foo-1.2]"
  #assert_output --partial "Deployment successful"
  #refute_output --partial "Invalid collection"
}
