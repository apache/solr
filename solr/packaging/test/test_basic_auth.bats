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
  solr start -c
  
  # we don't want the convencie system variables for user auth to be set as that defeats the test, 
  # so fake it out via -solrIncludeFile param being bogus path.
  solr auth enable -type basicAuth -credentials name:password -solrIncludeFile /force/credentials/to/be/supplied
  
  solr assert -credentials name:password --cloud http://localhost:${SOLR_PORT} --timeout 5000
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  run solr auth disable -v
  solr stop -all >/dev/null 2>&1
}

# Remaining commands that should support basic auth:
# zk,  config, export, api, package, post


@test "create, api, and delete with basic auth" {

  
}

@test "postlogs and export with basic auth" {
  run solr create -u name:password -c COLL_NAME 
  assert_output --partial "Created collection 'COLL_NAME'"
  
  # Test postlogs
  run solr postlogs -u name:password -url http://localhost:${SOLR_PORT}/solr/COLL_NAME -rootdir ${SOLR_LOGS_DIR}/solr.log
  assert_output --partial 'Sending last batch'
  
  # Test export
  #run solr export -u name:password -url "http://localhost:${SOLR_PORT}/solr/COLL_NAME" -query "*:*" -out "${BATS_TEST_TMPDIR}/output"
  #assert_output --partial 'Export complete'
}
