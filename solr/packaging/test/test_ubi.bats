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
  SOLR_STOP_WAIT=1 solr stop -all >/dev/null 2>&1
}

@test "Run set up process" {

  solr start -c -e films
  
  run solr healthcheck -c films
  refute_output --partial 'error'
  
  #echo "Here is the logs dir"
  #echo $SOLR_LOGS_DIR
  #run ls ${SOLR_LOGS_DIR}
  #assert_output --partial "Initializing authentication plugin: solr.KerberosPlugin"

  #assert [ -e ${SOLR_LOGS_DIR}/solr_ubi_queries.log ]

  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-searchcomponent": {
      "name": "ubi",
      "class": "solr.UBIComponent",
      "defaults":{ }
    }
  }' "http://localhost:${SOLR_PORT}/api/collections/films/config"

  assert_output --partial '"status":0'
  
  curl -X POST -H 'Content-type:application/json' -d '{
    "update-requesthandler": {
      "name": "/select",
      "class": "solr.SearchHandler",
      "last-components":["ubi"]
    }
  }' "http://localhost:${SOLR_PORT}/api/collections/films/config"
  
  assert_output --partial '"status":0'
  
  curl -X POST -H 'Content-type:application/json' -d '{
    "update-requesthandler": {
      "name": "/query",
      "class": "solr.SearchHandler",
      "last-components":["ubi"]
    }
  }' "http://localhost:${SOLR_PORT}/api/collections/films/config"  
  
  assert_output --partial '"status":0'

  run curl "http://localhost:${SOLR_PORT}/solr/films/select?q=*:*&rows=3&ubi=true"
  assert_output --partial '"status":0'
  assert_output --partial '"query_id":"1234'
  
  #run cat "${SOLR_LOGS_DIR}/solr.log"
  #assert_output --partial "Initializing authentication plugin: solr.KerberosPlugin"
  #assert_file_contains "${SOLR_LOGS_DIR}/solr_ubi_queries.log" 'eric'
}
