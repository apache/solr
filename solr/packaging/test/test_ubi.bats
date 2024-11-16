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
  SOLR_STOP_WAIT=1 solr stop --all >/dev/null 2>&1
}

@test "Track query using UBI with log file." {
  solr start -e techproducts
  
  run solr healthcheck -c techproducts
  refute_output --partial 'error'

  run curl -X POST -H 'Content-type:application/json' -d '{
    "add-searchcomponent": {
      "name": "ubi",
      "class": "solr.UBIComponent",
      "defaults":{ }
    }
  }' "http://localhost:${SOLR_PORT}/api/collections/techproducts/config"

  assert_output --partial '"status":0'
  
  run curl -X POST -H 'Content-type:application/json' -d '{
    "update-requesthandler": {
      "name": "/select",
      "class": "solr.SearchHandler",
      "last-components":["ubi"]
    }
  }' "http://localhost:${SOLR_PORT}/api/collections/techproducts/config"
  
  assert_output --partial '"status":0'

  # Simple UBI enabled query
  run curl "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=3&ubi=true&user_query=give%20me%20all&query_id=5678"
  assert_output --partial '"status":0'
  assert_output --partial '"query_id":"5678'
   
  # Check UBI query record was written out to default location
  assert_file_exist ${SOLR_TIP}/example/techproducts/solr/userfiles/ubi_queries.jsonl
  assert_file_contains ${SOLR_TIP}/example/techproducts/solr/userfiles/ubi_queries.jsonl '"query_id":"5678"'
  
  # Rich UBI user query tracking enabled query with JSON Query
  run curl -X POST -H 'Content-type:application/json' -d '{
    "query" : "ram OR memory",
    "filter": [
        "inStock:true"
    ],
    "limit":2,
    "params": {
      "ubi": "true",
      "application":"primary_search",
      "query_id": "xyz890",
      "user_query":"RAM memory",
      "query_attributes": {
        "experiment": "supersecret",
        "page": 1,
        "filter": "productStatus:available"
      }            
    }
  }' "http://localhost:${SOLR_PORT}/solr/techproducts/select" 
  assert_output --partial '"query_id":"xyz890"'
  
  # Check UBI query record was written out to default location with additional metadata
  assert_file_contains ${SOLR_TIP}/example/techproducts/solr/userfiles/ubi_queries.jsonl '"query_id":"xyz890"'
  assert_file_contains ${SOLR_TIP}/example/techproducts/solr/userfiles/ubi_queries.jsonl '"experiment":"supersecret"'
  
  
}
