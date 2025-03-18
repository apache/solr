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

# These tests demonstrate that you can round trip data out of Solr and back.
# However you need to be careful about which fields you select, as if you include copyFields
# then on the import you run into issues.
# "json" format works with the bin/solr post tool.
# "jsonl" format works with curl and posting directory to /update/json
# There are no options for javabin or cbor.

load bats_helper

setup() {
  common_clean_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  solr stop --all >/dev/null 2>&1
}

@test "roundtrip export and import using .json and post command" {
  run solr start -e techproducts
  run solr export --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url "http://localhost:${SOLR_PORT}" --name techproducts --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/output"

  assert [ -e ${BATS_TEST_TMPDIR}/output.json ]
  
  run solr create -c COLL_NAME -d sample_techproducts_configs
  
  run solr post --format --solr-url "http://localhost:${SOLR_PORT}" -c COLL_NAME ${BATS_TEST_TMPDIR}/output.json

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'
  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*&fl=id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads"
  assert_output --partial '"numFound":32'
  
  # export once more the imported data, and compare that export to the original export from techproducts
  run solr export --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url http://localhost:${SOLR_PORT} --name COLL_NAME --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/COLL_NAME"
  
  run diff <(jq -S . ${BATS_TEST_TMPDIR}/output.json) <(jq -S . ${BATS_TEST_TMPDIR}/COLL_NAME.json)
  assert_success
  assert_output ""
}

@test "roundtrip export and import using .jsonl and curl command" {
  run solr start -e techproducts
  run solr export --format jsonl --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url http://localhost:${SOLR_PORT} --name techproducts --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/output"

  assert [ -e ${BATS_TEST_TMPDIR}/output.jsonl ]
  
  run solr create -c COLL_NAME -d sample_techproducts_configs
  
  run curl "http://localhost:${SOLR_PORT}/api/collections/COLL_NAME/update/json" -H 'Content-type:application/json' -d "@${BATS_TEST_TMPDIR}/output.jsonl"

  assert_output --partial '"status":0'
  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*"
  assert_output --partial '"numFound":32'
  
  run solr export --format jsonl --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url http://localhost:${SOLR_PORT} --name COLL_NAME --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/COLL_NAME"

  run diff <(jq -c . ${BATS_TEST_TMPDIR}/output.jsonl | sort) <(jq -c . ${BATS_TEST_TMPDIR}/COLL_NAME.jsonl | sort)
  assert_success
  assert_output ""
}

@test "roundtrip export and import using .javabin and curl command" {
  run solr start -e techproducts
  run solr export --format javabin --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url http://localhost:${SOLR_PORT} --name techproducts --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/output"
  run solr export --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url "http://localhost:${SOLR_PORT}" --name techproducts --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/output"

  assert [ -e ${BATS_TEST_TMPDIR}/output.javabin ]
  
  run solr create -c COLL_NAME -d sample_techproducts_configs
  
  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/update?commit=true" -H 'Content-type:application/javabin' --data-binary "@${BATS_TEST_TMPDIR}/output.javabin"

  assert_output --partial '"status":0'
  run curl "http://localhost:${SOLR_PORT}/solr/COLL_NAME/select?q=*:*"
  assert_output --partial '"numFound":32'
  
  # We compare the success of the round trip of javabin formatted data using json formatted exports to leverage `diff` command.
  run solr export --fields id,name,manu,manu_id_s,cat,features,price,popularity,inStock,store,manufacturedate_dt,payloads --solr-url http://localhost:${SOLR_PORT} --name COLL_NAME --query "*:* -id:test" --output "${BATS_TEST_TMPDIR}/COLL_NAME"

  run diff <(jq -S . ${BATS_TEST_TMPDIR}/output.json) <(jq -S . ${BATS_TEST_TMPDIR}/COLL_NAME.json)
  assert_success
  assert_output ""
}
