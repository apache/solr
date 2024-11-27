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

setup_file() {
  common_clean_setup
  solr start -e techproducts
  solr auth enable --type basicAuth --credentials name:password --solr-include-file /force/credentials/to/be/supplied
}

teardown_file() {
  common_setup
  solr stop --all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure
}

@test "searching solr via locally executed streaming expression" {
  
  local solr_stream_file="${BATS_TEST_TMPDIR}/search.expr"
  echo 'search(techproducts,' > "${solr_stream_file}"
  echo 'q="name:memory",' >> "${solr_stream_file}"
  echo 'fl="name,price",' >> "${solr_stream_file}"
  echo 'sort="price desc"' >> "${solr_stream_file}"
  echo ')' >> "${solr_stream_file}"  
  
  run solr stream --execution local --header --credentials name:password ${solr_stream_file}

  assert_output --partial 'name   price'
  assert_output --partial 'CORSAIR  XMS'
  refute_output --partial 'ERROR'
}

@test "searching solr via remotely executed streaming expression" {
  
  local solr_stream_file="${BATS_TEST_TMPDIR}/search.expr"
  echo 'search(techproducts,' > "${solr_stream_file}"
  echo 'q="name:memory",' >> "${solr_stream_file}"
  echo 'fl="name,price",' >> "${solr_stream_file}"
  echo 'sort="price desc"' >> "${solr_stream_file}"
  echo ')' >> "${solr_stream_file}"
  
  run solr stream --name techproducts --solr-url http://localhost:${SOLR_PORT} --header --credentials name:password ${solr_stream_file}

  assert_output --partial 'name   price'
  assert_output --partial 'CORSAIR  XMS'
  refute_output --partial 'ERROR'
}

@test "variable interpolation" {
  
  local solr_stream_file="${BATS_TEST_TMPDIR}/search.expr"
  echo 'search(techproducts,' > "${solr_stream_file}"
  echo 'q="name:$1",' >> "${solr_stream_file}"
  echo 'fl="name,price",' >> "${solr_stream_file}"
  echo 'sort="price $2"' >> "${solr_stream_file}"
  echo ')' >> "${solr_stream_file}"
  
  run solr stream --execution local --header --credentials name:password ${solr_stream_file} apple asc

  assert_output --partial 'name   price'
  assert_output --partial 'Apple 60 GB iPod'
  refute_output --partial 'ERROR'
}

@test "searching solr without credentials fails with error" {

  local solr_stream_file="${BATS_TEST_TMPDIR}/search.expr"
  echo 'search(techproducts,' > "${solr_stream_file}"
  echo 'q="name:memory",' >> "${solr_stream_file}"
  echo 'fl="name,price",' >> "${solr_stream_file}"
  echo 'sort="price desc"' >> "${solr_stream_file}"
  echo ')' >> "${solr_stream_file}"

  run ! solr stream --execution local --header ${solr_stream_file}

  assert_output --partial 'ERROR'
}
