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
  solr start -c
}

teardown_file() {
  common_setup
  solr stop -all
}

setup() {
  common_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure
}

@test "Check help command" {

  run solr post
  assert_output --partial 'Failed to parse command-line arguments due to: Missing required option: url'
  
  run solr post -h
  assert_output --partial 'usage: post'
  refute_output --partial 'ERROR'
  
  run solr post -help
  assert_output --partial 'usage: post'
  refute_output --partial 'ERROR'
  
  run solr post --help
  assert_output --partial 'usage: post'
  refute_output --partial 'ERROR'  
  
}


@test "basic post with a type specified" {
  
  run solr create_collection -c monitors -d _default
  assert_output --partial "Created collection 'monitors'"
  
  run solr post -type application/xml -url http://localhost:8983/solr/monitors/update ${SOLR_TIP}/example/exampledocs/monitor.xml

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'
}

@test "basic post WITHOUT a type specified" {
  
  solr create_collection -c monitors_no_type -d _default
  
  run solr post -url http://localhost:8983/solr/monitors_no_type/update -commit ${SOLR_TIP}/example/exampledocs/monitor.xml

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'
  run curl 'http://localhost:8983/solr/monitors_no_type/select?q=*:*'
  assert_output --partial '"numFound":1'
  
  solr create_collection -c books_no_type -d _default
  
  run solr post -url http://localhost:8983/solr/books_no_type/update -commit ${SOLR_TIP}/example/exampledocs/books.json

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'
  run curl 'http://localhost:8983/solr/books_no_type/select?q=*:*'
  assert_output --partial '"numFound":4'
  
  solr create_collection -c books_csv_no_type -d _default
  
  run solr post -url http://localhost:8983/solr/books_csv_no_type/update -commit ${SOLR_TIP}/example/exampledocs/books.csv

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'
  run curl 'http://localhost:8983/solr/books_csv_no_type/select?q=*:*'
  assert_output --partial '"numFound":10'  
}

@test "crawling a directory" {
  
  solr create_collection -c mixed_content -d _default
  
  # We filter to xml,json,and csv as we don't want to invoke the Extract handler.
  run solr post -filetypes xml,json,csv -url http://localhost:8983/solr/mixed_content/update -commit ${SOLR_TIP}/example/exampledocs

  assert_output --partial '16 files indexed.'
  refute_output --partial 'ERROR'
  run curl 'http://localhost:8983/solr/mixed_content/select?q=*:*'
  assert_output --partial '"numFound":46'
}

@test "commit and optimize" {
  
  run solr create_collection -c monitors2 -d _default
  assert_output --partial "Created collection 'monitors2'"
  
  run solr post -url http://localhost:8983/solr/monitors2/update -type application/xml -commit -optimize ${SOLR_TIP}/example/exampledocs/monitor.xml

  assert_output --partial '1 files indexed.'
  assert_output --partial 'COMMITting Solr index'
  assert_output --partial 'Performing an OPTIMIZE'
  refute_output --partial 'ERROR'
}
