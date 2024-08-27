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

  solr start -c -e films

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

@test "snapshot lifecycle" {  
  # the way snapshots live, if you run create twice you get an error because snapshot already exists
  run solr snapshot-create -c films --snapshot-name snapshot1 --solr-url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully created snapshot with name snapshot1 for collection films"  
  
  run solr snapshot-delete -c films --snapshot-name snapshot1 -url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully deleted snapshot with name snapshot1 for collection films"
  
   # make sure you can create it again!
  run solr snapshot-create -c films --snapshot-name snapshot1 -z localhost:${ZK_PORT}
  assert_output --partial "Successfully created snapshot with name snapshot1 for collection films"
  
  run solr snapshot-delete -c films --snapshot-name snapshot1 
  assert_output --partial "Successfully deleted snapshot with name snapshot1 for collection films"
}

@test "snapshot list" {  
  solr snapshot-create -c films --snapshot-name snapshot3 --solr-url http://localhost:${SOLR_PORT}
  
  run solr snapshot-list -c films -url http://localhost:${SOLR_PORT}/solr
  assert_output --partial "snapshot3"
  
  run solr snapshot-delete -c films --snapshot-name snapshot3 -url http://localhost:${SOLR_PORT}
  assert_output --partial "Successfully deleted snapshot with name snapshot3 for collection films"
}

@test "snapshot describe" {  
  solr snapshot-create -c films --snapshot-name snapshot4 -url http://localhost:${SOLR_PORT}
  
  run solr snapshot-describe -c films --snapshot-name snapshot4
  assert_output --partial "Name: snapshot4"
  
  run solr snapshot-delete -c films --snapshot-name snapshot4
  assert_output --partial "Successfully deleted snapshot with name snapshot4 for collection films"
}
