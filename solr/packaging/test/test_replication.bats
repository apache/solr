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

@test "user managed index replication" {

  # This test is fragile.  I think we are creating files etc in solr/packaging/build/solr-10.0.0-SNAPSHOT/example/techproducts
  # which means you may need to run gradle clean between runs or we can't start up Solr.
  # I think we can't run both bats test either, one after the other ;-( 
  
  mkdir -p test_data_dir_8983
  mkdir -p test_data_dir_7574
  
  solr start -e techproducts -p 8983 -t test_data_dir_8983 -V 
  solr start -e techproducts -p 7574 -t test_data_dir_7574 -V -Dsolr.disable.allowUrls=true
  solr assert --started http://localhost:8983 --timeout 5000
  solr assert --started http://localhost:7574 --timeout 5000
  
  run curl 'http://localhost:8983/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32'
  
  # Not totally sure why this didn't load it's data, but it works for our needs!
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":0'  
  
  run curl 'http://localhost:8983/solr/techproducts/replication?command=enablereplication'
  assert_output --partial '"OK"'
  run curl 'http://localhost:7574/solr/techproducts/replication?command=fetchindex&leaderUrl=http://localhost:8983/solr/techproducts'
  assert_output --partial '"OK"'
  
  # Wish we could block on fetchindex..   Does checking details help?
  sleep 5
  run curl 'http://localhost:7574/solr/techproducts/select?q=*:*'
  assert_output --partial '"numFound":32' 
  
  run bash -c 'solr stop -all 2>&1'
  refute_output --partial 'forcefully killing'
}
