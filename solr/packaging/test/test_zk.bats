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

@test "listing out files" {
  sleep 1
  run solr zk ls / -z localhost:${ZK_PORT}
  assert_output --partial "aliases.json"
}

@test "copying files around" {
  touch myfile.txt
  # Umm, what is solr cp?  It's like bin/solr zk cp but not?
  run solr cp -src myfile.txt -dst zk:/myfile.txt -z localhost:${ZK_PORT}
  assert_output --partial "Copying from 'myfile.txt' to 'zk:/myfile.txt'. ZooKeeper at localhost:${ZK_PORT}"
  sleep 1
  run solr zk ls / -z localhost:${ZK_PORT}
  assert_output --partial "myfile.txt"

  touch myfile2.txt
  run solr zk cp myfile2.txt zk:myfile2.txt -z localhost:${ZK_PORT}
  assert_output --partial "Copying from 'myfile2.txt' to 'zk:myfile2.txt'. ZooKeeper at localhost:${ZK_PORT}"
  sleep 1
  run solr zk ls / -z localhost:${ZK_PORT}
  assert_output --partial "myfile2.txt"

  rm myfile.txt
  rm myfile2.txt
}

@test "upconfig" {
  local source_configset_dir="${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs"
  test -d $source_configset_dir

  run solr zk upconfig -d ${source_configset_dir} -n techproducts2 -z localhost:${ZK_PORT}
  assert_output --partial "Uploading"
  refute_output --partial "ERROR"

  sleep 1
  run curl "http://localhost:${SOLR_PORT}/api/cluster/configs?omitHeader=true"
  assert_output --partial '"configSets":["_default","techproducts2"]'

}

@test "zkcli.sh gets 'solrhome' from 'solr.home' system property" {
  sleep 1
  run "${SOLR_TIP}/server/scripts/cloud-scripts/zkcli.sh" -v
  local extracted_solrhome=$(echo "$output" | grep -oE "solrhome=[^ ]+")
  # remove 'solrhome='
  local path_value=${extracted_solrhome#*=}
  [[ $path_value == *"/server/scripts/cloud-scripts/../../solr" ]] || [[ $path_value == *"/server/solr" ]]
}

@test "zkcli.sh gets 'solrhome' from 'solrhome' command line option" {
  sleep 1
  run "${SOLR_TIP}/server/scripts/cloud-scripts/zkcli.sh" -v -s /path/to/solr/home
  local extracted_solrhome=$(echo "$output" | grep -oE "solrhome=[^ ]+")
  # remove 'solrhome='
  local path_value=${extracted_solrhome#*=}
  [[ $path_value == "/path/to/solr/home" ]]
}
