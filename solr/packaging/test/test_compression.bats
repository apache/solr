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
  solr start -e films
}

setup() {
  common_setup
}

teardown_file() {
  save_home_on_failure
  solr stop --all >/dev/null 2>&1
}

@test "server does not compress response without Accept-Encoding header" {
  run curl -s -D - -o /dev/null "http://localhost:${SOLR_PORT}/solr/films/select?q=*:*&rows=100"
  refute_output --partial "Content-Encoding:"
}

@test "server compresses response when Accept-Encoding: gzip is requested" {
  run curl -s -D - -o /dev/null -H "Accept-Encoding: gzip" "http://localhost:${SOLR_PORT}/solr/films/select?q=*:*&rows=100"
  assert_output --partial "gzip"
}

@test "compressed response can be decompressed and parsed" {
  run curl -s --compressed "http://localhost:${SOLR_PORT}/solr/films/select?q=*:*&rows=100"
  assert_output --partial '"status":0'
}
