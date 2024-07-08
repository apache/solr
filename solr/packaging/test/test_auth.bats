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

  run solr auth disable
}

# Note: there are additional auth related tests in test_ssl.bats

@test "auth rejects block-unknown option with invalid boolean" {
  run ! solr auth enable --type basicAuth --credentials any:any --block-unknown ture
  assert_output --partial "Argument [block-unknown] must be either true or false, but was [ture]"
}

@test "auth rejects update-include-file-only option with invalid boolean" {
  run ! solr auth enable --type basicAuth --credentials any:any --update-include-file-only ture
  assert_output --partial "Argument [update-include-file-only] must be either true or false, but was [ture]"
}

@test "auth enable/disable lifecycle" {
  solr start -c
  solr auth enable --type basicAuth --credentials name:password
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

  run curl -u name:password --basic "http://localhost:${SOLR_PORT}/solr/admin/collections?action=CREATE&collection.configName=_default&name=test&numShards=2&replicationFactor=1&router.name=compositeId&wt=json"
  assert_output --partial '"status":0'

  solr auth disable
  run curl "http://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'
  solr stop --all
}
