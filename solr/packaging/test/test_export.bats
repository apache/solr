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

@test "Check export command" {
  run solr start -c -e techproducts
  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -out "${BATS_TEST_TMPDIR}/output"

  refute_output --partial 'Unrecognized option'
  assert_output --partial 'Export complete'

  assert [ -e ${BATS_TEST_TMPDIR}/output.json ]

  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test"
  assert [ -e techproducts.json ]
  rm techproducts.json

  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -format javabin
  assert [ -e techproducts.javabin ]
  rm techproducts.javabin

  # old pattern of putting a suffix on the out that controlled the format no longer supported ;-).
  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -out "${BATS_TEST_TMPDIR}/output.javabin"
  assert [ -e ${BATS_TEST_TMPDIR}/output.javabin.json ]

  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -out "${BATS_TEST_TMPDIR}"
  assert [ -e ${BATS_TEST_TMPDIR}/techproducts.json ]

  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -format jsonl -out "${BATS_TEST_TMPDIR}/output"
  assert [ -e ${BATS_TEST_TMPDIR}/output.jsonl ]

  run solr export -url "http://localhost:${SOLR_PORT}/solr/techproducts" -query "*:* -id:test" -limit 10 -compress -format jsonl -out "${BATS_TEST_TMPDIR}/output"
  assert [ -e ${BATS_TEST_TMPDIR}/output.jsonl.gz ]
  assert_output --partial 'Total Docs exported: 10'

}

@test "export fails on non cloud mode" {
  run solr start
  run solr create_core -c COLL_NAME
  run solr export -url "http://localhost:${SOLR_PORT}/solr/COLL_NAME"
  refute_output --partial 'Export complete'
  assert_output --partial "ERROR: Couldn't initialize a HttpClusterStateProvider"
}
