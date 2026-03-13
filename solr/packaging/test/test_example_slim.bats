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

  solr stop --all >/dev/null 2>&1

  # Restore modules directory if it was renamed
  if [ -d "${SOLR_TIP}/modules.backup" ]; then
    mv "${SOLR_TIP}/modules.backup" "${SOLR_TIP}/modules"
  fi
}

@test "start -e techproducts works in slim distribution (no modules dir)" {
  # Simulate a slim distribution by temporarily hiding the Solr modules directory
  # Note: SOLR_TIP points to the distribution directory (e.g., solr/)
  # which contains modules/ (Solr modules) and server/ (Jetty server)
  if [ -d "${SOLR_TIP}/modules" ]; then
    mv "${SOLR_TIP}/modules" "${SOLR_TIP}/modules.backup"
  fi

  # Verify Solr modules directory is not present (slim distribution)
  run test -d "${SOLR_TIP}/modules"
  assert_failure

  # Start techproducts example - should work without Solr modules
  solr start -e techproducts --no-prompt

  # Verify Solr started successfully
  solr assert --started http://localhost:${SOLR_PORT} --timeout 10000

  # Verify the techproducts collection was created
  run curl -s "http://localhost:${SOLR_PORT}/solr/admin/collections?action=LIST"
  assert_output --partial '"techproducts"'

  # Verify documents were indexed (techproducts has 31 docs)
  run curl -s "http://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_output --partial '"numFound":31'

  # Verify that module system properties were NOT set (since modules dir doesn't exist)
  # The solr process should NOT have -Dsolr.modules=... in its command line
  run ps aux
  refute_output --partial "solr.modules=clustering"
}

@test "start -e techproducts includes modules when available" {
  # Verify modules directory exists
  run test -d "${SOLR_TIP}/modules"
  assert_success

  # Start techproducts example
  run solr start -e techproducts --no-prompt
  assert_success

  # Verify Solr started with module-related system properties
  # Check the logs or process for the module properties
  run ps aux
  assert_output --partial "solr.modules=clustering,extraction,langid,ltr,scripting"

  # Verify Solr started successfully
  solr assert --started http://localhost:${SOLR_PORT} --timeout 10000
}