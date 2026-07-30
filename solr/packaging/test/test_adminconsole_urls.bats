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
}


@test "assert able to launch solr admin console" {
  run solr start

  run curl -s -o /dev/null -w "%{http_code}" http://localhost:${SOLR_PORT}/solr/
  assert_output "200"

  run curl -s -I http://localhost:${SOLR_PORT}/solr/
  assert_output --partial "text/html"

  run curl -s http://localhost:${SOLR_PORT}/solr/
  assert_output --partial "<html"

  run curl -s -o /dev/null -w "%{http_code}" http://localhost:${SOLR_PORT}/solr/css/angular/chosen.css
  assert_output "200"

  run curl -s -I http://localhost:${SOLR_PORT}/solr/css/angular/chosen.css
  assert_output --partial "text/css"

  run curl -s http://localhost:${SOLR_PORT}/solr/css/angular/chosen.css
  assert_output --partial "{"
}

@test "assert CSP header contains custom connect src URLs" {
  # Set custom CSP connect-src URLs via system property
  local csp_urls="http://example1.com/token,https://example2.com/path/uri1,http://example3.com/oauth2/uri2"

  run solr start -Dsolr.ui.headers.csp.connect-src.urls="${csp_urls}"

  run curl -s -I http://localhost:${SOLR_PORT}/solr/
  assert_output --partial "http://example1.com/token"
  assert_output --partial "https://example2.com/path/uri1"
  assert_output --partial "http://example3.com/oauth2/uri2"
}
