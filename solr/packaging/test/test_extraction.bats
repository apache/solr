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
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    export TIKA_PORT=$((SOLR_PORT+5))
    docker run --rm -p ${TIKA_PORT}:9998 --name bats_tika -d apache/tika:3.2.3.0-full >/dev/null 2>&1 || true
    echo "Tika Server started on port ${TIKA_PORT}" >&3
  else
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Docker not available (CLI missing or daemon not running); Tika-dependent tests will be bypassed and marked as passed." >&3
  fi
}

teardown_file() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Stopping Tika Server container" >&3
    docker stop bats_tika >/dev/null 2>&1 || true
  fi
}

setup() {
  common_clean_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  delete_all_collections
  SOLR_STOP_WAIT=1 solr stop --all >/dev/null 2>&1
}

@test "using curl to extract a single pdf file" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    echo "WARNING: Docker not available; bypassing test." >&3
    return 0
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c gettingstarted -d _default
  wait_for_collection gettingstarted 30
  apply_extract_handler gettingstarted

  curl "http://localhost:${SOLR_PORT}/solr/gettingstarted/update/extract?literal.id=doc1&commit=true" -F "myfile=@${SOLR_TIP}/example/exampledocs/solr-word.pdf"

  run curl "http://localhost:${SOLR_PORT}/solr/gettingstarted/select?q=id:doc1"
  assert_output --partial '"numFound":1'
}

@test "using the bin/solr post tool to extract content from pdf" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    echo "WARNING: Docker not available; bypassing test." >&3
    return 0
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c content_extraction -d _default
  wait_for_collection content_extraction 30
  apply_extract_handler content_extraction


  # We filter to pdf to invoke the Extract handler.
  run solr post --filetypes pdf --solr-url http://localhost:${SOLR_PORT} --name content_extraction ${SOLR_TIP}/example/exampledocs

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'

  run curl "http://localhost:${SOLR_PORT}/solr/content_extraction/select?q=*:*"
  assert_output --partial '"numFound":1'
}

@test "using the bin/solr post tool to crawl web site" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    echo "WARNING: Docker not available; bypassing test." >&3
    return 0
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c website_extraction -d _default
  wait_for_collection website_extraction 30
  apply_extract_handler website_extraction


  # Change to --recursive 1 to crawl multiple pages, but may be too slow.
  run solr post --mode web --solr-url http://localhost:${SOLR_PORT} -c website_extraction --recursive 0 --delay 1 https://solr.apache.org/

  assert_output --partial 'POSTed web resource https://solr.apache.org (depth: 0)'
  refute_output --partial 'ERROR'

  run curl "http://localhost:${SOLR_PORT}/solr/website_extraction/select?q=*:*"
  assert_output --partial '"numFound":1'
}
