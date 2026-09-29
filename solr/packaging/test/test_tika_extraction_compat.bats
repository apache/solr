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

# A real Solr 9.10 release image, used as the oracle for genuine Tika 3.x-era (Tika 1.28.5)
# extraction field names, to compare against this branch's Tika 4.x-based tikaserver backend.
SOLR_910_IMAGE="${SOLR_910_IMAGE:-solr:9.10}"

# Apply the ExtractingRequestHandler via Config API and print the error body on failure.
# $1: base collection URL (e.g. http://localhost:8983/solr/mycollection)
# $2: extra top-level JSON properties (e.g. "tikaserver.url"), including a leading comma (may be
#     empty). These must be top-level handler properties, not nested inside "defaults": that's
#     where ExtractingRequestHandler.inform() reads its init args (tikaserver.url, etc.) from.
apply_extract_handler() {
  local base_url="$1"
  local extra_props="$2"
  local json="{\"add-requesthandler\":{\"name\":\"/update/extract\",\"class\":\"org.apache.solr.handler.extraction.ExtractingRequestHandler\"${extra_props},\"defaults\":{\"lowernames\":\"true\",\"captureAttr\":\"true\"}}}"
  local resp code body
  sleep 5
  resp=$(curl -s -S -w "\n%{http_code}" -X POST -H 'Content-type:application/json' -d "$json" "${base_url}/config")
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  if [ "$code" != "200" ]; then
    echo "Config API error applying ExtractingRequestHandler at ${base_url} (HTTP ${code}): ${body}" >&3
    return 1
  fi
}

setup_file() {
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Docker not available (CLI missing or daemon not running); Tika compatibility tests will be bypassed." >&3
    return
  fi

  export TIKA_PORT=$((SOLR_PORT+5))
  export SOLR_910_PORT=$((SOLR_PORT+10))

  docker run --rm -p ${TIKA_PORT}:9998 --name bats_tika4_compat -d apache/tika:4.0.0-full >/dev/null 2>&1 || true
  docker run --rm -p ${SOLR_910_PORT}:8983 -e SOLR_MODULES=extraction --name bats_solr910_compat -d "$SOLR_910_IMAGE" solr-precreate legacycompat >/dev/null 2>&1 || true

  echo "Waiting for Tika 4 Server to be ready on port ${TIKA_PORT}" >&3
  if ! wait_for 120 3 curl -s -f "http://localhost:${TIKA_PORT}/tika" -o /dev/null; then
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Tika 4 Server did not become ready in time; Tika compatibility tests will be bypassed." >&3
    return
  fi

  echo "Waiting for Solr 9.10 to be ready on port ${SOLR_910_PORT}" >&3
  if ! wait_for 60 3 curl -s -f "http://localhost:${SOLR_910_PORT}/solr/legacycompat/select?q=*:*" -o /dev/null; then
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Solr 9.10 container did not become ready in time; Tika compatibility tests will be bypassed." >&3
    return
  fi
  echo "Tika 4 Server and Solr 9.10 are ready" >&3
}

teardown_file() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Stopping Tika Server and Solr 9.10 containers" >&3
    docker stop bats_tika4_compat >/dev/null 2>&1 || true
    docker stop bats_solr910_compat >/dev/null 2>&1 || true
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

@test "tikaserver.legacyFieldNames restores Tika 3.x-style metadata key names" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    skip "Docker is not available"
  fi

  local pdf="${SOLR_TIP}/example/exampledocs/solr-word.pdf"

  # 1. Extract the PDF via a real Solr 9.10 running its own bundled Tika 1.28.5: a baseline for what
  #    a pre-Tika-4 deployment actually looked like. Note Tika 1.28.5 itself predates the "X-TIKA:"
  #    prefix convention (that arrived later, in Tika 3.x) and used a bare "X-Parsed-By" key; Tika's
  #    own migration table -- and so tikaserver.legacyFieldNames -- targets the Tika 3.x convention
  #    ("X-TIKA:Parsed-By"), not this even-older Tika 1.x one. So this step checks the fields that
  #    are stable across all three extractions (dc_title, dc_creator), not the parsed-by field.
  #    (Tika 4 also drops the plain "title"/"author" aliases Tika 1.28.5 emits alongside dc:title/
  #    dc:creator, so those aren't usable as a cross-version check either.)
  apply_extract_handler "http://localhost:${SOLR_910_PORT}/solr/legacycompat" ""
  curl -s -S -f "http://localhost:${SOLR_910_PORT}/solr/legacycompat/update/extract?literal.id=doc-910&commit=true" \
    -F "myfile=@${pdf}" -o /dev/null

  run curl -s "http://localhost:${SOLR_910_PORT}/solr/legacycompat/select?q=id:doc-910&fl=*"
  assert_success
  assert_output --partial '"numFound":1'
  assert_output --partial '"x_parsed_by"'
  assert_output --partial '"dc_title":["solr-word"]'
  assert_output --partial '"dc_creator":["Grant Ingersoll"]'

  # Free up resources before starting the local Solr instance below: running the Solr 9.10 and
  # Tika 4 containers plus a local Solr JVM all at once can starve the local Solr's Config API
  # calls (which wait on a same-node HTTP round trip) past their internal 30s timeout.
  docker stop bats_solr910_compat >/dev/null 2>&1 || true

  # 2. Extract the same PDF on this branch via the tikaserver backend (real Tika 4 Docker container)
  #    WITHOUT the compat flag: Tika 4's tk: prefix shows up instead of any older-style name.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c tika4only -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/tika4only/select?q=*:*" -o /dev/null
  apply_extract_handler "http://localhost:${SOLR_PORT}/solr/tika4only" \
    ",\"tikaserver.url\":\"http://localhost:${TIKA_PORT}\""

  curl -s -S -f "http://localhost:${SOLR_PORT}/solr/tika4only/update/extract?literal.id=doc-tika4&commit=true" \
    -F "myfile=@${pdf}" -o /dev/null

  run curl -s "http://localhost:${SOLR_PORT}/solr/tika4only/select?q=id:doc-tika4&fl=*"
  assert_success
  assert_output --partial '"numFound":1'
  assert_output --partial '"tk_parsed_by"'
  refute_output --partial '"x_parsed_by"'
  refute_output --partial '"x_tika_parsed_by"'
  assert_output --partial '"dc_title":["solr-word"]'
  assert_output --partial '"dc_creator":["Grant Ingersoll"]'

  # 3. Same extraction, but WITH tikaserver.legacyFieldNames=true: the Tika 4.x key is migrated back
  #    to its Tika 3.x form ("X-TIKA:Parsed-By" -> "x_tika_parsed_by"), per Tika's own migration table.
  solr create -c tika4legacy -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/tika4legacy/select?q=*:*" -o /dev/null
  apply_extract_handler "http://localhost:${SOLR_PORT}/solr/tika4legacy" \
    ",\"tikaserver.url\":\"http://localhost:${TIKA_PORT}\",\"tikaserver.legacyFieldNames\":\"true\""

  curl -s -S -f "http://localhost:${SOLR_PORT}/solr/tika4legacy/update/extract?literal.id=doc-legacy&commit=true" \
    -F "myfile=@${pdf}" -o /dev/null

  run curl -s "http://localhost:${SOLR_PORT}/solr/tika4legacy/select?q=id:doc-legacy&fl=*"
  assert_success
  assert_output --partial '"numFound":1'
  assert_output --partial '"dc_title":["solr-word"]'
  assert_output --partial '"dc_creator":["Grant Ingersoll"]'
  assert_output --partial '"x_tika_parsed_by"'
  refute_output --partial '"tk_parsed_by"'
}
