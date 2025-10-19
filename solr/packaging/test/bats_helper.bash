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

# Use this method when in all "teardown"/"teardownFile" functions and any "setup" functions that should not clear the SOLR_HOME directory.
# - "teardown"/"teardownFile" usually stop all Solr processes, so you should not clear the SOLR_HOME directory before they are run.
#   The SOLR_HOME directory will be cleared when the next test file is executed.
# - "setup" should use "common_setup" if a Solr process is NOT being started in that same "setup" function.
common_setup() {
    bats_require_minimum_version 1.8.2

    if [ -z ${BATS_LIB_PREFIX:-} ]; then
        # Debugging help, if you want to run bats directly, try to detect where libraries might be
        if brew list bats-core; then
            BATS_LIB_PREFIX="$(brew --prefix)/lib";
        fi
    fi

    load "${BATS_LIB_PREFIX}/bats-support/load.bash"
    load "${BATS_LIB_PREFIX}/bats-assert/load.bash"
    load "${BATS_LIB_PREFIX}/bats-file/load.bash"

    PATH="${SOLR_TIP:-.}/bin:$PATH"
    export SOLR_ULIMIT_CHECKS=false
}

# Use this method in all "setupFile" functions and any "setup" functions that should start with a clean SOLR_HOME directory.
# - "setupFile" should always start with a clean SOLR_HOME, so "common_clean_setup" should always be used there instead of "common_setup".
# - "setup" should only use "common_clean_setup" if a Solr Process is created in that same "setup" function.
common_clean_setup() {
    common_setup

    if [ -d "${SOLR_HOME}" ]; then
        rm -r "${SOLR_HOME}"
        mkdir "${SOLR_HOME}"
    fi
}

# Use this method in all "teardown" functions
save_home_on_failure() {
    if [[ -z "${BATS_TEST_COMPLETED:-}" ]] && [[ -z "${BATS_TEST_SKIPPED:-}" ]] && [ -d "${SOLR_HOME}" ]; then
        local solrhome_failure_dir="${TEST_FAILURE_DIR}/${BATS_SUITE_TEST_NUMBER}-${BATS_TEST_NUMBER}"
        cp -r "${SOLR_HOME}" "${solrhome_failure_dir}"
        >&2 echo "Please find the SOLR_HOME snapshot for failed test #${BATS_TEST_NUMBER} at: ${solrhome_failure_dir}"
    fi
}

shutdown_all() {
  solr stop --all >/dev/null 2>&1
}

delete_all_collections() {
  local collection_list="$(solr zk ls /collections -z localhost:${ZK_PORT})"
  for collection in $collection_list; do
    if [[ -n $collection ]]; then
      solr delete -c $collection --delete-config >/dev/null 2>&1
    fi
  done
}

config_exists() {
  local config_name=$1
  local config_list=$(solr zk ls /configs -z localhost:${ZK_PORT})

  for config in $config_list; do
    if [[ $(echo $config | tr -d " ") == $config_name ]]; then
      return 0
    fi
  done

  return 1
}

collection_exists() {
  local coll_name=$1
  local coll_list=$(solr zk ls /collections -z localhost:${ZK_PORT})

  for coll in $coll_list; do
    if [[ $(echo $coll | tr -d " ") == $coll_name ]]; then
      return 0
    fi
  done

  return 1
}

# Wait for a collection to be queryable
wait_for_collection() {
  local collection="$1"
  local timeout=${2:-180}
  local start_ts
  start_ts=$(date +%s)
  while true; do
    if curl -s -S -f "http://localhost:${SOLR_PORT}/solr/${collection}/select?q=*:*" | grep -q '"responseHeader"'; then
      return 0
    fi
    local now
    now=$(date +%s)
    if [ $(( now - start_ts )) -ge ${timeout} ]; then
      echo "Timed out waiting for collection '${collection}' to become queryable" >&2
      return 1
    fi
    sleep 3
  done
}

# Apply the ExtractingRequestHandler via Config API and print error body on failure
apply_extract_handler() {
  local collection="$1"
  local json="{\"add-requesthandler\":{\"name\":\"/update/extract\",\"class\":\"org.apache.solr.handler.extraction.ExtractingRequestHandler\",\"tikaserver.url\":\"http://localhost:${TIKA_PORT}\",\"defaults\":{\"lowernames\":\"true\",\"captureAttr\":\"true\"}}}"
  local url="http://localhost:${SOLR_PORT}/solr/${collection}/config"
  # Capture body and status code
  local resp code body
  sleep 5
  resp=$(curl -s -S -w "\n%{http_code}" -X POST -H 'Content-type:application/json' -d "$json" "$url")
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  if [ "$code" = "200" ]; then
    return 0
  else
    echo "Config API error applying ExtractingRequestHandler to ${collection} (HTTP ${code}): ${body}" >&3
    return 1
  fi
}

start_tika_server_docker() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    export TIKA_PORT=$((SOLR_PORT+5))
    docker run --rm -p ${TIKA_PORT}:9998 --name bats_tika -d apache/tika:3.2.3.0-full >/dev/null 2>&1 || true
    echo "Tika Server started on port ${TIKA_PORT}" >&3
  else
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Docker not available (CLI missing or daemon not running); Tika-dependent tests will be bypassed and marked as passed." >&3
  fi
}

stop_tika_server_docker() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Stopping Tika Server container" >&3
    docker stop bats_tika >/dev/null 2>&1 || true
  fi
}
