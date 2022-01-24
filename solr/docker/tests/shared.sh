#!/bin/bash
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

# Shared functions for testing

function container_cleanup {
  local container_name
  container_name=$1
  previous=$(docker inspect "$container_name" --format '{{.ID}}' 2>/dev/null || true)
  if [[ -n $previous ]]; then
    container_status=$(docker inspect --format='{{.State.Status}}' "$previous" 2>/dev/null)
    if [[ $container_status == 'running' ]]; then
      echo "killing $previous"
      docker kill "$previous" 2>/dev/null || true
      sleep 2
    fi
    echo "removing $previous"
    docker rm "$previous" 2>/dev/null || true
  fi
}

function wait_for_container_and_solr {
  local container_name
  container_name=$1
  wait_for_server_started "$container_name" 0

  printf '\nWaiting for Solr...\n'
  local status
  status=$(docker exec "$container_name" wait-for-solr.sh --max-attempts 60 --wait-seconds 1)
#  echo "Got status from Solr: $status"
  if ! grep -E -i -q 'Solr is running' <<<"$status"; then
    echo "Solr did not start"
    container_cleanup "$container_name"
    exit 1
  else
    echo "Solr is running"
  fi
  sleep 4
}

function wait_for_container_and_solr_exporter {
  local container_name
  container_name=$1
  wait_for_server_started "$container_name" 0 'org.apache.solr.prometheus.exporter.SolrExporter; Solr Prometheus Exporter is running'

  printf '\nWaiting for Solr Prometheus Exporter...\n'
  local status
  status=$(docker exec --env SOLR_PORT=8989 "$container_name" wait-for-solr.sh --max-attempts 15 --wait-seconds 1)
#  echo "Got status from Solr Prometheus Exporter: $status"
  if ! grep -E -i -q 'Solr is running' <<<"$status"; then
    echo "Solr Prometheus Exporter did not start"
    container_cleanup "$container_name"
    exit 1
  else
    echo "Solr Prometheus Exporter is running"
  fi
  sleep 4
}

function wait_for_server_started {
  local container_name
  container_name=$1
  local sleep_time
  sleep_time=5
  if [ -n "${2:-}" ]; then
    sleep_time=$2
  fi
  local log_grep
  log_grep='(o\.e\.j\.s\.Server Started|Started SocketConnector)'
  if [ -n "${3:-}" ]; then
    log_grep=$3
  fi
  echo "Waiting for container start: $container_name"
  local TIMEOUT_SECONDS
  TIMEOUT_SECONDS=$(( 5 * 60 ))
  local started
  started=$(date +%s)
  local log
  log="${BUILD_DIR}/${container_name}.log"
  while true; do
    docker logs "$container_name" > "${log}" 2>&1
    if grep -E -q "${log_grep}" "${log}" ; then
      docker logs "$container_name"
      break
    fi

    local container_status
    container_status=$(docker inspect --format='{{.State.Status}}' "$container_name")
    if [[ $container_status == 'exited' ]]; then
      docker logs "$container_name"
      exit 1
    fi

    if (( $(date +%s) > started + TIMEOUT_SECONDS )); then
      echo "giving up after $TIMEOUT_SECONDS seconds"
      exit 1
    fi
    printf '.'
    sleep 2
  done
  echo "Server started"
  rm "${log}"
  sleep "$sleep_time"
}

function prepare_dir_to_mount {
  local userid
  userid=8983
  local folder
  folder="${BUILD_DIR}/myvarsolr"
  if [ -n "$1" ]; then
    userid=$1
  fi
  if [ -n "$2" ]; then
    folder=$2
  fi
  rm -fr "$folder" >/dev/null 2>&1
  mkdir "$folder"
  #echo "***** Created varsolr folder $BUILD_DIR / $folder"

  # The /var/solr mountpoint is owned by solr, so when we bind mount there our directory,
  # owned by the current user in the host, will show as owned by solr, and our attempts
  # to write to it as the user will fail. To deal with that, set the ACL to allow that.
  # If you can't use setfacl (eg on macOS), you'll have to chown the directory to 8983, or apply world
  # write permissions.
  if ! command -v setfacl &> /dev/null; then
    echo "Test case requires the 'setfacl' command but it can not be found. Will set the directory to have read/write all permissions"
    chmod a+rwx "$folder"
  fi
  if ! setfacl -m "u:$userid:rwx" "$folder"; then
    echo "Unable to add permissions for $userid to '$folder'"
  fi
}


# Shared setup

if (( $# == 0 )); then
  echo "Usage: ${TEST_DIR}/test.sh <tag>"
  exit
fi
tag=$1

if [[ -n "${DEBUG:-}" ]]; then
  set -x
fi

TEST_NAME="$(basename -- "${TEST_DIR}")"

# Create build directory if it hasn't been provided already
if [[ -z ${BUILD_DIR:-} ]]; then
  BASE_DIR="$(dirname -- "${BASH_SOURCE-$0}")"
  BASE_DIR="$(cd "${BASE_DIR}/.." && pwd)"
  BUILD_DIR="${BASE_DIR}/build/tmp/tests/${TEST_NAME}"
fi
mkdir -p "${BUILD_DIR}"

echo "Test $TEST_DIR $tag"
echo "Test logs and build files can be found at: ${BUILD_DIR}"
container_name="test-$(echo "${TEST_NAME}" | tr ':/-' '_')-$(echo "${tag}" | tr ':/-' '_')"

echo "Cleaning up left-over containers from previous runs"
container_cleanup "${container_name}"
