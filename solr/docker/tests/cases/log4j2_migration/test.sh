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

set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

# Clean up any existing test containers and volumes
container_cleanup "$container_name"
docker volume rm "${container_name}-vol" 2>/dev/null || true

# Create a Docker volume
echo "Creating Docker volume"
docker volume create "${container_name}-vol"

# Use a temporary container to copy the old log4j2.xml into the volume
echo "Copying old log4j2.xml into volume"
docker run --rm \
  -v "${container_name}-vol:/var/solr" \
  -v "$TEST_DIR/log4j2_old.xml:/tmp/log4j2_old.xml:ro" \
  "$tag" \
  bash -c "cp /tmp/log4j2_old.xml /var/solr/log4j2.xml"

# Verify initial state - file should contain old property name
echo "Verifying initial log4j2.xml contains old property name"
old_log4j=$(docker run --rm -v "${container_name}-vol:/var/solr" -e NO_INIT_VAR_SOLR=1 "$tag" cat /var/solr/log4j2.xml)
if ! grep -q 'solr\.log\.dir' <<<"$old_log4j"; then
  echo "Test setup failed; log4j2.xml does not contain solr.log.dir"
  docker volume rm "${container_name}-vol"
  exit 1
fi
echo "Confirmed: log4j2.xml contains solr.log.dir property"

# Start Solr with the volume (init-var-solr will run and migrate the file)
echo "Running $container_name with volume"
docker run --name "$container_name" -d \
  -v "${container_name}-vol:/var/solr" \
  "$tag" solr-precreate gettingstarted

wait_for_container_and_solr "$container_name"

# Verify the log4j2.xml has been migrated
echo "Verifying log4j2.xml was migrated to new property name"
migrated_log4j=$(docker exec "$container_name" cat /var/solr/log4j2.xml)

if grep -q 'solr\.log\.dir' <<<"$migrated_log4j"; then
  echo "Test $TEST_NAME $tag failed; log4j2.xml still contains old solr.log.dir property"
  docker exec "$container_name" cat /var/solr/log4j2.xml
  container_cleanup "$container_name"
  docker volume rm "${container_name}-vol"
  exit 1
fi

if ! grep -q 'solr\.logs\.dir' <<<"$migrated_log4j"; then
  echo "Test $TEST_NAME $tag failed; log4j2.xml does not contain new solr.logs.dir property"
  docker exec "$container_name" cat /var/solr/log4j2.xml
  container_cleanup "$container_name"
  docker volume rm "${container_name}-vol"
  exit 1
fi

echo "Confirmed: log4j2.xml successfully migrated from solr.log.dir to solr.logs.dir"

# Clean up
container_cleanup "$container_name"
docker volume rm "${container_name}-vol"

echo "Test $TEST_NAME $tag succeeded"
