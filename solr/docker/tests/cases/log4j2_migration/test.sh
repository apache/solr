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
container_cleanup "${container_name}-old"
docker volume rm "${container_name}-vol" 2>/dev/null || true

# Create a named volume and start Solr 9.10.0 to initialize it
echo "Creating volume and starting Solr 9.10.0"
docker volume create "${container_name}-vol"

echo "Running $container_name-old with Solr 9.10.0"
docker run --name "${container_name}-old" -d \
  -v "${container_name}-vol:/var/solr" \
  solr:9.10.0 solr-precreate gettingstarted

wait_for_container_and_solr "${container_name}-old"

# Verify the old log4j2.xml exists with old property name
echo "Verifying log4j2.xml was created with old property name"
old_log4j=$(docker exec "${container_name}-old" cat /var/solr/log4j2.xml)
if ! grep -q 'solr\.log\.dir' <<<"$old_log4j"; then
  echo "Test $TEST_NAME $tag failed; log4j2.xml does not contain solr.log.dir"
  container_cleanup "${container_name}-old"
  docker volume rm "${container_name}-vol"
  exit 1
fi
echo "Confirmed: log4j2.xml contains solr.log.dir property"

# Stop and remove the old container, keeping the volume
echo "Stopping Solr 9.10.0 container"
container_cleanup "${container_name}-old"

# Start the new Solr version with the same volume
echo "Starting new Solr version ($tag) with existing volume"
docker run --name "$container_name" -d \
  -v "${container_name}-vol:/var/solr" \
  "$tag" solr-precreate gettingstarted

wait_for_container_and_solr "$container_name"

# Verify the log4j2.xml has been migrated to use new property name
echo "Verifying log4j2.xml was migrated to new property name"
new_log4j=$(docker exec "$container_name" cat /var/solr/log4j2.xml)
if grep -q 'solr\.log\.dir' <<<"$new_log4j"; then
  echo "Test $TEST_NAME $tag failed; log4j2.xml still contains old solr.log.dir property"
  docker exec "$container_name" cat /var/solr/log4j2.xml
  container_cleanup "$container_name"
  docker volume rm "${container_name}-vol"
  exit 1
fi

if ! grep -q 'solr\.logs\.dir' <<<"$new_log4j"; then
  echo "Test $TEST_NAME $tag failed; log4j2.xml does not contain new solr.logs.dir property"
  docker exec "$container_name" cat /var/solr/log4j2.xml
  container_cleanup "$container_name"
  docker volume rm "${container_name}-vol"
  exit 1
fi

echo "Confirmed: log4j2.xml successfully migrated from solr.log.dir to solr.logs.dir"

# Verify Solr is still functional after migration
echo "Loading data to verify Solr is functional"
docker exec --user=solr "$container_name" bin/solr post -c gettingstarted example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/gettingstarted/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load after migration"
  container_cleanup "$container_name"
  docker volume rm "${container_name}-vol"
  exit 1
fi

echo "Confirmed: Solr is functional after log4j2.xml migration"

# Clean up
container_cleanup "$container_name"
docker volume rm "${container_name}-vol"

echo "Test $TEST_NAME $tag succeeded"
