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
  # Capture Docker container logs and stats on failure for debugging
  if [[ -z "${BATS_TEST_COMPLETED:-}" ]] && [[ -z "${BATS_TEST_SKIPPED:-}" ]]; then
    echo "# Test failed - capturing Docker diagnostics" >&3
    for container in solr-node1 solr-node2 solr-node3; do
      if docker ps -a --format '{{.Names}}' | grep -q "^${container}$" 2>/dev/null; then
        echo "# === Docker logs for $container ===" >&3
        docker logs "$container" | tail -50 >&3 2>&3 || echo "# Failed to get logs for $container" >&3
        echo "# === Docker stats for $container ===" >&3
        docker stats --no-stream "$container" >&3 2>&3 || echo "# Failed to get stats for $container" >&3
        echo "# === Docker inspect for $container ===" >&3
        docker inspect "$container" | jq '.[] | {State: .State, HostConfig: {Memory: .HostConfig.Memory}}' >&3 2>&3 || echo "# Failed to inspect $container" >&3
      fi
    done
  fi

  # Clean up Docker network and containers (--rm should handle container cleanup)
  docker network rm solrcloud-test 2>/dev/null || true
  # Force cleanup in case --rm didn't work
  docker stop solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker rm solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker volume rm solr-data1 solr-data2 solr-data3 2>/dev/null || true
}

@test "Docker SolrCloud rolling upgrade from 9 to 10" {
  # Environment variables for Docker images
  local SOLR_IMAGE_V9="apache/solr-nightly:9.10.0-SNAPSHOT-slim"
  local SOLR_IMAGE_V10="solr:9-slim"

  # Pre-check requirements - fail immediately if not available
  docker version || skip "Docker is not available"
  docker pull "$SOLR_IMAGE_V9" || skip "Docker image $SOLR_IMAGE_V9 is not available"
  docker pull "$SOLR_IMAGE_V10" || skip "Docker image $SOLR_IMAGE_V10 is not available"

  # Create Docker network for consistent networking
  docker network create solrcloud-test

  # Create Docker volumes for data persistence
  docker volume create solr-data1
  docker volume create solr-data2
  docker volume create solr-data3

  echo "Starting solr-node1 with embedded ZooKeeper"
  docker run --name solr-node1 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data1:/var/solr/data \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m --host solr-node1 -p 8983
  docker exec solr-node1 solr assert --started http://solr-node1:8983 --timeout 30000

  # start next 2 in parallel

  echo "Starting solr-node2 connected to first node's ZooKeeper"
  docker run --name solr-node2 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data2:/var/solr/data \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m --host solr-node2 -p 8984 -z solr-node1:9983

  echo "Started solr-node3 connected to first node's ZooKeeper"
  docker run --name solr-node3 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data3:/var/solr/data \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m --host solr-node3 -p 8985 -z solr-node1:9983

  docker exec solr-node2 solr assert --started http://solr-node2:8984 --timeout 30000
  docker exec solr-node3 solr assert --started http://solr-node3:8985 --timeout 30000

  echo "Creating a Collection"
  docker exec --user=solr solr-node1 solr create -c test-collection --shards 3

  # Begin rolling upgrade - upgrade node 3 first (reverse order: 3, 2, 1)
  echo "Starting rolling upgrade - upgrading node 3"
  docker stop solr-node3
  docker run --name solr-node3 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data3:/var/solr/data \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m --host solr-node3 -p 8985 -z solr-node1:9983
  docker exec solr-node3 solr assert --started http://solr-node3:8985 --timeout 30000

  # Upgrade node 2 second
  echo "Upgrading node 2"
  docker stop solr-node2
  docker run --name solr-node2 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data2:/var/solr/data \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m --host solr-node2 -p 8984 -z solr-node1:9983
  docker exec solr-node2 solr assert --started http://solr-node2:8984 --timeout 30000

  echo "Upgrading node 1 (ZK node)"
  docker stop solr-node1
  docker run --name solr-node1 --rm -d \
    --network solrcloud-test \
    --memory=300m \
    -v solr-data1:/var/solr/data \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m --host solr-node1 -p 8983
  docker exec solr-node1 solr assert --started http://solr-node1:8983 --timeout 30000

  # Final verification - validate exactly 3 nodes in cluster
  run docker exec solr-node1 curl -s 'http://solr-node1:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Final cluster status: $output"
  # Verify we have exactly 3 live nodes
  local final_live_nodes_count=$(echo "$output" | grep -o 'solr-node[0-9]:898[0-9]_solr' | wc -l)
  echo "Number of live nodes after rolling upgrade: $final_live_nodes_count"
  [ "$final_live_nodes_count" -eq 3 ]

  echo "Docker SolrCloud rolling upgrade completed successfully!"
}
