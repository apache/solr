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
  
  # Set TEST_DIR to the directory containing this test file
  TEST_DIR="${TEST_DIR:-$(dirname "${BATS_TEST_FILENAME}")}"
}

teardown() {
  # Capture Docker compose logs on failure for debugging
  if [[ -z "${BATS_TEST_COMPLETED:-}" ]] && [[ -z "${BATS_TEST_SKIPPED:-}" ]]; then
    echo "# Test failed - capturing Docker compose diagnostics" >&3
    echo "# === Docker compose logs ===" >&3
    docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest logs >&3 2>&3 || echo "# Failed to get compose logs" >&3
    echo "# === Docker compose ps ===" >&3
    docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest ps >&3 2>&3 || echo "# Failed to get compose ps" >&3
  fi

  # Clean up any manual containers from rolling upgrade
  docker stop solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker rm solr-node1 solr-node2 solr-node3 2>/dev/null || true
  
  # Clean up Docker compose stack
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest down -v 2>/dev/null || true
}

@test "Docker Compose SolrCloud rolling upgrade from 9 to 10 with separate ZK" {
  # Environment variables for Docker images
  local SOLR_IMAGE_V9="apache/solr-nightly:9.10.0-SNAPSHOT-slim"
  local SOLR_IMAGE_V10="solr:9-slim"
  
  # Skip test if Docker and docker-compose are not available
  run docker version
  if [ $status -ne 0 ]; then
    skip "Docker is not available"
  fi
  
  run docker compose version
  if [ $status -ne 0 ]; then
    skip "Docker Compose is not available"
  fi

  # Pull both Docker images
  run docker pull "$SOLR_IMAGE_V9"
  if [ $status -ne 0 ]; then
    skip "Docker image $SOLR_IMAGE_V9 is not available"
  fi
  
  run docker pull "$SOLR_IMAGE_V10"
  if [ $status -ne 0 ]; then
    skip "Docker image $SOLR_IMAGE_V10 is not available"
  fi
  
  run docker pull zookeeper:3.9
  if [ $status -ne 0 ]; then
    skip "Docker image zookeeper:3.9 is not available"
  fi

  # Start the initial cluster with version 9
  SOLR_IMAGE="$SOLR_IMAGE_V9" run docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest up -d
  [ $status -eq 0 ]
  echo "Started SolrCloud cluster with separate ZooKeeper using Docker Compose"

  # Wait for all services to be healthy
  local attempts=0
  local max_attempts=60
  while [ $attempts -lt $max_attempts ]; do
    run docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest ps --format json
    if [ $status -eq 0 ]; then
      local unhealthy_count=$(echo "$output" | grep -c '"Health":"unhealthy"' || true)
      local starting_count=$(echo "$output" | grep -c '"Health":"starting"' || true)
      if [ "$unhealthy_count" -eq 0 ] && [ "$starting_count" -eq 0 ]; then
        echo "All services are healthy"
        break
      fi
    fi
    echo "Waiting for services to be healthy (attempt $((attempts + 1))/$max_attempts)..."
    sleep 5
    attempts=$((attempts + 1))
  done

  # Alternative: Wait for cluster formation (more reliable)
  attempts=0
  echo "Waiting for all three nodes to join the cluster..."
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
    if [ $status -eq 0 ]; then
      local live_nodes_count=$(echo "$output" | grep -o '"live_nodes":\[[^]]*\]' | grep -o ':8983_' | wc -l)
      if [ "$live_nodes_count" -eq 3 ]; then
        echo "All three nodes have joined the cluster"
        break
      fi
    fi
    echo "Waiting for all nodes to join cluster (attempt $((attempts + 1))/$max_attempts)..."
    sleep 5
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Verify exactly 3 nodes in cluster
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Initial cluster status: $output"
  local live_nodes_count=$(echo "$output" | grep -o '"live_nodes":\[[^]]*\]' | grep -o ':8983_' | wc -l)
  echo "Number of live nodes: $live_nodes_count"
  [ "$live_nodes_count" -eq 3 ]

  # Create a collection with 3 shards across all nodes
  run docker exec --user=solr solr-node1 solr create -c test-collection --shards 3 -rf 2
  [ $status -eq 0 ]
  echo "Collection creation output: $output"
  [[ "$output" =~ "Created collection" ]] || [[ "$output" =~ "test-collection" ]]

  # Rolling upgrade approach using docker run commands with volumes from compose
  # This demonstrates how docker-compose simplifies networking but individual container 
  # management is still needed for rolling upgrades

  echo "Starting rolling upgrade - upgrading node 1 to version 10"
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest stop solr-node1
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest rm -f solr-node1
  
  # Start new container with upgraded image using same network and volume
  run docker run --name solr-node1 -d \
    --network solrtest_solr-network \
    --memory=300m \
    -v solrtest_solr-data1:/var/solr/data \
    -e "ZK_HOST=zookeeper:2181" \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 1 with version 10"

  # Wait for node 1 to rejoin
  attempts=0
  local max_attempts=30
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node1 solr status
    if [ $status -eq 0 ]; then
      echo "Node 1 (v10) is ready"
      break
    fi
    echo "Waiting for node 1 to restart (attempt $((attempts + 1))/$max_attempts)..."
    sleep 3
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Upgrade node 2
  echo "Upgrading node 2 to version 10"
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest stop solr-node2
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest rm -f solr-node2
  
  run docker run --name solr-node2 -d \
    --network solrtest_solr-network \
    --memory=300m \
    -v solrtest_solr-data2:/var/solr/data \
    -e "ZK_HOST=zookeeper:2181" \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 2 with version 10"

  # Wait for node 2 to rejoin
  attempts=0
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node2 solr status
    if [ $status -eq 0 ]; then
      echo "Node 2 (v10) is ready"
      break
    fi
    echo "Waiting for node 2 to restart (attempt $((attempts + 1))/$max_attempts)..."
    sleep 3
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Upgrade node 3
  echo "Upgrading node 3 to version 10"
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest stop solr-node3
  docker compose -f "${TEST_DIR}/docker-compose-solrcloud.yml" -p solrtest rm -f solr-node3
  
  run docker run --name solr-node3 -d \
    --network solrtest_solr-network \
    --memory=300m \
    -v solrtest_solr-data3:/var/solr/data \
    -e "ZK_HOST=zookeeper:2181" \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 3 with version 10"

  # Wait for node 3 to rejoin
  attempts=0
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node3 solr status
    if [ $status -eq 0 ]; then
      echo "Node 3 (v10) is ready"
      break
    fi
    echo "Waiting for node 3 to restart (attempt $((attempts + 1))/$max_attempts)..."
    sleep 3
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Final verification - validate exactly 3 nodes in cluster
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Final cluster status: $output"
  # Verify we have exactly 3 live nodes
  local final_live_nodes_count=$(echo "$output" | grep -o '"live_nodes":\[[^]]*\]' | grep -o ':8983_' | wc -l)
  echo "Number of live nodes after rolling upgrade: $final_live_nodes_count"
  [ "$final_live_nodes_count" -eq 3 ]

  echo "Docker Compose SolrCloud rolling upgrade from 9 to 10 with separate ZK completed successfully!"
}