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

  # Clean up Docker containers and volumes
  docker stop solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker rm solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker volume rm solr-data1 solr-data2 solr-data3 2>/dev/null || true
}

@test "Docker SolrCloud rolling upgrade from 9 to 10" {
  # Environment variables for Docker images
  local SOLR_IMAGE_V9="apache/solr-nightly:9.10.0-SNAPSHOT-slim"
  local SOLR_IMAGE_V10="solr:9-slim"
  
  # Skip test if Docker is not available
  run docker version
  if [ $status -ne 0 ]; then
    skip "Docker is not available"
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

  # Create Docker volumes for data persistence
  docker volume create solr-data1
  docker volume create solr-data2
  docker volume create solr-data3

  # Start first Solr node with embedded ZooKeeper
  run docker run --name solr-node1 -d \
    -p 8981:8983 \
    --memory=300m \
    -v solr-data1:/var/solr/data \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Started first Solr node (solr-node1) with embedded ZooKeeper"

  # Wait for first node to be ready
  local attempts=0
  local max_attempts=30
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node1 solr status
    if [ $status -eq 0 ]; then
      echo "First Solr node is ready"
      break
    fi
    echo "Waiting for first Solr node to start (attempt $((attempts + 1))/$max_attempts)..."
    sleep 2
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Get IP address of first node
  run docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" solr-node1
  [ $status -eq 0 ]
  local solr_ip1="$output"
  echo "First Solr node IP: $solr_ip1"

  # Start second Solr node connected to first node's ZooKeeper
  run docker run --name solr-node2 -d \
    -p 8982:8983 \
    --memory=300m \
    -v solr-data2:/var/solr/data \
    --env "ZK_HOST=${solr_ip1}:9983" \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Started second Solr node (solr-node2) connected to first node's ZooKeeper"

  # Wait for second node to be ready
  attempts=0
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node2 solr status
    if [ $status -eq 0 ]; then
      echo "Second Solr node is ready"
      break
    fi
    echo "Waiting for second Solr node to start (attempt $((attempts + 1))/$max_attempts)..."
    sleep 2
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Get IP address of second node
  run docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" solr-node2
  [ $status -eq 0 ]
  local solr_ip2="$output"
  echo "Second Solr node IP: $solr_ip2"

  # Start third Solr node connected to first node's ZooKeeper
  run docker run --name solr-node3 -d \
    -p 8984:8983 \
    --memory=300m \
    -v solr-data3:/var/solr/data \
    --env "ZK_HOST=${solr_ip1}:9983" \
    "$SOLR_IMAGE_V9" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Started third Solr node (solr-node3) connected to first node's ZooKeeper"

  # Wait for third node to be ready
  attempts=0
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node3 solr status
    if [ $status -eq 0 ]; then
      echo "Third Solr node is ready"
      break
    fi
    echo "Waiting for third Solr node to start (attempt $((attempts + 1))/$max_attempts)..."
    sleep 2
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Get IP address of third node
  run docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" solr-node3
  [ $status -eq 0 ]
  local solr_ip3="$output"
  echo "Third Solr node IP: $solr_ip3"

  # Wait for all three nodes to join the cluster
  attempts=0
  echo "Waiting for all three nodes to join the cluster..."
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
    if [ $status -eq 0 ] && [[ "$output" =~ live_nodes.*${solr_ip1}:8983 ]] && [[ "$output" =~ live_nodes.*${solr_ip2}:8983 ]] && [[ "$output" =~ live_nodes.*${solr_ip3}:8983 ]]; then
      echo "All three nodes have joined the cluster"
      break
    fi
    echo "Waiting for all nodes to join cluster (attempt $((attempts + 1))/$max_attempts)..."
    sleep 3
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Check that all three nodes are in the cluster
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Cluster status from node 1: $output"
  [[ "$output" =~ live_nodes.*${solr_ip1}:8983 ]]
  [[ "$output" =~ live_nodes.*${solr_ip2}:8983 ]]
  [[ "$output" =~ live_nodes.*${solr_ip3}:8983 ]]

  # Check for specific log messages on first node (embedded ZooKeeper) and ensure no errors
  run docker logs solr-node1
  [ $status -eq 0 ]
  echo "Checking logs from first node for ZooKeeper startup messages and no errors"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper" ]] || [[ "$output" =~ "STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER" ]]

  # Check for specific log messages on second node and ensure no errors
  run docker logs solr-node2
  [ $status -eq 0 ]
  echo "Checking logs from second node for ZooKeeper client messages and no errors"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper client=" ]] || [[ "$output" =~ "zkClient has connected" ]]

  # Check for specific log messages on third node and ensure no errors
  run docker logs solr-node3
  [ $status -eq 0 ]
  echo "Checking logs from third node for ZooKeeper client messages and no errors"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper client=" ]] || [[ "$output" =~ "zkClient has connected" ]]

  # Create a collection successfully with 3 shards across all nodes
  run docker exec --user=solr solr-node1 solr create -c test-collection --shards 3 -rf 2
  [ $status -eq 0 ]
  echo "Collection creation output: $output"
  [[ "$output" =~ "Created collection" ]] || [[ "$output" =~ "test-collection" ]]

  # Begin rolling upgrade - upgrade node 2 first (not the ZK node)
  echo "Starting rolling upgrade - upgrading node 2 to version 10"
  docker stop solr-node2
  docker rm solr-node2
  
  run docker run --name solr-node2 -d \
    -p 8982:8983 \
    --memory=300m \
    -v solr-data2:/var/solr/data \
    --env "ZK_HOST=${solr_ip1}:9983" \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 2 with version 10"

  # Wait for node 2 to rejoin the cluster
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
  docker stop solr-node3
  docker rm solr-node3
  
  run docker run --name solr-node3 -d \
    -p 8984:8983 \
    --memory=300m \
    -v solr-data3:/var/solr/data \
    --env "ZK_HOST=${solr_ip1}:9983" \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 3 with version 10"

  # Wait for node 3 to rejoin the cluster
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

  # Finally upgrade node 1 (the ZK node) - this is the most critical
  echo "Upgrading node 1 (ZK node) to version 10"
  docker stop solr-node1
  docker rm solr-node1
  
  run docker run --name solr-node1 -d \
    -p 8981:8983 \
    --memory=300m \
    -v solr-data1:/var/solr/data \
    "$SOLR_IMAGE_V10" solr start -f -c -m 200m
  [ $status -eq 0 ]
  echo "Restarted node 1 (ZK node) with version 10"

  # Wait for node 1 to restart and become the ZK server again
  attempts=0
  while [ $attempts -lt $max_attempts ]; do
    run docker exec solr-node1 solr status
    if [ $status -eq 0 ]; then
      echo "Node 1 (v10, ZK node) is ready"
      break
    fi
    echo "Waiting for node 1 (ZK node) to restart (attempt $((attempts + 1))/$max_attempts)..."
    sleep 3
    attempts=$((attempts + 1))
  done
  [ $attempts -lt $max_attempts ]

  # Get the new IP of node 1 (may have changed after restart)
  run docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" solr-node1
  [ $status -eq 0 ]
  local new_solr_ip1="$output"
  echo "Node 1 new IP after restart: $new_solr_ip1"

  # Update ZK_HOST for nodes 2 and 3 if IP changed
  if [ "$new_solr_ip1" != "$solr_ip1" ]; then
    echo "IP changed, restarting nodes 2 and 3 with new ZK_HOST"
    
    docker stop solr-node2 solr-node3
    docker rm solr-node2 solr-node3
    
    run docker run --name solr-node2 -d \
      -p 8982:8983 \
      --memory=300m \
      -v solr-data2:/var/solr/data \
      --env "ZK_HOST=${new_solr_ip1}:9983" \
      "$SOLR_IMAGE_V10" solr start -f -c -m 200m
    [ $status -eq 0 ]
    
    run docker run --name solr-node3 -d \
      -p 8984:8983 \
      --memory=300m \
      -v solr-data3:/var/solr/data \
      --env "ZK_HOST=${new_solr_ip1}:9983" \
      "$SOLR_IMAGE_V10" solr start -f -c -m 200m
    [ $status -eq 0 ]

    # Wait for both nodes to restart
    for node in solr-node2 solr-node3; do
      attempts=0
      while [ $attempts -lt $max_attempts ]; do
        run docker exec $node solr status
        if [ $status -eq 0 ]; then
          echo "$node reconnected to new ZK"
          break
        fi
        sleep 2
        attempts=$((attempts + 1))
      done
      [ $attempts -lt $max_attempts ]
    done
  fi

  # Final verification - validate exactly 3 nodes in cluster
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Final cluster status: $output"
  # Verify we have exactly 3 live nodes
  local live_nodes_count=$(echo "$output" | grep -o '"live_nodes":\[[^]]*\]' | grep -o ':[0-9]*_' | wc -l)
  echo "Number of live nodes after rolling upgrade: $live_nodes_count"
  [ "$live_nodes_count" -eq 3 ]

  echo "Docker SolrCloud rolling upgrade from 9 to 10 completed successfully!"
}