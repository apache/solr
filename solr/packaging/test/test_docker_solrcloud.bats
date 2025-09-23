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

  # Clean up Docker containers
  docker stop solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker rm solr-node1 solr-node2 solr-node3 2>/dev/null || true
}

@test "Docker SolrCloud cluster with 3 nodes and embedded ZooKeeper" {
  # Skip test if Docker is not available
  run docker version
  if [ $status -ne 0 ]; then
    skip "Docker is not available"
  fi

  # Pull the Docker image if not present
  run docker pull apache/solr-nightly:9.10.0-SNAPSHOT-slim
  if [ $status -ne 0 ]; then
    skip "Docker image apache/solr-nightly:9.10.0-SNAPSHOT-slim is not available"
  fi

  # Start first Solr node with embedded ZooKeeper (memory limited to 300MB)
  run docker run --name solr-node1 -d \
    -p 8981:8983 \
    --memory=300m \
    apache/solr-nightly:9.10.0-SNAPSHOT-slim solr start -f -c -m 256m
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

  # Start second Solr node connected to first node's ZooKeeper (memory limited to 300MB)
  run docker run --name solr-node2 -d \
    -p 8982:8983 \
    --memory=300m \
    --env "ZK_HOST=${solr_ip1}:9983" \
    apache/solr-nightly:9.10.0-SNAPSHOT-slim solr start -f -c -m 256m
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

  # Start third Solr node connected to first node's ZooKeeper (memory limited to 300MB)
  run docker run --name solr-node3 -d \
    -p 8984:8983 \
    --memory=300m \
    --env "ZK_HOST=${solr_ip1}:9983" \
    apache/solr-nightly:9.10.0-SNAPSHOT-slim solr start -f -c -m 256m
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
  # Assert no serious errors in logs (allow deprecation warnings)
  ! echo "$output" | grep -E "(ERROR.*Exception|SEVERE|FATAL)" | grep -v "WARNING"

  # Check for specific log messages on second node and ensure no errors
  run docker logs solr-node2
  [ $status -eq 0 ]
  echo "Checking logs from second node for ZooKeeper client messages and no errors"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper client=" ]] || [[ "$output" =~ "zkClient has connected" ]]
  # Assert no serious errors in logs (allow deprecation warnings)
  ! echo "$output" | grep -E "(ERROR.*Exception|SEVERE|FATAL)" | grep -v "WARNING"

  # Check for specific log messages on third node and ensure no errors
  run docker logs solr-node3
  [ $status -eq 0 ]
  echo "Checking logs from third node for ZooKeeper client messages and no errors"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper client=" ]] || [[ "$output" =~ "zkClient has connected" ]]
  # Assert no serious errors in logs (allow deprecation warnings)
  ! echo "$output" | grep -E "(ERROR.*Exception|SEVERE|FATAL)" | grep -v "WARNING"

  # Create a collection successfully with 3 shards across all nodes
  run docker exec --user=solr solr-node1 solr create -c test-collection --shards 3 -rf 2
  [ $status -eq 0 ]
  echo "Collection creation output: $output"
  [[ "$output" =~ "Created collection" ]] || [[ "$output" =~ "test-collection" ]]

  # Verify collection was created by querying it from each node
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/test-collection/select?q=*:*'
  [ $status -eq 0 ]
  echo "Query result from first node: $output"
  [[ "$output" =~ '"numFound"' ]]

  run docker exec solr-node2 curl -s 'http://localhost:8983/solr/test-collection/select?q=*:*'
  [ $status -eq 0 ]
  echo "Query result from second node: $output"
  [[ "$output" =~ '"numFound"' ]]

  run docker exec solr-node3 curl -s 'http://localhost:8983/solr/test-collection/select?q=*:*'
  [ $status -eq 0 ]
  echo "Query result from third node: $output"
  [[ "$output" =~ '"numFound"' ]]

  echo "Docker SolrCloud cluster test with 3 nodes completed successfully!"
}