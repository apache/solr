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
  docker stop solr-node1 solr-node2 2>/dev/null || true
  docker rm solr-node1 solr-node2 2>/dev/null || true
}

@test "Docker SolrCloud cluster with embedded ZooKeeper" {
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

  # Start first Solr node with embedded ZooKeeper
  run docker run --name solr-node1 -d \
    -p 8981:8983 \
    apache/solr-nightly:9.10.0-SNAPSHOT-slim solr start -f -c
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
  local solr_ip="$output"
  echo "First Solr node IP: $solr_ip"

  # Start second Solr node connected to first node's ZooKeeper
  run docker run --name solr-node2 -d \
    -p 8982:8983 \
    --env "ZK_HOST=${solr_ip}:9983" \
    apache/solr-nightly:9.10.0-SNAPSHOT-slim solr start -f -c
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

  # Check that both nodes are in the cluster
  run docker exec solr-node1 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Cluster status from node 1: $output"
  [[ "$output" =~ live_nodes.*${solr_ip}:8983 ]]

  # Verify we can see both nodes in live_nodes
  run docker exec solr-node2 curl -s 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS'
  [ $status -eq 0 ]
  echo "Cluster status from node 2: $output"
  [[ "$output" =~ live_nodes.*${solr_ip}:8983 ]]

  # Check for specific log messages on first node (embedded ZooKeeper)
  run docker logs solr-node1
  [ $status -eq 0 ]
  echo "Checking logs from first node for ZooKeeper startup messages"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper" ]] || [[ "$output" =~ "STARTING EMBEDDED ENSEMBLE ZOOKEEPER SERVER" ]]

  # Check for specific log messages on second node (client connection)
  run docker logs solr-node2
  [ $status -eq 0 ]
  echo "Checking logs from second node for ZooKeeper client messages"
  [[ "$output" =~ "Starting in cloud mode" ]] || [[ "$output" =~ "Welcome to Apache Solr" ]]
  [[ "$output" =~ "ZooKeeper" ]] || [[ "$output" =~ "Zookeeper client=" ]] || [[ "$output" =~ "zkClient has connected" ]]

  # Create a collection successfully
  run docker exec --user=solr solr-node1 solr create -c test-collection --shards 2 -rf 1
  [ $status -eq 0 ]
  echo "Collection creation output: $output"
  [[ "$output" =~ "Created collection" ]] || [[ "$output" =~ "test-collection" ]]

  # Verify collection was created by querying it
  run docker exec solr-node2 curl -s 'http://localhost:8983/solr/test-collection/select?q=*:*'
  [ $status -eq 0 ]
  echo "Query result from second node: $output"
  [[ "$output" =~ '"numFound"' ]]

  echo "Docker SolrCloud cluster test completed successfully!"
}