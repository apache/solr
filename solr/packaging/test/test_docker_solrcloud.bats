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

# Environment variables for Docker images
SOLR_BEGIN_IMAGE="${SOLR_BEGIN_IMAGE:-apache/solr-nightly:9.10.0-SNAPSHOT-slim}"
SOLR_END_IMAGE="${SOLR_END_IMAGE:-apache/solr-nightly:10.0.0-SNAPSHOT-slim}"

setup() {
  common_clean_setup

  # Pre-checks
  docker version || skip "Docker is not available"
  docker pull "$SOLR_BEGIN_IMAGE" || skip "Docker image $SOLR_BEGIN_IMAGE is not available"
  docker pull "$SOLR_END_IMAGE" || skip "Docker image $SOLR_END_IMAGE is not available"

  # Record test start time for scoping logs on failure
  TEST_STARTED_AT_ISO=$(date -Iseconds)
  export TEST_STARTED_AT_ISO

  # Persist artifacts under Gradle’s test-output (fallback to Bats temp dir)
  ARTIFACT_DIR="${TEST_OUTPUT_DIR:-$BATS_TEST_TMPDIR}/docker"
  mkdir -p "$ARTIFACT_DIR"
  export ARTIFACT_DIR
}

teardown() {
  failed=$([[ -z "${BATS_TEST_COMPLETED:-}" ]] && [[ -z "${BATS_TEST_SKIPPED:-}" ]] && echo 1 || echo 0)
  if [[ "$failed" -eq 1 ]]; then
    echo "# Test failed - capturing Docker diagnostics" >&3
    echo "# === docker ps (summary) ===" >&3
    docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}' >&3 2>&3 || true
  fi

  for container in solr-node1 solr-node2 solr-node3; do
    if docker ps -a --format '{{.Names}}' | grep -q "^${container}$" 2>/dev/null; then
      if [[ "$failed" -eq 1 ]]; then
        echo "# === Docker logs for $container ===" >&3
        docker logs --timestamps --since "$TEST_STARTED_AT_ISO" "$container" >&3 2>&3 || echo "# Failed to get logs for $container" >&3
        echo "# === Docker inspect for $container ===" >&3
        docker inspect "$container" | jq '.[] | {Name: .Name, State: .State, Ports: .NetworkSettings.Ports}' >&3 2>&3 || true
      fi
      # Persist artifacts
      docker logs --timestamps "$container" >"$ARTIFACT_DIR/${container}.log" 2>&1 || true
      docker inspect "$container" >"$ARTIFACT_DIR/${container}.inspect.json" 2>&1 || true
      docker exec "$container" ps aux >"$ARTIFACT_DIR/${container}.ps.txt" 2>&1 || true
    fi
  done

  echo "# Docker artifacts saved to: $ARTIFACT_DIR" >&3

  docker stop solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker rm solr-node1 solr-node2 solr-node3 2>/dev/null || true
  docker volume rm solr-data1 solr-data2 solr-data3 2>/dev/null || true
  docker network rm solrcloud-test 2>/dev/null || true
}

@test "Docker SolrCloud rolling upgrade" {
  # Networking & volumes
  docker network create solrcloud-test
  docker volume create solr-data1
  docker volume create solr-data2
  docker volume create solr-data3

  echo "Starting solr-node1 with embedded ZooKeeper"
  docker run --name solr-node1 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data1:/var/solr \
    "$SOLR_BEGIN_IMAGE" solr start -f -c -m 200m --host solr-node1 -p 8983
  docker exec solr-node1 solr assert --started http://solr-node1:8983 --timeout 10000

  # start next 2 in parallel

  echo "Starting solr-node2 connected to first node's ZooKeeper"
  docker run --name solr-node2 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data2:/var/solr \
    "$SOLR_BEGIN_IMAGE" solr start -f -c -m 200m --host solr-node2 -p 8984 -z solr-node1:9983

  echo "Started solr-node3 connected to first node's ZooKeeper"
  docker run --name solr-node3 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data3:/var/solr \
    "$SOLR_BEGIN_IMAGE" solr start -f -c -m 200m --host solr-node3 -p 8985 -z solr-node1:9983

  docker exec solr-node2 solr assert --started http://solr-node2:8984 --timeout 30000
  docker exec solr-node3 solr assert --started http://solr-node3:8985 --timeout 30000

  echo "Creating a Collection"
  docker exec --user=solr solr-node1 solr create -c test-collection --shards 3

  echo "Checking collection health"
  wait_for 30 1 docker exec solr-node1 solr healthcheck -c test-collection

  # Begin rolling upgrade - upgrade node 3 first (reverse order: 3, 2, 1)
  echo "Starting rolling upgrade - upgrading node 3"
  docker stop solr-node3
  docker rm solr-node3
  docker run --name solr-node3 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data3:/var/solr \
    "$SOLR_END_IMAGE" solr start -f -m 200m --host solr-node3 -p 8985 -z solr-node1:9983
  docker exec solr-node3 solr assert --started http://solr-node3:8985 --timeout 30000

  # Upgrade node 2 second
  echo "Upgrading node 2"
  docker stop solr-node2
  docker rm solr-node2
  docker run --name solr-node2 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data2:/var/solr \
    "$SOLR_END_IMAGE" solr start -f -m 200m --host solr-node2 -p 8984 -z solr-node1:9983
  docker exec solr-node2 solr assert --started http://solr-node2:8984 --timeout 30000

  echo "Upgrading node 1 (ZK node)"
  docker stop solr-node1
  docker rm solr-node1
  docker run --name solr-node1 -d \
    --network solrcloud-test \
    --memory=400m \
    -v solr-data1:/var/solr \
    "$SOLR_END_IMAGE" solr start -f -m 200m --host solr-node1 -p 8983
  docker exec solr-node1 solr assert --started http://solr-node1:8983 --timeout 30000

  # Final collection health check
  wait_for 30 1 docker exec solr-node1 solr healthcheck -c test-collection

  echo "checking cluster has exactly 3 live nodes"
  local cluster_status=$(docker exec solr-node1 curl -s "http://solr-node1:8983/solr/admin/collections?action=CLUSTERSTATUS")
  local live_nodes_count=$(echo "$cluster_status" | jq -r '.cluster.live_nodes | length')

  if [ "$live_nodes_count" != "3" ]; then
    echo "Expected 3 live nodes, but found $live_nodes_count"
    echo "Cluster status: $cluster_status"
    return 1
  fi

}
