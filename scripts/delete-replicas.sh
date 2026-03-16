#!/bin/bash
#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one or more
#  * contributor license agreements.  See the NOTICE file distributed with
#  * this work for additional information regarding copyright ownership.
#  * The ASF licenses this file to You under the Apache License, Version 2.0
#  * (the "License"); you may not use this file except in compliance with
#  * the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

# =============================================================================
# Script to remove all replicas from a node
#
# Usage:
#   ./delete-replicas.sh [SOLR_URL] [COLLECTION] [TARGET_NODE] [TIMEOUT]
#
# Example:
#   ./delete-replicas.sh http://localhost:8983/solr test solr2:8983_solr
#   ./delete-replicas.sh http://localhost:8983/solr test solr2:8983_solr 10
# =============================================================================

set -e

SOLR_URL="${1:-http://localhost:8983/solr}"
COLLECTION="${2:-test}"
TARGET_NODE="${3:-solr2:8983_solr}"
TIMEOUT="${4:-10}"

echo "Deleting replicas from $TARGET_NODE for collection $COLLECTION"
echo "Timeout: ${TIMEOUT}s"
echo ""

# Get cluster status
cluster_status=$(curl -s "$SOLR_URL/admin/collections?action=CLUSTERSTATUS")

# Find all replicas on the target node
# Format: shard_name:replica_name
replicas_on_node=$(echo "$cluster_status" | jq -r "
    .cluster.collections[\"$COLLECTION\"].shards | to_entries[] |
    .key as \$shard |
    .value.replicas | to_entries[] |
    select(.value.node_name == \"$TARGET_NODE\") |
    \"\(\$shard):\(.key)\"
")

if [ -z "$replicas_on_node" ]; then
    echo "No replicas found on $TARGET_NODE for collection $COLLECTION"
    exit 0
fi

echo "Found replicas on $TARGET_NODE:"
echo "$replicas_on_node"
echo ""

echo "========================================="
echo "Removing replicas from $TARGET_NODE"
echo "========================================="

deleted_count=0
skipped_count=0

for replica_info in $replicas_on_node; do
    shard=$(echo "$replica_info" | cut -d: -f1)
    replica=$(echo "$replica_info" | cut -d: -f2)

    echo "Removing $replica from $shard..."

    response=$(curl -s -w "\n%{http_code}" --max-time "$TIMEOUT" \
        "$SOLR_URL/admin/collections?action=DELETEREPLICA&collection=$COLLECTION&shard=$shard&replica=$replica" 2>&1) || true

    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)

    # Check for timeout or empty response
    if [[ -z "$http_code" ]] || [[ ! "$http_code" =~ ^[0-9]+$ ]]; then
        echo "  Warning: Request timed out for $replica, continuing..."
        skipped_count=$((skipped_count + 1))
        continue
    fi

    if [ "$http_code" != "200" ]; then
        echo "  Warning: HTTP $http_code for $replica"
        echo "  $body"
        echo "  Continuing with next replica..."
        skipped_count=$((skipped_count + 1))
        continue
    fi

    echo "  Done"
    deleted_count=$((deleted_count + 1))
done

echo ""
echo "========================================="
echo "Replica deletion complete!"
echo "Collection: $COLLECTION"
echo "Node: $TARGET_NODE"
echo "Deleted: $deleted_count"
echo "Skipped: $skipped_count"
echo "========================================="

