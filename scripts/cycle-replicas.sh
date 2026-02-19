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
# Script to remove all replicas from a node and then add them back
#
# Usage:
#   ./cycle-replicas.sh [SOLR_URL] [COLLECTION] [TARGET_NODE]
#
# Example:
#   ./cycle-replicas.sh http://localhost:8983/solr test solr2:8983_solr
# =============================================================================

set -e

SOLR_URL="${1:-http://localhost:8983/solr}"
COLLECTION="${2:-test}"
TARGET_NODE="${3:-solr2:8983_solr}"

echo "Cycling replicas on $TARGET_NODE for collection $COLLECTION"
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

# Get list of shards that have replicas on target node
shards_on_node=$(echo "$replicas_on_node" | cut -d: -f1 | sort -u)

echo "Found replicas on $TARGET_NODE:"
echo "$replicas_on_node"
echo ""

# =========================================
# PHASE 1: Remove all replicas from node
# =========================================
echo "========================================="
echo "PHASE 1: Removing replicas from $TARGET_NODE"
echo "========================================="

# Get the directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Call the delete-replicas subscript
"$SCRIPT_DIR/delete-replicas.sh" "$SOLR_URL" "$COLLECTION" "$TARGET_NODE" 2

echo ""
echo "All replicas removed from $TARGET_NODE"
echo ""

# =========================================
# PHASE 2: Add replicas back to node
# =========================================
echo "========================================="
echo "PHASE 2: Adding replicas back to $TARGET_NODE (async)"
echo "========================================="

async_ids=()
timestamp=$(date +%s)

for shard in $shards_on_node; do
    echo "Adding TLOG replica for $shard on $TARGET_NODE..."

    async_id="${COLLECTION}_${shard}_add_${timestamp}"

    # Delete any existing async status with this ID (ignore errors)
    curl -s "$SOLR_URL/admin/collections?action=DELETESTATUS&requestid=$async_id" > /dev/null 2>&1 || true

    response=$(curl -s -w "\n%{http_code}" \
        "$SOLR_URL/admin/collections?action=ADDREPLICA&collection=$COLLECTION&shard=$shard&node=$TARGET_NODE&type=TLOG&async=$async_id")

    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" != "200" ]; then
        echo "Error: HTTP $http_code"
        echo "$body"
        exit 1
    fi

    async_ids+=("$async_id")
    echo "  Submitted (async id: $async_id)"
done

echo ""
echo "Waiting for async operations to complete..."

# Wait for all async operations to complete
for async_id in "${async_ids[@]}"; do
    echo "Checking status of $async_id..."

    while true; do
        status_response=$(curl -s "$SOLR_URL/admin/collections?action=REQUESTSTATUS&requestid=$async_id")
        state=$(echo "$status_response" | jq -r '.status.state')

        if [ "$state" == "completed" ]; then
            echo "  $async_id: completed"
            # Clean up the async request
            curl -s "$SOLR_URL/admin/collections?action=DELETESTATUS&requestid=$async_id" > /dev/null
            break
        elif [ "$state" == "failed" ]; then
            echo "  $async_id: FAILED"
            echo "$status_response" | jq '.status'
            exit 1
        else
            echo "  $async_id: $state (waiting...)"
            sleep 2
        fi
    done
done

echo ""
echo "========================================="
echo "Replica cycling complete!"
echo "Collection: $COLLECTION"
echo "Node: $TARGET_NODE"
echo "Shards cycled: $(echo "$shards_on_node" | wc -w)"
echo "========================================="

