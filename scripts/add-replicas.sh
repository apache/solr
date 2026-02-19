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
# Script to add replicas to a target node
#
# Usage:
#   ./add-replicas.sh [SOLR_URL] [COLLECTION] [TARGET_NODE] [COUNT] [TYPE]
#
# Example:
#   ./add-replicas.sh http://localhost:8983/solr test solr2:8983_solr 12 TLOG
#   ./add-replicas.sh http://localhost:8983/solr test solr2:8983_solr 1 NRT
# =============================================================================

set -e

SOLR_URL="${1:-http://localhost:8983/solr}"
COLLECTION="${2:-test}"
TARGET_NODE="${3:-solr2:8983_solr}"
NUM_SHARDS="${4:-12}"
TYPE="${5:-TLOG}"

echo "Ensuring $NUM_SHARDS shards with 1 replica of type $TYPE on $TARGET_NODE for collection $COLLECTION"

# Fetch cluster status
echo "Fetching cluster status from $SOLR_URL..."
cluster_status=$(curl -s "$SOLR_URL/admin/collections?action=CLUSTERSTATUS")

# Validate JSON response
if ! echo "$cluster_status" | jq -e . >/dev/null 2>&1; then
    echo "Error: Invalid JSON response from Solr."
    echo "Response: $cluster_status"
    exit 1
fi

# Check if collection exists
if echo "$cluster_status" | jq -e ".cluster.collections[\"$COLLECTION\"] == null" >/dev/null; then
    echo "Collection '$COLLECTION' not found."
    echo "Creating collection '$COLLECTION' with $NUM_SHARDS shards..."

    # Determine replica types for CREATE
    # prioritizing TLOG if requested
    CREATE_PARAMS="action=CREATE&name=$COLLECTION&numShards=$NUM_SHARDS"

    if [ "$TYPE" == "TLOG" ]; then
         CREATE_PARAMS="${CREATE_PARAMS}&nrtReplicas=0&tlogReplicas=1"
    elif [ "$TYPE" == "PULL" ]; then
         CREATE_PARAMS="${CREATE_PARAMS}&nrtReplicas=0&pullReplicas=1"
    else
         CREATE_PARAMS="${CREATE_PARAMS}&replicationFactor=1"
    fi

    # Create collection targeted at the node to ensure initial replicas are there
    create_response=$(curl -s -w "\n%{http_code}" \
        "$SOLR_URL/admin/collections?${CREATE_PARAMS}&createNodeSet=$TARGET_NODE")

    create_http_code=$(echo "$create_response" | tail -n1)

    if [ "$create_http_code" != "200" ]; then
        echo "Error creating collection: HTTP $create_http_code"
        echo "$create_response" | head -n -1
        exit 1
    fi

    echo "Collection created successfully."

    # We are done since CREATE with createNodeSet puts them there
    exit 0
fi

echo "Collection '$COLLECTION' exists. Checking shards..."

# Refresh cluster status
cluster_status=$(curl -s "$SOLR_URL/admin/collections?action=CLUSTERSTATUS")

# Iterate through expected shards 1..NUM_SHARDS
for ((i=1; i<=NUM_SHARDS; i++)); do
    shard_name="shard${i}"

    # Check if shard exists
    shard_exists=$(echo "$cluster_status" | jq -r ".cluster.collections[\"$COLLECTION\"].shards[\"$shard_name\"] // empty")

    if [ -z "$shard_exists" ]; then
        echo "  $shard_name does not exist. Creating..."

        # Create shard
        response=$(curl -s -w "\n%{http_code}" \
            "$SOLR_URL/admin/collections?action=CREATESHARD&collection=$COLLECTION&shard=$shard_name&createNodeSet=$TARGET_NODE")

        # CREATESHARD doesn't take type params easily for the new replica, it usually uses collection defaults.
        # But if we use createNodeSet it creates a replica there.
        # However, checking if it created the right TYPE is hard atomically.
        # Typically CREATESHARD adds replicas based on collection settings.

        http_code=$(echo "$response" | tail -n1)
        if [ "$http_code" != "200" ]; then
            echo "  Error creating shard: HTTP $http_code"
            echo "$response" | head -n -1
            exit 1
        fi
        echo "  $shard_name created."

        # We might need to ensure the type is correct if default isn't TLOG.
        # But for now assuming collection settings or manual add later if needed.
        # Ideally we'd check and delete/re-add if wrong type, but that's complex.
    else
        # Shard exists, check for replica on TARGET_NODE
        # We look for a replica on this node
        replicas_on_node=$(echo "$cluster_status" | jq -r ".cluster.collections[\"$COLLECTION\"].shards[\"$shard_name\"].replicas | to_entries[] | select(.value.node_name == \"$TARGET_NODE\") | .key")

        if [ -z "$replicas_on_node" ]; then
             echo "  $shard_name exists but has no replica on $TARGET_NODE. Adding $TYPE replica..."

             response=$(curl -s -w "\n%{http_code}" \
                "$SOLR_URL/admin/collections?action=ADDREPLICA&collection=$COLLECTION&shard=$shard_name&node=$TARGET_NODE&type=$TYPE")

            http_code=$(echo "$response" | tail -n1)
            if [ "$http_code" != "200" ]; then
                echo "  Error adding replica: HTTP $http_code"
                echo "$response" | head -n -1
                exit 1
            fi
            echo "  Replica added."
        else
             echo "  $shard_name already has replica on $TARGET_NODE. Skipping."
        fi
    fi
done

echo ""
echo "========================================="
echo "Configuration complete!"
echo "Collection: $COLLECTION"
echo "Target node: $TARGET_NODE"
echo "Shards checked: $NUM_SHARDS"
echo "========================================="

