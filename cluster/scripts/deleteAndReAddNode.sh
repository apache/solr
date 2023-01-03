#!/usr/bin/env bash

endpoint=$1
nodeIP=$2
collection=$3
shard=$4
replicaType=$5

date

echo "deleting node: $nodeIP"
curl "http://$endpoint:8983/solr/admin/collections?action=DELETENODE&node=$nodeIP:8983_solr"
echo "$nodeIP deleted"

sleep 3

echo "ReAdding node: $nodeIP"
curl "http://$endpoint:8983/solr/admin/collections?action=ADDREPLICA&collection=$collection&shard=$shard&type=$replicaType&node=$nodeIP:8983_solr"
echo "$nodeIP added"

date