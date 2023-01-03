#!/usr/bin/env bash

endpoint=$1
collection=$2
shard=$3
replica=$4

date
startTimestamp=$(date +%s)

echo "collection: $collection, shard: $shard, replica: $replica"
curl "http://$endpoint:8983/solr/admin/collections?action=DELETEREPLICA&collection=$collection&shard=$shard&replica=$replica"
echo "$replica deleted"

currentTimestamp=$(date +%s)
echo "timeElapsed $(($currentTimestamp-$startTimestamp))"

date