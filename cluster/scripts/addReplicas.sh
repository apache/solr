#!/usr/bin/env bash

endpoint=$1
collection=$2
shard=$3
replicaType=$4
replicasToBeAdded=$5
echo "collection: $collection, shard: $shard, replicaType: $replicaType, replicasToBeAdded: $replicasToBeAdded"

date
startTimestamp=$(date +%s)

for (( i=1; i<=$replicasToBeAdded; i++ ))
do
 curl "http://$endpoint:8983/solr/admin/collections?action=ADDREPLICA&collection=$collection&shard=$shard&type=$replicaType"

 echo "$i replicas added"
 currentTimestamp=$(date +%s)
 echo "timeElapsed $(($currentTimestamp-$startTimestamp))"
done

date