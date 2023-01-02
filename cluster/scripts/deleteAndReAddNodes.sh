#!/usr/bin/env bash

endpoint=$1
collection=$2
shard=$3
replicaType=$4
nodesFileName=$5
batchSize=$6
sleepBetweenDeleteAndReAdd=$7
sleepBetweenBatches=$8

IFS=$'\n' read -d '' -r -a nodeIPs < ${nodesFileName}

date
startTimestamp=$(date +%s)

len=${#nodeIPs[@]}

if (( $batchSize > $len )); then
    batchSize=${len}
fi

batchNumber=1
count=0

echo "numOfNodes:$len, batchSize:$batchSize"

while :
do
  batchStart=$(( ( $batchNumber - 1 ) * $batchSize ))
  batchEnd=$(( $batchNumber * $batchSize ))
  
  batch=("${nodeIPs[@]:$batchStart:$batchSize}")

  for nodeIP in "${batch[@]}"
  do
    echo "deleting node: $nodeIP"
    curl "http://$endpoint:8983/solr/admin/collections?action=DELETENODE&node=$nodeIP:8983_solr"
    echo "$nodeIP deleted"
  done

  echo "sleeping for $sleepBetweenDeleteAndReAdd seconds"
  sleep ${sleepBetweenDeleteAndReAdd}

  for nodeIP in "${batch[@]}"
  do
    echo "ReAdding node: $nodeIP"
    curl "http://$endpoint:8983/solr/admin/collections?action=ADDREPLICA&collection=$collection&shard=$shard&type=$replicaType&node=$nodeIP:8983_solr"
    echo "$nodeIP added"
  done

  currentTimestamp=$(date +%s)
  echo "timeElapsed: $(($currentTimestamp-$startTimestamp)) secs, batches completed till now: $batchNumber"

  if (( $batchEnd >= $len )); then
    break
  fi

  echo "sleeping for $sleepBetweenBatches seconds"
  sleep ${sleepBetweenBatches}

  batchNumber=$(($batchNumber+1))
done
