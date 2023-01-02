#!/usr/bin/env bash

endpoint=$1
nodesFileName=$2

IFS=$'\n' read -d '' -r -a nodeIPs < ${nodesFileName}

date
startTimestamp=$(date +%s)

count=0
for nodeIP in "${nodeIPs[@]}"
do
  echo "deleting $nodeIP"
  curl "http://$endpoint:8983/solr/admin/collections?action=DELETENODE&node=$nodeIP:8983_solr"
  echo "$nodeIP deleted"

  count=$(($count+1))
  currentTimestamp=$(date +%s)
  echo "timeElapsed: $(($currentTimestamp-$startTimestamp)) secs, nodes deleted till now: $count"
done
