#!/usr/bin/env bash

endpoint=$1
nodeIP=$2

date

echo "deleting node: $nodeIP"
curl "http://$endpoint:8983/solr/admin/collections?action=DELETENODE&node=$nodeIP:8983_solr"
echo "$nodeIP deleted"

date