#!/bin/bash
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

set -euo pipefail

TEST_DIR="${TEST_DIR:-$(dirname -- "${BASH_SOURCE[0]}")}"
source "${TEST_DIR}/../../shared.sh"

container_cleanup "${container_name}-solr"

echo "Running $container_name"
docker run --name "${container_name}-solr" -d "$tag" "solr-demo"

wait_for_container_and_solr "${container_name}-solr"

solr_ip=$(docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "${container_name}-solr")

docker run --name "$container_name" --add-host "solr-host:${solr_ip}" -d \
  --env "SOLR_URL=http://solr-host:8983/solr" \
  --env "SCRAPE_INTERVAL=1" \
  --env "CLUSTER_ID=myCluster" \
  "$tag" "solr-exporter"

wait_for_container_and_solr_exporter "${container_name}"

echo "Submitting Solr query"
docker exec --user=solr "${container_name}-solr" wget -q -O - 'http://localhost:8983/solr/demo/select?q=id%3Adell' > /dev/null

echo "Checking prometheus data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8989/metrics')

if ! grep -E -q 'solr_metrics_core_query_requests_total{category="QUERY",searchHandler="/select",internal="false",core="demo",base_url="http://solr-host:8983/solr",cluster_id="myCluster",} [0-9]+.0' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; did not find correct data"
  echo "$data"
  exit 1
fi

container_cleanup "${container_name}-solr"
container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
