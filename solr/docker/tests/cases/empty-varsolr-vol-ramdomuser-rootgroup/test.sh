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

myvarsolr="myvarsolr-${container_name}"

docker volume rm "$myvarsolr" >/dev/null 2>&1 || true
docker volume create "$myvarsolr"

# when we mount onto /var/solr, that will be owned by "solr"

echo "Running $container_name"
docker run \
  --user 7777:0 \
  -v "$myvarsolr:/var/solr" \
  --name "$container_name" \
  -d "$tag" solr-precreate getting-started

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c getting-started example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/getting-started/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load"
  exit 1
fi

docker exec --user=7777 "$container_name" ls -l /var/solr/data

container_cleanup "$container_name"

docker volume rm "$myvarsolr"

echo "Test $TEST_NAME $tag succeeded"
