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

echo "Running base solr node w/embeddedZk - $container_name"
docker run --name "${container_name}" -d "$tag" solr-fg -c

wait_for_container_and_solr "${container_name}"

solr_ip=$(docker inspect --format="{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "${container_name}")


container_cleanup "${container_name}-2"

echo "Running additional solr node - $container_name-2"
docker run --name "$container_name-2" -d \
  --env "ZK_HOST=${solr_ip}:9983" \
  "$tag" solr-fg -c

wait_for_container_and_solr "${container_name}-2"

echo "Check live nodes"
data=$(docker exec --user=solr "${container_name}-2" wget -q -O - 'http://localhost:8983/solr/admin/collections?action=CLUSTERSTATUS')

if ! grep -q "${solr_ip}:8983" <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; could not find first solr node in cluster state of second node"
  echo "$data"
  exit 1
fi

echo "Creating distributed collection"
data=$(docker exec --user=solr "$container_name" solr create -c test -rf 1 -s 2)

if ! grep -q "Created collection 'test'" <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; could not create distributed collection"
  echo "$data"
  exit 1
fi

echo "Submitting Solr query"
data=$(docker exec --user=solr "${container_name}-2" wget -q -O - 'http://localhost:8983/solr/test/select?q=*:*')

if ! grep -q '"numFound":0' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; could not query distributed collection"
  echo "$data"
  exit 1
fi


container_cleanup "${container_name}-2"
container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
