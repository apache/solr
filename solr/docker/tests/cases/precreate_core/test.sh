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

echo "Running $container_name"
docker run --name "$container_name" -d -e VERBOSE=yes "$tag" solr-precreate gettingstarted

wait_for_container_and_solr "$container_name"

echo "Loading data"
docker exec --user=solr "$container_name" bin/post -c gettingstarted example/exampledocs/manufacturers.xml
sleep 1
echo "Checking data"
data=$(docker exec --user=solr "$container_name" wget -q -O - 'http://localhost:8983/solr/gettingstarted/select?q=id%3Adell')
if ! grep -E -q 'One Dell Way Round Rock, Texas 78682' <<<"$data"; then
  echo "Test $TEST_NAME $tag failed; data did not load"
  exit 1
fi
container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
