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

initdb="$BUILD_DIR/initdb-$container_name"
prepare_dir_to_mount 8983 "$initdb"

cat > "$initdb/create-was-here.sh" <<EOM
touch /var/solr/initdb-was-here
export SOLR_HEAP="745m"
EOM
cat > "$initdb/ignore-me" <<EOM
touch /var/solr/should-not-be
EOM

echo "Running $container_name"
docker run --name "$container_name" -d -e VERBOSE=yes -v "$initdb:/docker-entrypoint-initdb.d" "$tag"

wait_for_server_started "$container_name"

echo "Checking initdb"
data=$(docker exec --user=solr "$container_name" ls /var/solr/initdb-was-here)
if [[ "$data" != /var/solr/initdb-was-here ]]; then
  echo "Test $TEST_DIR $tag failed; script did not run"
  exit 1
fi
data=$(docker exec --user=solr "$container_name" ls /var/solr/should-not-be || true)
if [[ -n "$data" ]]; then
  echo "Test $TEST_DIR $tag failed; should-not-be was"
  exit 1
fi
data=$(docker exec --user=solr "$container_name" ps -ef)
if ! grep -q 'Xms745m' <<< "$data"; then
  echo "Test $TEST_DIR $tag failed; environment variable in initdb script not exported correctly"
  exit 1
fi
echo "Checking docker logs"
log="${BUILD_DIR}/docker.log-$container_name"
if ! docker logs "$container_name" >"$log" 2>&1; then
  echo "Could not get logs for $container_name"
  exit
fi
if ! grep -q 'ignoring /docker-entrypoint-initdb.d/ignore-me' "$log"; then
  echo "missing ignoring message"
  cat "$log"
  exit 1
fi
rm "$log"

rm -fr "$initdb"
container_cleanup "$container_name"

echo "Test $TEST_NAME $tag succeeded"
