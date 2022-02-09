#!/usr/bin/env bats

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

common_setup() {
    TEST_BREW_PREFIX="$(brew --prefix)"
    load "${TEST_BREW_PREFIX}/lib/bats-support/load.bash"
    load "${TEST_BREW_PREFIX}/lib/bats-assert/load.bash"

    # use $BATS_TEST_FILENAME instead of ${BASH_SOURCE[0]} or $0,
    # as those will point to the bats executable's location or the preprocessed file respectively
    DIR="$( cd "$( dirname "$BATS_TEST_FILENAME" )" >/dev/null 2>&1 && pwd )"
    # add solr/bin to the path for our tests
    SOLR_HOME="$DIR/../build/solr-10.0.0-SNAPSHOT" # currently hard coded, should fix
    PATH="$SOLR_HOME/bin:$PATH"
}

delete_all_collections() {
  local collection_list="$(solr zk ls /collections -z localhost:9983)"
  for collection in $collection_list;
  do
    if [[ -n $collection ]]; then
      solr delete -c $collection >/dev/null 2>&1
    fi
  done
}

config_exists() {
  local config_name=$1
  local config_list=$(solr zk ls /configs -z localhost:9983)

  for config in $config_list; do
    if [[ $(echo $config | tr -d " ") == $config_name ]]; then
      return 0
    fi
  done

  return 1
}

collection_exists() {
  local coll_name=$1
  local coll_list=$(solr zk ls /collections -z localhost:9983)

  for coll in $coll_list; do
    if [[ $(echo $coll | tr -d " ") == $coll_name ]]; then
      return 0
    fi
  done

  return 1
}
