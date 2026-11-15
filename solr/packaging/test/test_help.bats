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

load bats_helper

setup() {
  common_clean_setup
}

@test "solr help flag prints help" {
  for flag in --help -h; do
    run -0 solr "$flag"
    assert_output_for_cli 'Usage: solr COMMAND OPTIONS' 'usage: bin/solr'
    refute_output --partial 'ERROR'
  done
}
@test "solr with no flags prints help" {
  run -2 solr
  assert_output_for_cli 'Usage: solr COMMAND OPTIONS' 'usage: bin/solr'
  refute_output --partial 'ERROR'
}

check_parent_help() {
  local parent=$1 commons_marker=$2 picocli_marker=$3
  run -2 solr "$parent"
  assert_output_for_cli "$commons_marker" "$picocli_marker"
  refute_output --partial 'ERROR'
  for flag in --help -h; do
    run -0 solr "$parent" "$flag"
    assert_output_for_cli "$commons_marker" "$picocli_marker"
    refute_output --partial 'ERROR'
  done
}

@test "zk requires a subcommand" {
  check_parent_help zk 'bin/solr zk ls' 'usage: bin/solr zk'
}

@test "snapshot requires a subcommand" {
  check_parent_help snapshot 'snapshot-create' 'usage: bin/solr snapshot'
}

@test "package requires a subcommand" {
  check_parent_help package 'Package Manager' 'usage: bin/solr package'
}

@test "help propagates a JVM startup failure" {
  run -1 env SOLR_TOOL_OPTS=-XX:SolrInvalidOptionForTest solr --help
  assert_output --partial 'Unrecognized VM option'
}

@test "start help flag prints help" {
  run solr start --help
  assert_output_for_cli 'Usage: solr start' 'usage: bin/solr start'
  refute_output --partial 'ERROR'
}

@test "start h flag prints help" {
  run solr start -h
  assert_output_for_cli 'Usage: solr start' 'usage: bin/solr start'
  refute_output --partial 'ERROR: Hostname is required when using the -h option!'
}



@test "stop help flag prints help" {
  run solr stop --help
  assert_output_for_cli 'Usage: solr stop' 'usage: bin/solr stop'
  refute_output --partial 'ERROR'
}

@test "restart help flag prints help" {
  run solr restart --help
  assert_output_for_cli 'Usage: solr restart' 'usage: bin/solr restart'
  refute_output --partial 'ERROR'
}

@test "status help flag prints help" {
  run solr status --help
  assert_output --partial 'usage: bin/solr status'
  refute_output --partial 'ERROR'
}

@test "healthcheck help flag prints help" {
  run solr healthcheck --help
  assert_output --partial 'usage: bin/solr healthcheck'
  refute_output --partial 'ERROR'
}

@test "create help flag prints help" {
  run solr create --help
  assert_output --partial 'usage: bin/solr create'
  refute_output --partial 'ERROR'
}

@test "delete help flag prints help" {
  run solr delete -h
  assert_output --partial 'usage: bin/solr delete'
  refute_output --partial 'ERROR'
}

@test "zk help flag prints help" {
  run solr zk --help
  assert_output --partial 'usage:'
  assert_output_for_cli 'bin/solr zk ls' 'usage: bin/solr zk'
  refute_output --partial 'ERROR'
}

@test "auth help flag prints help" {
  run solr auth --help
  assert_output_for_cli 'bin/solr auth enable' 'usage: bin/solr auth'
  refute_output --partial 'ERROR'
}

@test "assert help flag prints help" {
  run solr assert --help
  assert_output --partial 'usage: bin/solr assert'
  refute_output --partial 'ERROR'
}

@test "post help flag prints help" {
  run solr post --help
  assert_output --partial 'usage: bin/solr post'
  refute_output --partial 'ERROR'
}
