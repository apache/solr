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

# Solr started in the foreground writes no PID file, which is the normal case in containers.
# Hiding the PID file of a node started in the background reproduces that state.
#
# Note that "stop --all" and a bare "stop" intentionally remain limited to nodes with a PID file
# in SOLR_PID_DIR, so that they never reap Solr instances belonging to another installation.
# The PID file is therefore moved aside rather than deleted, and restored in teardown, so that
# teardown can still shut the node down the ordinary way instead of relying on the code under
# test or on an asynchronous kill.

load bats_helper

setup() {
  common_clean_setup
  hidden_pid_dir="${BATS_TEST_TMPDIR}/hidden-pid"
  mkdir -p "${hidden_pid_dir}"
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  # Put any hidden PID file back so that "stop --all" can reap the node synchronously
  mv "${hidden_pid_dir}"/solr-*.pid "${SOLR_PID_DIR}/" 2>/dev/null || true
  SOLR_STOP_WAIT=30 solr stop --all >/dev/null 2>&1
}

start_solr_without_pid_file() {
  solr start
  solr assert --started "http://localhost:${SOLR_PORT}" --timeout 5000
  mv "${SOLR_PID_DIR}"/solr-${SOLR_PORT}.pid "${hidden_pid_dir}/"
}

@test "status finds a running node that has no PID file" {
  start_solr_without_pid_file

  run solr status
  assert_success
  assert_output --partial "running on port ${SOLR_PORT}"
}

@test "status --short finds a running node that has no PID file" {
  start_solr_without_pid_file

  run solr status --short
  assert_success
  assert_output --partial "http://localhost:${SOLR_PORT}/solr"
}

@test "stop by port works without a PID file" {
  start_solr_without_pid_file

  run solr stop -p ${SOLR_PORT}
  assert_success
  refute_output --partial "No process found"
  solr assert --not-started "http://localhost:${SOLR_PORT}" --timeout 5000
}

@test "stop --all reports when it stopped nothing" {
  run solr stop --all
  assert_success
  assert_output --partial "No Solr nodes found to stop."
}
