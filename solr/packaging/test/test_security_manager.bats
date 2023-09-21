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

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  delete_all_collections
  SOLR_STOP_WAIT=1 solr stop -all >/dev/null 2>&1
}

@test "allowPaths - backup" {
  # Make a test tmp dir, as the security policy includes TMP, so that might already contain the BATS_TEST_TMPDIR
  test_tmp_dir="${BATS_TEST_TMPDIR}/tmp"
  mkdir -p "${test_tmp_dir}"
  test_tmp_dir="$(cd -P "${test_tmp_dir}" && pwd)"

  backup_dir="${BATS_TEST_TMPDIR}/backup-dir"
  mkdir -p "${backup_dir}"
  backup_dir="$(cd -P "${backup_dir}" && pwd)"

  export SOLR_SECURITY_MANAGER_ENABLED=true
  export SOLR_OPTS="-Dsolr.allowPaths=${backup_dir} -Djava.io.tmpdir=${test_tmp_dir}"
  run solr start -c
  run solr create_collection -c COLL_NAME
  run solr api -get "http://localhost:${SOLR_PORT}/solr/admin/collections?action=BACKUP&name=test&collection=COLL_NAME&location=file://${backup_dir}"
  assert_output --partial '"status":0'

  # Solr is not permissioned for this directory, so it should fail
  backup_dir_other="${backup_dir}-other"
  mkdir -p "${backup_dir_other}"
  run solr api -get "http://localhost:${SOLR_PORT}/solr/admin/collections?action=BACKUP&name=test-fail&collection=COLL_NAME&location=file://${backup_dir_other}"
  assert_output --partial 'access denied'
}
