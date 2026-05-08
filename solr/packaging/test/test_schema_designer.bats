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

# System-level coverage of the schema-designer HTTP surface only.
# Per-endpoint behavior is tested in TestSchemaDesigner.java.

load bats_helper

DESIGNER_CONFIGSET="bats_books"

setup_file() {
  common_clean_setup
  solr start
  solr assert --started http://localhost:${SOLR_PORT} --timeout 60000
}

teardown_file() {
  common_setup
  solr stop --all
}

setup() {
  common_setup
}

teardown() {
  save_home_on_failure
  curl -s -X DELETE "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}" > /dev/null || true
}

@test "list schema-designer configs returns JSON with configSets key" {
  run curl -s "http://localhost:${SOLR_PORT}/api/schema-designer/configs"
  assert_output --partial '"configSets"'
  refute_output --partial '"status":400'
  refute_output --partial '"status":500'
}

@test "schema-designer end-to-end flow over HTTP" {
  run curl -s -X POST \
    "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}/prep?copyFrom=_default"
  assert_output --partial '"configSet"'
  refute_output --partial '"status":400'
  refute_output --partial '"status":500'

  run curl -s -X POST \
    -H "Content-Type: application/json" \
    --data-binary "@${SOLR_TIP}/example/exampledocs/books.json" \
    "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}/analyze"
  assert_output --partial '"configSet"'
  refute_output --partial '"status":400'
  refute_output --partial '"status":500'

  run curl -s \
    "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}/query?q=*:*"
  assert_output --partial '"numFound"'
  refute_output --partial '"status":400'
  refute_output --partial '"status":500'

  run curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}"
  assert_output "200"
}

@test "download schema-designer configSet as zip" {
  curl -s -X POST \
    "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}/prep?copyFrom=_default" \
    > /dev/null

  local zip_file="${BATS_TEST_TMPDIR}/${DESIGNER_CONFIGSET}.zip"
  local mutable_id="._designer_${DESIGNER_CONFIGSET}"

  local http_code
  http_code=$(curl -s -o "${zip_file}" -w "%{http_code}" \
    "http://localhost:${SOLR_PORT}/api/configsets/${mutable_id}/files?displayName=${DESIGNER_CONFIGSET}")

  [ "${http_code}" = "200" ]
  [ -s "${zip_file}" ]

  # ZIP magic bytes (PK = 0x504B)
  run bash -c "xxd '${zip_file}' | head -1"
  assert_output --partial '504b'
}

@test "download schema-designer configSet has correct Content-Disposition header" {
  curl -s -X POST \
    "http://localhost:${SOLR_PORT}/api/schema-designer/${DESIGNER_CONFIGSET}/prep?copyFrom=_default" \
    > /dev/null

  local mutable_id="._designer_${DESIGNER_CONFIGSET}"
  run curl -s -I \
    "http://localhost:${SOLR_PORT}/api/configsets/${mutable_id}/files?displayName=${DESIGNER_CONFIGSET}"
  assert_output --partial 'Content-Disposition'
  assert_output --partial '.zip'
}
