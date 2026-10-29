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
  export SOLR_INCLUDE="${BATS_TEST_TMPDIR}/global-solr.in.sh"
  touch "$SOLR_INCLUDE"
}

teardown() {
  save_home_on_failure
  solr stop --all >/dev/null 2>&1
}

@test "secure example configures HTTPS before users and reuses it on restart" {
  run solr start -e secure_solr --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial 'Verify HTTPS in another terminal:'
  assert_output --partial 'curl --cacert'
  assert_output --partial '/api/node/system'
  assert_output --partial 'Secure Solr example running at https://'
  assert_output --partial 'Sign in as solr. You will need to accept the self-signed certificate in the browser.'
  refute_output --partial 'Press Enter after curl'

  run cat "$SOLR_INCLUDE"
  assert_output ''

  run cat "$SOLR_HOME/tls/solr.in.sh"
  assert_output --partial "SOLR_HOST_ADVERTISE='localhost'"

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks "https://localhost:${SOLR_PORT}/api/node/system"
  assert_success
  assert_output --partial '"mode":"solrcloud"'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":31'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_failure
  assert_output --partial '403'

  run solr stop -p "$SOLR_PORT"
  assert_output --partial 'Sending stop command'
  run solr start -e secure_solr --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial "Reusing the example's HTTPS configuration."
  assert_output --partial 'Using the existing security configuration.'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":31'
}
