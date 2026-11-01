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
  solr stop -p "$SOLR2_PORT" >/dev/null 2>&1 || true
  solr stop --all >/dev/null 2>&1
}

@test "secure example configures HTTPS before users and reuses it on restart" {
  run solr start -e secure-setup --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial 'Verify HTTPS in another terminal:'
  assert_output --partial 'curl --cacert'
  assert_output --partial '/api/node/system'
  assert_output --partial '-Dsolr.environment=prod,label=Secure+Solr+Setup'
  assert_output --partial 'Secure Solr example running at https://'
  assert_output --partial 'Sign in as solr. You will need to accept the self-signed certificate in the browser.'
  refute_output --partial 'Press Enter after curl'
  refute_output --partial 'Restart Solr to enable HTTPS.'
  refute_output --partial 'No cluster property changed.'
  refute_output --partial 'Password for index'
  refute_output --partial 'Administrator username'
  assert_output --partial "HTTP account credentials are saved in $SOLR_HOME/http-credentials.properties.
ZooKeeper account credentials are saved in $SOLR_HOME/zk-credentials.properties."
  refute_output --partial 'Use SOLR_INCLUDE='
  assert_output --partial "Private key: $SOLR_HOME/tls/key.pem"
  assert_output --partial "Keystore password file: $SOLR_HOME/tls/keystore-password.txt"
  assert_output --partial "--zk-credentials 'zk-admin:SolrRocks'"


  run test -f "$SOLR_HOME/logs/solr.log"
  assert_success
  run test -f "${SOLR_HOME}-node2/logs/solr.log"
  assert_success
  run cmp "$SOLR_HOME/zk-credentials.properties" "${SOLR_HOME}-node2/zk-credentials.properties"
  assert_success
  run test ! -d "${SOLR_HOME}-node2/zoo_data"
  assert_success

  run cat "$SOLR_INCLUDE"
  assert_output ''

  run cat "$SOLR_HOME/tls/solr.in.sh"
  assert_output --partial "SOLR_HOST_ADVERTISE='localhost'"

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks "https://localhost:${SOLR_PORT}/api/node/system"
  assert_success
  assert_output --partial '"mode":"solrcloud"'
  assert_output --partial '"environment":"prod"'
  assert_output --partial '"environment_label":"Secure Solr Setup"'

  run solr zk cp zk:/security.json "$BATS_TEST_TMPDIR/generated-security.json" \
    --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-admin:SolrRocks
  assert_success
  run cat "$BATS_TEST_TMPDIR/generated-security.json"
  assert_output --partial '"blockUnknown":false'

  for node_port in "$SOLR_PORT" "$SOLR2_PORT"; do
    for endpoint in node/health metrics; do
      run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail-with-body -H 'Accept:' --cacert "$SOLR_HOME/tls/cert.pem" \
        "https://localhost:${node_port}/api/${endpoint}"
      assert_success
    done
    run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      "https://localhost:${node_port}/solr/techproducts/select?q=*:*&rows=0"
    assert_failure
    assert_output --partial '401'
  done

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":31'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":31'

  for node_port in "$SOLR_PORT" "$SOLR2_PORT"; do
    run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user search:SolrRocks "https://localhost:${node_port}/solr/techproducts/select?q=*:*&rows=0&distrib=false"
    assert_success
    refute_output --partial '"numFound":0'
    refute_output --partial '"numFound":31'
  done

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_failure
  assert_output --partial '403'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '[{"id":"secure-example-product","name":"Secure example product"}]' \
    "https://localhost:${SOLR_PORT}/solr/techproducts/update?commit=true"
  assert_failure
  assert_output --partial '403'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user index:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '[{"id":"secure-example-product","name":"Secure example product"}]' \
    "https://localhost:${SOLR2_PORT}/solr/techproducts/update?commit=true"
  assert_success

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=id:secure-example-product"
  assert_success
  assert_output --partial '"numFound":1'

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '{"set-user":{"reader":"ReaderRocks"}}' \
    "https://localhost:${SOLR_PORT}/api/cluster/security/authentication"
  assert_success

  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user solr:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '{"set-user-role":{"reader":["search"]}}' \
    "https://localhost:${SOLR_PORT}/api/cluster/security/authorization"
  assert_success

  for attempt in {1..10}; do
    run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user reader:ReaderRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
    if [ "$status" -eq 0 ]; then
      break
    fi
    sleep 1
  done
  assert_success
  assert_output --partial '"numFound":32'

  run solr zk ls / --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-admin:SolrRocks
  assert_success
  assert_output --partial 'security.json'

  run env SOLR_PICOCLI=true solr zk ls / --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-admin:SolrRocks
  assert_success
  assert_output --partial 'security.json'

  run solr zk ls /collections --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-readonly:SolrRocks
  assert_success
  assert_output --partial 'techproducts'

  run solr zk ls /security.json --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-readonly:SolrRocks
  assert_failure
  assert_output --partial 'NoAuth'

  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" solr zk cp zk:/security.json "$BATS_TEST_TMPDIR/security.json" \
    --zk-host "localhost:${ZK_PORT}/solr" --zk-credentials zk-readonly:SolrRocks
  assert_failure
  assert_output --partial 'NoAuth'

  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" solr zk ls /collections --solr-url "https://localhost:${SOLR_PORT}" \
    --credentials solr:SolrRocks --zk-credentials zk-readonly:SolrRocks
  assert_success
  assert_output --partial 'techproducts'


  run env SOLR_INCLUDE=/dev/null SOLR_ZK_CREDS_AND_ACLS= solr zk ls / --zk-host "localhost:${ZK_PORT}/solr"
  assert_failure
  assert_output --partial 'NoAuth'

  run solr stop -p "$SOLR2_PORT"
  assert_output --partial 'Sending stop command'
  run solr stop -p "$SOLR_PORT"
  assert_output --partial 'Sending stop command'
  run solr start -e secure-setup --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial "Reusing the example's HTTPS configuration."
  assert_output --partial "Reusing the example's ZooKeeper credentials and ACL configuration."
  assert_output --partial 'Using the existing security configuration.'

  for attempt in {1..10}; do
    run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user search:SolrRocks "https://localhost:${SOLR_PORT}/solr/techproducts/select?q=*:*&rows=0"
    if [ "$status" -eq 0 ]; then
      break
    fi
    sleep 1
  done
  assert_success
  assert_output --partial '"numFound":32'
  run curl --noproxy localhost,127.0.0.1 --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:${SOLR2_PORT}/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":32'
}
