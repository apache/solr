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
  command -v openssl >/dev/null 2>&1 || skip "OpenSSL is not available"
  export SOLR_INCLUDE="$BATS_TEST_TMPDIR/global-solr.in.sh"
  export SOLR_STOP_WAIT=15
  touch "$SOLR_INCLUDE"
}

teardown() {
  save_home_on_failure
  solr stop --all >/dev/null 2>&1 || true
}

@test "secure example resets two HTTPS nodes with authentication and ZooKeeper ACLs" {
  mkdir -p "$SOLR_HOME/node1"
  printf 'left over\n' > "$SOLR_HOME/node1/incomplete-file"
  run env SOLR_ULIMIT_CHECKS=true solr start -e secure --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial 'Two-node secure example is ready'
  assert_output --partial "http://localhost:$SOLR_PORT/api/node/health"
  assert_output --partial "http://localhost:$SOLR2_PORT/api/node/health"
  refute_output --partial 'forcefully killing it now'
  refute_output --partial 'set SOLR_ULIMIT_CHECKS to false'
  assert_output --partial 'Check status: SOLR_INCLUDE='

  run ls -a "$SOLR_HOME"
  assert_success
  refute_output --partial '.secure-example'
  run ls -a "$SOLR_HOME/node1"
  assert_success
  refute_output --partial 'incomplete-file'


  run cat "$SOLR_INCLUDE"
  assert_output ''

  run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user admin:SolrRocks "https://localhost:$SOLR_PORT/api/cluster/security/authentication"
  assert_success
  assert_output --partial '"admin"'
  refute_output --partial '"solr"'

  for port in "$SOLR_PORT" "$SOLR2_PORT"; do
    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user search:SolrRocks "https://localhost:$port/solr/techproducts/select?q=*:*&rows=0&shards.info=true"
    assert_success
    assert_output --partial '"numFound":31'
    assert_output --partial "https://127.0.0.1:$SOLR_PORT/"
    assert_output --partial "https://127.0.0.1:$SOLR2_PORT/"
    refute_output --partial 'http://'

    run curl --noproxy '*' --silent --show-error --fail --max-time 5 "https://localhost:$port/api/node/health"
    assert_failure
    assert_output --partial 'certificate'

    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      "https://localhost:$port/api/node/health"
    assert_failure
    assert_output --partial '401'

    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user solr:SolrRocks "https://localhost:$port/api/node/health"
    assert_failure
    assert_output --partial '401'

    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user admin:SolrRocks "https://localhost:$port/solr/techproducts/select?q=*:*"
    assert_failure
    assert_output --partial '403'

    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user admin:SolrRocks -H 'Content-Type: application/json' --data-binary '[{"id":"forbidden-admin"}]' \
      "https://localhost:$port/solr/techproducts/update"
    assert_failure
    assert_output --partial '403'

    run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user search:SolrRocks -H 'Content-Type: application/json' --data-binary '[{"id":"forbidden"}]' \
      "https://localhost:$port/solr/techproducts/update"
    assert_failure
    assert_output --partial '403'

    run curl --noproxy '*' --silent --show-error --fail --max-time 5 "http://localhost:$port/api/node/health"
    assert_failure
  done

  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" solr zk ls / --zk-host "localhost:$ZK_PORT"
  assert_success
  assert_output --partial 'collections'

  run env SOLR_INCLUDE=/dev/null SOLR_ZK_CREDS_AND_ACLS= solr zk ls / --zk-host "localhost:$ZK_PORT"
  assert_failure
  assert_output --partial 'NoAuth'

  local readonly_opts='-DzkCredentialsInjector=org.apache.solr.common.cloud.VMParamsZkCredentialsInjector -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider -DzkDigestUsername=zk-readonly -DzkDigestPassword=SolrRocks'
  run env SOLR_INCLUDE=/dev/null SOLR_ZK_CREDS_AND_ACLS="$readonly_opts" \
    solr zk ls /collections --zk-host "localhost:$ZK_PORT"
  assert_success
  assert_output --partial 'techproducts'
  run env SOLR_INCLUDE=/dev/null SOLR_ZK_CREDS_AND_ACLS="$readonly_opts" \
    solr zk cp zk:/security.json "$BATS_TEST_TMPDIR/security.json" --zk-host "localhost:$ZK_PORT"
  assert_failure
  assert_output --partial 'NoAuth'
  run env SOLR_INCLUDE=/dev/null SOLR_ZK_CREDS_AND_ACLS="$readonly_opts" \
    solr zk mkroot /forbidden --zk-host "localhost:$ZK_PORT"
  assert_failure
  assert_output --partial 'NoAuth'

  run cp "$SOLR_HOME/tls/solr-ssl.keystore.p12" "$BATS_TEST_TMPDIR/original.p12"
  assert_success
  run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user index:SolrRocks -H 'Content-Type: application/json' --data-binary '[{"id":"previous-run"}]' \
    "https://localhost:$SOLR_PORT/solr/techproducts/update?commit=true"
  assert_success

  run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:$SOLR_PORT/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":32'
  printf 'left over\n' > "$SOLR_HOME/leftover-file.txt"
  rm "$SOLR_HOME/tls/cert.pem"
  run solr start -e secure --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"
  assert_success
  assert_output --partial 'Resetting the secure example'
  refute_output --partial 'forcefully killing it now'
  run ls -a "$SOLR_HOME"
  assert_success
  refute_output --partial 'leftover-file.txt'

  run cmp "$SOLR_HOME/tls/solr-ssl.keystore.p12" "$BATS_TEST_TMPDIR/original.p12"
  assert_failure
  run curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:$SOLR2_PORT/solr/techproducts/select?q=*:*&rows=0"
  assert_success
  assert_output --partial '"numFound":31'

  run keytool -genkeypair -alias unrelated -keyalg RSA -keysize 2048 -validity 1 \
    -dname CN=unrelated -storetype PKCS12 -keystore "$SOLR_HOME/tls/untrusted.p12" \
    -storepass SolrRocks -keypass SolrRocks
  assert_success
  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" solr stop -p "$SOLR2_PORT"
  assert_success
  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" \
    SOLR_SSL_CLIENT_TRUST_STORE="$SOLR_HOME/tls/untrusted.p12" \
    SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=SolrRocks SOLR_SSL_CLIENT_TRUST_STORE_TYPE=PKCS12 \
    SOLR_TOOL_OPTS="-Djavax.net.ssl.trustStore=$SOLR_HOME/tls/solr-ssl.truststore.p12" \
    SOLR_LOGS_DIR="$SOLR_HOME/node2/logs" \
    solr start --solr-home "$SOLR_HOME/node2/solr" -p "$SOLR2_PORT" -z "localhost:$ZK_PORT"
  assert_success
  for port in "$SOLR_PORT" "$SOLR2_PORT"; do
    run wait_for 60 1 curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
      --user admin:SolrRocks "https://localhost:$port/api/node/health"
    assert_success
  done
  run wait_for 60 1 curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:$SOLR2_PORT/solr/techproducts/select?q=*:*&rows=0&distrib=false"
  assert_success
  run curl --noproxy '*' --silent --show-error --max-time 30 --write-out '\nHTTP %{http_code}\n' \
    --cacert "$SOLR_HOME/tls/cert.pem" --user search:SolrRocks \
    "https://localhost:$SOLR2_PORT/solr/techproducts/select?q=*:*&rows=0&shards.info=true&shards.tolerant=false"
  assert_success
  assert_output --partial 'HTTP 500'
  assert_output --regexp 'SSLHandshakeException|PKIX|certificate'
  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" solr stop -p "$SOLR2_PORT"
  assert_success
  run env SOLR_INCLUDE="$SOLR_HOME/tls/solr.in.sh" SOLR_LOGS_DIR="$SOLR_HOME/node2/logs" \
    solr start --solr-home "$SOLR_HOME/node2/solr" -p "$SOLR2_PORT" -z "localhost:$ZK_PORT"
  assert_success
  run wait_for 60 1 curl --noproxy '*' --silent --show-error --fail --cacert "$SOLR_HOME/tls/cert.pem" \
    --user search:SolrRocks "https://localhost:$SOLR2_PORT/solr/techproducts/select?q=*:*&rows=0&shards.info=true&shards.tolerant=false"
  assert_success
  assert_output --partial '"numFound":31'
  assert_output --partial "https://127.0.0.1:$SOLR_PORT/"
  assert_output --partial "https://127.0.0.1:$SOLR2_PORT/"
}

@test "secure example rejects unsupported options" {
  run solr start -e secure --no-prompt --solr-home "$SOLR_HOME" --host other-host
  assert_failure
  assert_output --partial 'host'
  run solr start -e secure --no-prompt --solr-home "$SOLR_HOME" --user-managed
  assert_failure
  assert_output --partial 'requires its own embedded ZooKeeper'
}
