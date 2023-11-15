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

  run solr auth disable
  solr stop -all >/dev/null 2>&1
}

@test "test keystore reload" {
  # Create a keystore
  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  mkdir -p "$ssl_dir"
  (
    cd "$ssl_dir"
    rm -f cert1.keystore.p12 cert1.pem cert2.keystore.p12 cert2.pem
    # cert and keystore 1
    keytool -genkeypair -alias cert1 -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999 -keystore cert1.keystore.p12 -storetype PKCS12 -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country"
    openssl pkcs12 -in cert1.keystore.p12 -out cert1.pem -passin pass:secret -passout pass:secret

    # cert and keystore 2
    keytool -genkeypair -alias cert2 -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999 -keystore cert2.keystore.p12 -storetype PKCS12 -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country"
    openssl pkcs12 -in cert2.keystore.p12 -out cert2.pem -passin pass:secret -passout pass:secret

    cp cert1.keystore.p12 server1.keystore.p12
    cp cert1.keystore.p12 server2.keystore.p12
  )

  # Set ENV_VARs so that Solr uses this keystore
  export SOLR_SSL_ENABLED=true
  export SOLR_SSL_KEY_STORE=$ssl_dir/server1.keystore.p12
  export SOLR_SSL_KEY_STORE_PASSWORD=secret
  export SOLR_SSL_TRUST_STORE=$ssl_dir/server1.keystore.p12
  export SOLR_SSL_TRUST_STORE_PASSWORD=secret
  export SOLR_SSL_NEED_CLIENT_AUTH=true
  export SOLR_SSL_WANT_CLIENT_AUTH=false
  export SOLR_HOST=localhost

  solr start -c -a "-Dsolr.jetty.sslContext.reload.scanInterval=1 -DsocketTimeout=5000"
  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000

  export SOLR_SSL_KEY_STORE=$ssl_dir/server2.keystore.p12
  export SOLR_SSL_TRUST_STORE=$ssl_dir/server2.keystore.p12
  solr start -c -z localhost:${ZK_PORT} -p ${SOLR2_PORT} -a "-Dsolr.jetty.sslContext.reload.scanInterval=1 -DsocketTimeout=5000"
  solr assert --started https://localhost:${SOLR2_PORT}/solr --timeout 5000

  run solr create -c test -s 2
  assert_output --partial "Created collection 'test'"

  run solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  run solr api -get "https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  run ! curl "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  run ! curl "https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*"

  export SOLR_SSL_KEY_STORE=$ssl_dir/cert2.keystore.p12
  export SOLR_SSL_KEY_STORE_PASSWORD=secret
  export SOLR_SSL_TRUST_STORE=$ssl_dir/cert2.keystore.p12
  export SOLR_SSL_TRUST_STORE_PASSWORD=secret

  run ! solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"

  (
    cd "$ssl_dir"
    # Replace server1 keystore with client's
    cp cert2.keystore.p12 server1.keystore.p12
  )
  # Give some time for the server reload
  sleep 6

  # run ! solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=query2"
  run ! solr api -get "https://localhost:${SOLR2_PORT}/solr/test/select?q=query2"

  (
    cd "$ssl_dir"
    # Replace server2 keystore with client's
    cp cert2.keystore.p12 server2.keystore.p12
  )
  # Give some time for the server reload
  sleep 6

  run solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=query3"
  assert_output --partial '"numFound":0'

  run solr api -get "https://localhost:${SOLR2_PORT}/solr/test/select?q=query3"
  assert_output --partial '"numFound":0'

}