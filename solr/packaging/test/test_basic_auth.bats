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

# Remaining commands that should support basic auth:
# healthcheck, delete, 
# zk, 
# auth, 
# assert, config, export, api, package, post


@test "post help flag prints help" {
  # Create a keystore
  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  mkdir -p "$ssl_dir"
  (
    cd "$ssl_dir"
    rm -f solr-ssl.keystore.p12 solr-ssl.pem
    keytool -genkeypair -alias solr-ssl -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999 -keystore solr-ssl.keystore.p12 -storetype PKCS12 -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country"
    openssl pkcs12 -in solr-ssl.keystore.p12 -out solr-ssl.pem -passin pass:secret -passout pass:secret
  )

  # Set ENV_VARs so that Solr uses this keystore
  export SOLR_SSL_ENABLED=true
  export SOLR_SSL_KEY_STORE=$ssl_dir/solr-ssl.keystore.p12
  export SOLR_SSL_KEY_STORE_PASSWORD=secret
  export SOLR_SSL_TRUST_STORE=$ssl_dir/solr-ssl.keystore.p12
  export SOLR_SSL_TRUST_STORE_PASSWORD=secret
  export SOLR_SSL_NEED_CLIENT_AUTH=false
  export SOLR_SSL_WANT_CLIENT_AUTH=false
  export SOLR_SSL_CHECK_PEER_NAME=true
  export SOLR_HOST=localhost

  solr start -c
  solr auth enable -type basicAuth -credentials name:password
  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000
  
  # Test creating a collection
  
  run solr create -u name:password -c COLL_NAME 
  assert_output --partial "Created collection 'COLL_NAME'"
  
  run solr postlogs -u name:password -url https://localhost:${SOLR_PORT}/solr/COLL_NAME -rootdir ${SOLR_LOGS_DIR}/solr.log
  assert_output --partial 'Sending last batch'
}
