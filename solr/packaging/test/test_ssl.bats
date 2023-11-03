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

@test "start solr with ssl" {
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
  export SOLR_HOST=localhost

  solr start -c
  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000

  run solr create -c test -s 2
  assert_output --partial "Created collection 'test'"

  run solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  run curl --cacert "$ssl_dir/solr-ssl.pem" "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  run ! curl "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
}

@test "use different hostname when not checking peer-name" {
  # Create a keystore
  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  mkdir -p "$ssl_dir"
  (
    cd "$ssl_dir"
    rm -f solr-ssl.keystore.p12 solr-ssl.pem
    # Using a CN that is not localhost, as we will not be checking peer-name
    keytool -genkeypair -alias solr-ssl -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999 -keystore solr-ssl.keystore.p12 -storetype PKCS12 -ext "SAN=DNS:test.solr.apache.org,IP:127.0.0.1" -dname "CN=test.solr.apache.org, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country"
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
  export SOLR_SSL_CHECK_PEER_NAME=false
  export SOLR_HOST=127.0.0.1

  solr start -c
  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000

  run solr create -c test -s 2
  assert_output --partial "Created collection 'test'"

  run solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  # Just test that curl can connect via insecure or via a custom host header
  run curl --http2 --cacert "$ssl_dir/solr-ssl.pem" "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial 'no alternative certificate subject name matches target host name'

  # Just test that curl can connect via insecure or via a custom host header
  run curl --http2 --cacert "$ssl_dir/solr-ssl.pem" -k "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  run curl --http2 --cacert "$ssl_dir/solr-ssl.pem" --resolve "test.solr.apache.org:${SOLR_PORT}:127.0.0.1" "https://test.solr.apache.org:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  # This is a client setting, so we don't need to restart Solr to make sure that it fails
  export SOLR_SSL_CHECK_PEER_NAME=true

  # This should fail the peername check
  run ! solr api -verbose -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --regexp '(No subject alternative DNS name matching localhost found|Server refused connection)'

  # Restart the server enabling the SNI hostcheck
  export SOLR_SSL_CHECK_PEER_NAME=false
  export SOLR_OPTS="${SOLR_OPTS} -Dsolr.jetty.ssl.sniHostCheck=true"
  solr restart -c
  # This should fail the SNI Hostname check
  run ! solr api -verbose -get "https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS"
  assert_output --partial 'Invalid SNI'

  # Using the right hostname should not fail the SNI Hostname check
  run curl --http2 --cacert "$ssl_dir/solr-ssl.pem" --resolve "test.solr.apache.org:${SOLR_PORT}:127.0.0.1" "https://test.solr.apache.org:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS"
  assert_output --partial '"urlScheme":"https"'
}

@test "start solr with ssl and auth" {
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

  run curl -u name:password --basic --cacert "$ssl_dir/solr-ssl.pem" "https://localhost:${SOLR_PORT}/solr/admin/collections?action=CREATE&collection.configName=_default&name=test&numShards=2&replicationFactor=1&router.name=compositeId&wt=json"
  assert_output --partial '"status":0'

  run curl -u name:password --basic --http2 --cacert "$ssl_dir/solr-ssl.pem" "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial '"numFound":0'

  # When the Jenkins box "curl" supports --fail-with-body, add "--fail-with-body" and change "run" to "run !", to expect a failure
  run curl --http2 --cacert "$ssl_dir/solr-ssl.pem" "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*"
  assert_output --partial 'Error 401 Authentication'
}

@test "start solr with client truststore and security manager" {
  # Make a test tmp dir, as the security policy includes TMP, so that might already contain the BATS_TEST_TMPDIR
  test_tmp_dir="${BATS_TEST_TMPDIR}/tmp"
  mkdir -p "${test_tmp_dir}"
  test_tmp_dir="$(cd -P "${test_tmp_dir}" && pwd)"

  export SOLR_SECURITY_MANAGER_ENABLED=true
  export SOLR_OPTS="-Djava.io.tmpdir=${test_tmp_dir}"
  export SOLR_TOOL_OPTS="-Djava.io.tmpdir=${test_tmp_dir}"

  # Create a keystore
  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  export client_ssl_dir="${ssl_dir}-client"
  mkdir -p "$ssl_dir"
  (
    cd "$ssl_dir"
    rm -f solr-ssl.keystore.p12
    keytool -genkeypair -alias solr-ssl -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999 -keystore solr-ssl.keystore.p12 -storetype PKCS12 -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country"
  )
  mkdir -p "$client_ssl_dir"
  (
    cd "$client_ssl_dir"
    rm -f *
    keytool -export -alias solr-ssl -file solr-ssl.crt -keystore "$ssl_dir/solr-ssl.keystore.p12" -keypass secret -storepass secret
    keytool -import -v -trustcacerts -alias solr-ssl -file solr-ssl.crt -storetype PKCS12 -keystore solr-ssl.truststore.p12 -keypass secret -storepass secret  -noprompt
  )
  cp -R "$ssl_dir" "$client_ssl_dir"

  # Set ENV_VARs so that Solr uses this keystore
  export SOLR_SSL_ENABLED=true
  export SOLR_SSL_KEY_STORE=$ssl_dir/solr-ssl.keystore.p12
  export SOLR_SSL_KEY_STORE_PASSWORD=secret
  export SOLR_SSL_TRUST_STORE=$ssl_dir/solr-ssl.keystore.p12
  export SOLR_SSL_TRUST_STORE_PASSWORD=secret
  export SOLR_SSL_CLIENT_TRUST_STORE=$client_ssl_dir/solr-ssl.truststore.p12
  export SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=secret
  export SOLR_SSL_NEED_CLIENT_AUTH=false
  export SOLR_SSL_WANT_CLIENT_AUTH=true
  export SOLR_SSL_CHECK_PEER_NAME=true
  export SOLR_HOST=localhost
  export SOLR_SECURITY_MANAGER_ENABLED=true

  run solr start -c

  export SOLR_SSL_KEY_STORE=
  export SOLR_SSL_KEY_STORE_PASSWORD=
  export SOLR_SSL_TRUST_STORE=
  export SOLR_SSL_TRUST_STORE_PASSWORD=

  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000

  run solr create -c test -s 2
  assert_output --partial "Created collection 'test'"

  run solr api -get "https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS"
  assert_output --partial '"urlScheme":"https"'
}

@test "start solr with mTLS needed" {
  # Make a test tmp dir, as the security policy includes TMP, so that might already contain the BATS_TEST_TMPDIR
  test_tmp_dir="${BATS_TEST_TMPDIR}/tmp"
  mkdir -p "${test_tmp_dir}"
  test_tmp_dir="$(cd -P "${test_tmp_dir}" && pwd)"

  export SOLR_SECURITY_MANAGER_ENABLED=true
  export SOLR_OPTS="-Djava.io.tmpdir=${test_tmp_dir}"
  export SOLR_LOG_LEVEL="DEBUG"
  export SOLR_TOOL_OPTS="-Djava.io.tmpdir=${test_tmp_dir}" # To debug further use: -Djavax.net.debug=SSL,keymanager,trustmanager,ssl:handshake

  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  export server_ssl_dir="${ssl_dir}/server"
  export client_ssl_dir="${ssl_dir}/client"

  # Create a root & intermediary CA
  echo "${ssl_dir}"
  mkdir -p "${ssl_dir}"
  (
    cd "$ssl_dir"
    rm -f root.p12 root.pem ca.p12 ca.pem

    keytool -genkeypair -keystore root.p12 -storetype PKCS12 -keypass secret -storepass secret -alias root -ext bc:c -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa
    keytool -genkeypair -keystore ca.p12 -storetype PKCS12 -keypass secret -storepass secret -alias ca -ext bc:c -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    keytool -keystore root.p12 -storetype PKCS12 -storepass secret -alias root -exportcert -rfc > root.pem

    keytool -storepass secret -storetype PKCS12 -keystore ca.p12 -certreq -alias ca | \
        keytool -storepass secret -keystore root.p12  -storetype PKCS12 \
        -gencert -alias root -ext BC=0 -rfc > ca.pem
    keytool -keystore ca.p12 -importcert -storetype PKCS12 -storepass secret -alias root -file root.pem -noprompt
    keytool -keystore ca.p12 -importcert -storetype PKCS12 -storepass secret -alias ca -file ca.pem
  )
  # Create a server keystore & truststore
  mkdir -p "$server_ssl_dir"
  (
    cd "$server_ssl_dir"
    rm -f solr-server.keystore.p12 server.pem solr-server.truststore.p12

    # Create a keystore and certificate
    keytool -genkeypair -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -alias server -ext SAN=DNS:localhost,IP:127.0.0.1 -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    # Trust the keystore cert with the CA
    keytool -storepass server-key -keystore solr-server.keystore.p12 -storetype PKCS12 -certreq -alias server | \
        keytool -storepass secret -keystore "$ssl_dir/ca.p12" -storetype PKCS12 -gencert -alias ca \
        -ext "SAN=DNS:localhost,IP:127.0.0.1" -ext "ku:c=nonRepudiation,digitalSignature,keyEncipherment" -ext eku:c=serverAuth -rfc > server.pem
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias server -file server.pem

    # Create a truststore with just the Root CA
    keytool -keystore solr-server.truststore.p12 -storetype PKCS12 -keypass server-trust -storepass server-trust -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-server.truststore.p12 -storetype PKCS12 -keypass server-trust -storepass server-trust -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
  )
  # Create a client keystore & truststore
  # The client cert will have a bogus DNS name and IP address, as we want the clientHostnameVerification to fail
  mkdir -p "$client_ssl_dir"
  (
    cd "$client_ssl_dir"
    rm -f solr-client.keystore.p12 client.pem solr-client.truststore.p12

    # Create a keystore and certificate
    keytool -genkeypair -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -alias client -ext SAN=DNS:test.solr.apache.org,IP:127.0.0.2 -dname "CN=test.solr.apache.org, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    # Trust the keystore cert with the CA
    keytool -storepass client-key -keystore solr-client.keystore.p12 -storetype PKCS12 -certreq -alias client | \
        keytool -storepass secret -keystore "$ssl_dir/ca.p12" -storetype PKCS12 -gencert -alias ca \
        -ext "SAN=DNS:test.solr.apache.org,IP:127.0.0.2" -ext "ku:c=nonRepudiation,digitalSignature,keyEncipherment" -ext eku:c=clientAuth -rfc > client.pem
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias client -file client.pem

    # Create a truststore with just the Root CA
    keytool -keystore solr-client.truststore.p12 -storetype PKCS12 -keypass client-trust -storepass client-trust -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-client.truststore.p12 -storetype PKCS12 -keypass client-trust -storepass client-trust -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
  )

  # Set ENV_VARs so that Solr uses this keystore
  export SOLR_SSL_ENABLED=true
  export SOLR_SSL_KEY_STORE="$server_ssl_dir/solr-server.keystore.p12"
  export SOLR_SSL_KEY_STORE_PASSWORD=server-key
  export SOLR_SSL_KEY_STORE_TYPE=PKCS12
  export SOLR_SSL_TRUST_STORE="$server_ssl_dir/solr-server.truststore.p12"
  export SOLR_SSL_TRUST_STORE_PASSWORD=server-trust
  export SOLR_SSL_TRUST_STORE_TYPE=PKCS12
  export SOLR_SSL_CLIENT_KEY_STORE="$client_ssl_dir/solr-client.keystore.p12"
  export SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=client-key
  export SOLR_SSL_CLIENT_KEY_STORE_TYPE=PKCS12
  export SOLR_SSL_CLIENT_TRUST_STORE="$client_ssl_dir/solr-client.truststore.p12"
  export SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=client-trust
  export SOLR_SSL_CLIENT_TRUST_STORE_TYPE=PKCS12
  export SOLR_SSL_NEED_CLIENT_AUTH=true
  export SOLR_SSL_WANT_CLIENT_AUTH=false
  export SOLR_SSL_CHECK_PEER_NAME=true
  # Cannot set this to true, because the client certificate does not have the right hostname ("localhost") or IP
  export SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=false
  export SOLR_HOST=localhost

  solr start -c
  solr start -c -z localhost:${ZK_PORT} -p ${SOLR2_PORT}

  # Test Client connections, which do not need the server keystore/truststore
  (
    export SOLR_SSL_KEY_STORE=
    export SOLR_SSL_KEY_STORE_PASSWORD=
    export SOLR_SSL_TRUST_STORE=
    export SOLR_SSL_TRUST_STORE_PASSWORD=

    solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000
    solr assert --started https://localhost:${SOLR2_PORT}/solr --timeout 5000

    run solr create -c test -s 2
    assert_output --partial "Created collection 'test'"

    run solr api -get "https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS"
    assert_output --partial '"urlScheme":"https"'

    run solr api -get "https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*&rows=0"
    assert_output --partial '"numFound":0'

    (
      # Try connecting with just the client truststore, which should fail because mTLS requires a keystore and mTLS is needed
      export SOLR_SSL_CLIENT_KEY_STORE=
      export SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=

      run ! solr api -verbose -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0"
      assert_output --regexp '(bad_certificate|Server refused connection)'
    )
  )

  # Turn on client hostname verification, and start a new Solr node since the property is a server setting.
  # Test that it fails because the client cert does not use "localhost"
  export SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=true
  solr start -c -z localhost:${ZK_PORT} -p ${SOLR3_PORT}

  # We can't check if the server has come up, because we can't connect to it, so just wait
  sleep 5

  run ! solr api -verbose -get "https://localhost:${SOLR3_PORT}/solr/test/select?q=*:*&rows=0"
  assert_output --regexp '(certificate_unknown|java.nio.channels.ClosedChannelException|Server refused connection)'
}

@test "start solr with mTLS wanted" {
  # Make a test tmp dir, as the security policy includes TMP, so that might already contain the BATS_TEST_TMPDIR
  test_tmp_dir="${BATS_TEST_TMPDIR}/tmp"
  mkdir -p "${test_tmp_dir}"
  test_tmp_dir="$(cd -P "${test_tmp_dir}" && pwd)"

  export SOLR_SECURITY_MANAGER_ENABLED=true
  export SOLR_OPTS="-Djava.io.tmpdir=${test_tmp_dir}"
  export SOLR_TOOL_OPTS="-Djava.io.tmpdir=${test_tmp_dir}" # To debug further use: -Djavax.net.debug=SSL,keymanager,trustmanager,ssl:handshake

  export ssl_dir="${BATS_TEST_TMPDIR}/ssl"
  export server_ssl_dir="${ssl_dir}/server"
  export client_ssl_dir="${ssl_dir}/client"

  # Create a root & intermediary CA
  echo "${ssl_dir}"
  mkdir -p "${ssl_dir}"
  (
    cd "$ssl_dir"
    rm -f root.p12 root.pem ca.p12 ca.pem

    keytool -genkeypair -keystore root.p12 -storetype PKCS12 -keypass secret -storepass secret -alias root -ext bc:c -ext "SAN=DNS:localhost,IP:127.0.0.1" -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa
    keytool -genkeypair -keystore ca.p12 -storetype PKCS12 -keypass secret -storepass secret -alias ca -ext bc:c -ext "SAN=DNS:localhost,IP:127.0.0.1" -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    keytool -keystore root.p12 -storetype PKCS12 -storepass secret -alias root -exportcert -rfc > root.pem

    keytool -storepass secret -storetype PKCS12 -keystore ca.p12 -certreq -alias ca | \
     keytool -storepass secret -keystore root.p12  -storetype PKCS12 \
     -gencert -alias root -ext BC=0 -rfc > ca.pem
    keytool -keystore ca.p12 -importcert -storetype PKCS12 -storepass secret -alias root -file root.pem -noprompt
    keytool -keystore ca.p12 -importcert -storetype PKCS12 -storepass secret -alias ca -file ca.pem
  )
  # Create a server keystore & truststore
  mkdir -p "$server_ssl_dir"
  (
    cd "$server_ssl_dir"
    rm -f solr-server.keystore.p12 server.pem solr-server.truststore.p12

    # Create a keystore and certificate
    keytool -genkeypair -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -alias server -ext "SAN=DNS:localhost,IP:127.0.0.1" -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    # Trust the keystore cert with the CA
    keytool -storepass server-key -keystore solr-server.keystore.p12 -storetype PKCS12 -certreq -alias server | \
     keytool -storepass secret -keystore "$ssl_dir/ca.p12" -storetype PKCS12 -gencert -alias ca \
     -ext "SAN=DNS:localhost,IP:127.0.0.1" -ext "ku:c=nonRepudiation,digitalSignature,keyEncipherment" -ext eku:c=serverAuth -rfc > server.pem
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
    keytool -keystore solr-server.keystore.p12 -storetype PKCS12 -keypass server-key -storepass server-key -importcert -alias server -file server.pem

    # Create a truststore with just the Root CA
    keytool -keystore solr-server.truststore.p12 -storetype PKCS12 -keypass server-trust -storepass server-trust -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-server.truststore.p12 -storetype PKCS12 -keypass server-trust -storepass server-trust -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
  )
  # Create a client keystore & truststore
  mkdir -p "$client_ssl_dir"
  (
    cd "$client_ssl_dir"
    rm -f solr-client.keystore.p12 client.pem solr-client.truststore.p12

    # Create a keystore and certificate
    keytool -genkeypair -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -alias client -ext "SAN=DNS:localhost,IP:127.0.0.1" -dname "CN=localhost, OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country" -keyalg rsa

    # Trust the keystore cert with the CA
    keytool -storepass client-key -keystore solr-client.keystore.p12 -storetype PKCS12 -certreq -alias client | \
     keytool -storepass secret -keystore "$ssl_dir/ca.p12" -storetype PKCS12 -gencert -alias ca \
     -ext "SAN=DNS:localhost,IP:127.0.0.1" -ext "ku:c=nonRepudiation,digitalSignature,keyEncipherment" -ext eku:c=clientAuth -rfc > client.pem
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
    keytool -keystore solr-client.keystore.p12 -storetype PKCS12 -keypass client-key -storepass client-key -importcert -alias client -file client.pem

    # Create a truststore with just the Root CA
    keytool -keystore solr-client.truststore.p12 -storetype PKCS12 -keypass client-trust -storepass client-trust -importcert -alias root -file "$ssl_dir/root.pem"  -noprompt
    keytool -keystore solr-client.truststore.p12 -storetype PKCS12 -keypass client-trust -storepass client-trust -importcert -alias ca -file "$ssl_dir/ca.pem"  -noprompt
  )

  # Set ENV_VARs so that Solr uses this keystore
  export SOLR_SSL_ENABLED=true
  export SOLR_SSL_KEY_STORE="$server_ssl_dir/solr-server.keystore.p12"
  export SOLR_SSL_KEY_STORE_PASSWORD=server-key
  export SOLR_SSL_KEY_STORE_TYPE=PKCS12
  export SOLR_SSL_TRUST_STORE="$server_ssl_dir/solr-server.truststore.p12"
  export SOLR_SSL_TRUST_STORE_PASSWORD=server-trust
  export SOLR_SSL_TRUST_STORE_TYPE=PKCS12
  export SOLR_SSL_CLIENT_KEY_STORE="$client_ssl_dir/solr-client.keystore.p12"
  export SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=client-key
  export SOLR_SSL_CLIENT_KEY_STORE_TYPE=PKCS12
  export SOLR_SSL_CLIENT_TRUST_STORE="$client_ssl_dir/solr-client.truststore.p12"
  export SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=client-trust
  export SOLR_SSL_CLIENT_TRUST_STORE_TYPE=PKCS12
  export SOLR_SSL_NEED_CLIENT_AUTH=false
  export SOLR_SSL_WANT_CLIENT_AUTH=true
  export SOLR_SSL_CHECK_PEER_NAME=true
  export SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION=true
  export SOLR_HOST=localhost

  solr start -c
  solr start -c -z localhost:${ZK_PORT} -p ${SOLR2_PORT}

  export SOLR_SSL_KEY_STORE=
  export SOLR_SSL_KEY_STORE_PASSWORD=
  export SOLR_SSL_TRUST_STORE=
  export SOLR_SSL_TRUST_STORE_PASSWORD=

  solr assert --started https://localhost:${SOLR_PORT}/solr --timeout 5000
  solr assert --started https://localhost:${SOLR2_PORT}/solr --timeout 5000

  run solr create -c test -s 2
  assert_output --partial "Created collection 'test'"

  run solr api -get "https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS"
  assert_output --partial '"urlScheme":"https"'

  run solr api -get "https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'

  export SOLR_SSL_CLIENT_KEY_STORE=
  export SOLR_SSL_CLIENT_KEY_STORE_PASSWORD=

  # mTLS requires a keyStore, so just using the truststore would fail if mTLS was "NEED"ed, however it is only "WANT"ed, so its ok
  run solr api -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0"
  assert_output --partial '"numFound":0'

  export SOLR_SSL_CLIENT_TRUST_STORE=
  export SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD=

  # TLS cannot work if a truststore and keystore are not provided (either Server or Client)
  run solr api -verbose -get "https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0"
  assert_output --regexp '(unable to find valid certification path to requested target|Server refused connection)'
}
