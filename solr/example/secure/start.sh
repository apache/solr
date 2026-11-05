#!/usr/bin/env bash

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

set -euo pipefail
umask 077

install_dir=$1
server_dir=$2
shift 2
cluster_dir="$install_dir/example/secure/cluster"
port=${SOLR_PORT_LISTEN:-8983}
no_prompt=false
force=''

die() { echo "ERROR: $*" >&2; exit 1; }

while (($#)); do
  case "$1" in
    -e|--example) shift 2 ;;
    --solr-home) cluster_dir=$2; shift 2 ;;
    -p|--port) port=$2; shift 2 ;;
    -y|--no-prompt) no_prompt=true; shift ;;
    --force) force=--force; shift ;;
    *) die "Unsupported secure example option: $1. Use --solr-home, -p, --no-prompt, or --force." ;;
  esac
done

[[ "$port" =~ ^[1-9][0-9]{3,4}$ ]] || die 'Choose a port between 1024 and 64535.'
((port >= 1024 && port <= 64535)) || die 'Choose a port between 1024 and 64535.'
[[ -z "${ZK_HOST:-}" ]] || die 'The secure example requires its own embedded ZooKeeper; unset ZK_HOST.'
[[ -z "${SOLR_DATA_HOME:-}" ]] || die 'The secure example requires its own data directories; unset SOLR_DATA_HOME.'
for executable in curl keytool openssl; do
  command -v "$executable" >/dev/null || die "$executable must be available on PATH."
done
[[ "$cluster_dir" != *[[:space:]]* ]] || die 'The example directory must not contain whitespace.'
mkdir -p "$cluster_dir"
cluster_dir=$(cd "$cluster_dir" && pwd -P)
tls="$cluster_dir/tls"
marker="$cluster_dir/.secure-example"
second_port=$((port + 1))
zk_host="localhost:$((port + 1000))/solr"
export SOLR_PID_DIR="$cluster_dir"
export SOLR_INCLUDE="$tls/solr.in.sh"

solr() { "$install_dir/bin/solr" "$@"; }

start_nodes() {
  SOLR_LOGS_DIR="$cluster_dir/node1/logs" solr start --solr-home "$cluster_dir/node1/solr" -p "$port" ${force:+"$force"}
  SOLR_LOGS_DIR="$cluster_dir/node2/logs" solr start --solr-home "$cluster_dir/node2/solr" -p "$second_port" -z "$zk_host" ${force:+"$force"}
}

stop_nodes() {
  solr stop -p "$second_port"
  solr stop -p "$port"
}

request() {
  curl --noproxy localhost,127.0.0.1 --silent --show-error --fail \
    --connect-timeout 2 --max-time 30 --cacert "$tls/cert.pem" "$@"
}

wait_for_node() {
  local deadline=$((SECONDS + 60))
  until request --user "${2:-solr:SolrRocks}" "$1" > /dev/null; do
    ((SECONDS < deadline)) || die "Timed out waiting for $1."
    sleep 1
  done
}

if [[ -f "$marker" ]]; then
  [[ "$(cat "$marker")" == "$port" ]] || die 'Restart with the same port used to create this example.'
  for file in tls/solr.in.sh tls/cert.pem tls/solr-ssl.keystore.p12 tls/solr-ssl.truststore.p12 \
      node1/solr/solr.xml node1/solr/zoo.cfg tls/basicAuth.conf node2/solr/solr.xml zkcredentials.properties; do
    [[ -f "$cluster_dir/$file" ]] || die "The example is incomplete: missing $file."
  done
  echo 'Reusing the secure example certificates, credentials, and data.'
  start_nodes
else
  [[ -z "$(ls -A "$cluster_dir")" ]] || die 'The example directory contains incomplete or unrelated data. Choose an empty --solr-home.'
  echo 'This local tutorial uses SolrRocks for its keystore and demonstration accounts.'
  echo 'ZooKeeper is protected by digest ACLs; its transport remains plaintext on loopback.'
  if ! "$no_prompt"; then
    read -r -p 'Press Enter to configure two local Solr nodes, or Ctrl-C to cancel: '
  fi
  mkdir -p "$tls" "$cluster_dir/node1/solr" "$cluster_dir/node2/solr"
  for node in node1 node2; do
    awk -v host="$zk_host" '{print} /<solrcloud>/ {print "    <str name=\"zkHost\">" host "</str>"}' \
      "$server_dir/solr/solr.xml" > "$cluster_dir/$node/solr/solr.xml"
    cp "$server_dir/solr/zoo.cfg" "$cluster_dir/$node/solr/zoo.cfg"
  done
  {
    printf 'SOLR_PID_DIR=%q\n' "$cluster_dir"
    printf 'SOLR_OPTS=%q\n' "-Dsolr.security.allow.paths=$cluster_dir"
    printf '%s\n' 'SOLR_HOST_BIND=127.0.0.1' 'SOLR_HOST_ADVERTISE=127.0.0.1' 'ZK_CREATE_CHROOT=true'
  } > "$SOLR_INCLUDE"
  cleanup() {
    local result=$?
    if ((result != 0)); then
      echo "Setup failed. See $cluster_dir/node{1,2}/logs/solr.log. Use an empty home to try again." >&2
      solr stop -p "$second_port" >/dev/null 2>&1 || true
      solr stop -p "$port" >/dev/null 2>&1 || true
    fi
    exit "$result"
  }
  trap cleanup EXIT

  echo '1. Start both nodes and configure the shared SolrCloud URL scheme.'
  start_nodes
  solr cluster --property urlScheme --value https --zk-host "$zk_host"
  stop_nodes

  echo '2. Generate a self-signed certificate and PKCS12 key and trust stores.'
  printf 'SolrRocks\n' > "$tls/keystore-password.txt"
  keytool -genkeypair -alias solr-ssl -keyalg RSA -keysize 2048 -validity 30 \
    -dname CN=localhost -ext SAN=DNS:localhost,IP:127.0.0.1 \
    -storetype PKCS12 -keystore "$tls/solr-ssl.keystore.p12" \
    -storepass:file "$tls/keystore-password.txt" -keypass:file "$tls/keystore-password.txt"
  keytool -exportcert -rfc -alias solr-ssl -keystore "$tls/solr-ssl.keystore.p12" \
    -storepass:file "$tls/keystore-password.txt" -file "$tls/cert.pem"
  keytool -importcert -noprompt -alias solr-ssl -file "$tls/cert.pem" \
    -storetype PKCS12 -keystore "$tls/solr-ssl.truststore.p12" -storepass:file "$tls/keystore-password.txt"
  openssl x509 -in "$tls/cert.pem" -noout -subject -dates
  {
    printf 'SOLR_SSL_KEY_STORE=%q\n' "$tls/solr-ssl.keystore.p12"
    printf 'SOLR_SSL_TRUST_STORE=%q\n' "$tls/solr-ssl.truststore.p12"
    cat <<'SETTINGS'
SOLR_SSL_ENABLED=true
SOLR_SSL_KEY_STORE_TYPE=PKCS12
SOLR_SSL_KEY_STORE_PASSWORD=SolrRocks
SOLR_SSL_TRUST_STORE_TYPE=PKCS12
SOLR_SSL_TRUST_STORE_PASSWORD=SolrRocks
SOLR_SSL_NEED_CLIENT_AUTH=false
SOLR_SSL_WANT_CLIENT_AUTH=false
SOLR_SSL_CHECK_PEER_NAME=true
SETTINGS
  } >> "$SOLR_INCLUDE"

  echo '3. Restart both nodes with HTTPS and verify the certificate.'
  start_nodes
  for node_port in "$port" "$second_port"; do
    wait_for_node "https://localhost:$node_port/api/node/system"
  done

  echo '4. Enable Basic Authentication and configure separate administrator, indexing, and search accounts.'
  SOLR_HOME="$tls" solr auth enable --credentials solr:SolrRocks \
    --block-unknown true --zk-host "$zk_host"
  wait_for_node "https://localhost:$port/api/node/system"
  request --user solr:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '{"set-user":{"index":"SolrRocks","search":"SolrRocks"},"delete-user":["admin"]}' \
    "https://localhost:$port/api/cluster/security/authentication"
  request --user solr:SolrRocks -H 'Content-Type: application/json' \
    --data-binary '{"update-permission":{"index":9,"role":["admin","search"]},"set-user-role":{"solr":["superadmin","admin"],"index":["index","search"],"search":["search"],"admin":null}}' \
    "https://localhost:$port/api/cluster/security/authorization"

  echo '5. Configure ZooKeeper digest accounts and migrate existing znodes to restricted ACLs.'
  cat > "$cluster_dir/zkcredentials.properties" <<'CREDENTIALS'
zkDigestUsername=zk-admin
zkDigestPassword=SolrRocks
zkDigestReadonlyUsername=zk-readonly
zkDigestReadonlyPassword=SolrRocks
CREDENTIALS
  {
    printf '\n'
    printf 'SOLR_ZK_CREDS_AND_ACLS=%q\n' "-DzkCredentialsInjector=org.apache.solr.common.cloud.VMParamsZkCredentialsInjector -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider -DzkACLProvider=org.apache.solr.common.cloud.DigestZkACLProvider -DzkDigestCredentialsFile=$cluster_dir/zkcredentials.properties"
    # shellcheck disable=SC2016
    printf '%s\n' 'SOLR_OPTS="${SOLR_OPTS:-} $SOLR_ZK_CREDS_AND_ACLS"'
  } >> "$SOLR_INCLUDE"
  stop_nodes
  start_nodes
  for node_port in "$port" "$second_port"; do
    wait_for_node "https://localhost:$node_port/api/node/system"
  done
  solr zk updateacls / --zk-host "$zk_host"

  echo '6. Create one shard per node and index the techproducts documents over HTTPS.'
  solr create -c techproducts --shards 2 --replication-factor 1 \
    --conf-dir "$server_dir/solr/configsets/sample_techproducts_configs/conf" \
    --solr-url "https://localhost:$port" --credentials solr:SolrRocks
  for document in "$install_dir"/example/exampledocs/*.xml; do
    request --user index:SolrRocks -H 'Content-Type: application/xml' --data-binary "@$document" \
      "https://localhost:$port/solr/techproducts/update" > /dev/null
  done
  request --user index:SolrRocks -H 'Content-Type: application/json' --data-binary '{"commit":{}}' \
    "https://localhost:$port/solr/techproducts/update" > /dev/null
fi

for node_port in "$port" "$second_port"; do
  wait_for_node "https://localhost:$node_port/solr/techproducts/select?q=*:*&rows=0&shards.tolerant=false" search:SolrRocks
done
printf '%s\n' "$port" > "$marker"
trap - EXIT

echo "Two-node secure example is ready: https://localhost:$port/solr/ and https://localhost:$second_port/solr/"
echo 'HTTP accounts: solr (administration), index (indexing/search), search (search only). Password: SolrRocks.'
echo 'ZooKeeper accounts: zk-admin (all), zk-readonly (metadata read only). Password: SolrRocks.'
echo "Configuration: $SOLR_INCLUDE"
echo 'Verify both HTTPS endpoints:'
for node_port in "$port" "$second_port"; do
  printf 'curl --cacert %q --user solr:SolrRocks %q\n' "$tls/cert.pem" "https://localhost:$node_port/api/node/system"
done
echo 'Verify distributed HTTPS requests:'
for node_port in "$port" "$second_port"; do
  printf 'curl --cacert %q --user search:SolrRocks %q\n' "$tls/cert.pem" "https://localhost:$node_port/solr/techproducts/select?q=*:*&rows=0&shards.info=true"
done
printf 'Stop node 2, then node 1: SOLR_INCLUDE=%q bin/solr stop -p %s\n' "$SOLR_INCLUDE" "$second_port"
printf 'SOLR_INCLUDE=%q bin/solr stop -p %s\n' "$SOLR_INCLUDE" "$port"
echo 'See the Securing Solr tutorial for the individual commands and inter-node trust exercise.'
