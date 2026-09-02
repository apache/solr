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

# Apply the ExtractingRequestHandler via Config API and print error body on failure.
# Defaults to the shared bats_tika container; pass a second arg to point at a different
# TikaServer port (e.g. a test-specific container).
apply_extract_handler() {
  local collection="$1"
  local tika_port="${2:-${TIKA_PORT}}"
  local json="{\"add-requesthandler\":{\"name\":\"/update/extract\",\"class\":\"org.apache.solr.handler.extraction.ExtractingRequestHandler\",\"tikaserver.url\":\"http://localhost:${tika_port}\",\"defaults\":{\"lowernames\":\"true\",\"captureAttr\":\"true\"}}}"
  local url="http://localhost:${SOLR_PORT}/solr/${collection}/config"
  # Capture body and status code
  local resp code body
  sleep 5
  resp=$(curl -s -S -w "\n%{http_code}" -X POST -H 'Content-type:application/json' -d "$json" "$url")
  code="${resp##*$'\n'}"
  body="${resp%$'\n'*}"
  if [ "$code" = "200" ]; then
    return 0
  else
    echo "Config API error applying ExtractingRequestHandler to ${collection} (HTTP ${code}): ${body}" >&3
    return 1
  fi
}

setup_file() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    export TIKA_PORT=$((SOLR_PORT+5))
    docker run --rm -p ${TIKA_PORT}:9998 --name bats_tika -d apache/tika:4.0.0-full >/dev/null 2>&1 || true
    echo "Waiting for Tika Server to be ready on port ${TIKA_PORT}" >&3
    if ! wait_for 120 3 curl -s -f "http://localhost:${TIKA_PORT}/tika" -o /dev/null; then
      export DOCKER_UNAVAILABLE=1
      echo "WARNING: Tika Server did not become ready in time; Tika-dependent tests will be bypassed." >&3
    else
      echo "Tika Server is ready on port ${TIKA_PORT}" >&3
    fi
  else
    export DOCKER_UNAVAILABLE=1
    echo "WARNING: Docker not available (CLI missing or daemon not running); Tika-dependent tests will be bypassed." >&3
  fi
}

teardown_file() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    echo "Stopping Tika Server container" >&3
    docker stop bats_tika >/dev/null 2>&1 || true
  fi
}

setup() {
  common_clean_setup
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  delete_all_collections
  SOLR_STOP_WAIT=1 solr stop --all >/dev/null 2>&1

  # Defensive cleanup in case the chunks test failed before reaching its own cleanup
  pkill -f mock_embeddings_server.py >/dev/null 2>&1 || true
  docker stop bats_tika_chunks >/dev/null 2>&1 || true
}

@test "using curl to extract a single pdf file" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    skip "Docker is not available"
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c gettingstarted -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/gettingstarted/select?q=*:*" -o /dev/null
  apply_extract_handler gettingstarted

  curl "http://localhost:${SOLR_PORT}/solr/gettingstarted/update/extract?literal.id=doc1&commit=true" -F "myfile=@${SOLR_TIP}/example/exampledocs/solr-word.pdf"

  run curl "http://localhost:${SOLR_PORT}/solr/gettingstarted/select?q=id:doc1"
  assert_output --partial '"numFound":1'
}

@test "using the bin/solr post tool to extract content from pdf" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    skip "Docker is not available"
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c content_extraction -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/content_extraction/select?q=*:*" -o /dev/null
  apply_extract_handler content_extraction

  # We filter to pdf to invoke the Extract handler.
  run solr post --filetypes pdf --solr-url http://localhost:${SOLR_PORT} --name content_extraction ${SOLR_TIP}/example/exampledocs

  assert_output --partial '1 files indexed.'
  refute_output --partial 'ERROR'

  run curl "http://localhost:${SOLR_PORT}/solr/content_extraction/select?q=*:*"
  assert_output --partial '"numFound":1'
}

@test "using the bin/solr post tool to crawl web site" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    skip "Docker is not available"
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false
  solr start -Dsolr.modules=extraction

  solr create -c website_extraction -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/website_extraction/select?q=*:*" -o /dev/null
  apply_extract_handler website_extraction

  # Change to --recursive 1 to crawl multiple pages, but may be too slow.
  run solr post --mode web --solr-url http://localhost:${SOLR_PORT} -c website_extraction --recursive 0 --delay 1 https://solr.apache.org/

  assert_output --partial 'POSTed web resource https://solr.apache.org (depth: 0)'
  refute_output --partial 'ERROR'

  run curl "http://localhost:${SOLR_PORT}/solr/website_extraction/select?q=*:*"
  assert_output --partial '"numFound":1'
}

@test "using tikaserver.chunks to index Tika 4.x chunk embeddings into a dense_vector field" {

  if [ -n "${DOCKER_UNAVAILABLE:-}" ]; then
    skip "Docker is not available"
  fi

  # Disable security manager to allow extraction
  # This appears to be a bug.
  export SOLR_SECURITY_MANAGER_ENABLED=false

  # A minimal, stdlib-only, OpenAI-compatible embeddings endpoint standing in for a real
  # embeddings API, so this test has no external dependency.
  local embed_port=$((SOLR_PORT+6))
  python3 "${BATS_TEST_DIRNAME}/mock_embeddings_server.py" "${embed_port}" &
  local embed_pid=$!

  # A dedicated TikaServer, distinct from the shared bats_tika container, configured with an
  # openai-embedding-filter pointed at the mock embeddings endpoint above. Chunking requires
  # the Markdown content handler, which is Tika 4.x's default.
  local chunks_tika_port=$((SOLR_PORT+7))
  local tika_config="${BATS_TEST_TMPDIR}/tika-chunks-config.json"
  cat > "$tika_config" <<EOF
{
  "server": {},
  "metadata-filters": [
    {
      "openai-embedding-filter": {
        "baseUrl": "http://host.docker.internal:${embed_port}",
        "model": "mock-embed"
      }
    }
  ]
}
EOF
  docker run --rm -p ${chunks_tika_port}:9998 --add-host=host.docker.internal:host-gateway \
    -v "${tika_config}:/tika-config.json:ro" \
    --name bats_tika_chunks -d apache/tika:4.0.0-full -c /tika-config.json >/dev/null 2>&1

  wait_for 15 1 curl -s -f -X POST -H 'Content-Type: application/json' \
    -d '{"model":"mock-embed","input":["ping"]}' "http://localhost:${embed_port}/v1/embeddings" -o /dev/null
  wait_for 60 3 curl -s -f "http://localhost:${chunks_tika_port}/tika" -o /dev/null

  solr start -Dsolr.modules=extraction

  solr create -c chunks_extraction -d _default
  wait_for 30 3 curl -s -S -f "http://localhost:${SOLR_PORT}/solr/chunks_extraction/select?q=*:*" -o /dev/null
  apply_extract_handler chunks_extraction "${chunks_tika_port}"

  curl -s -X POST -H 'Content-type:application/json' \
    -d '{"add-field-type":{"name":"knn_vector","class":"solr.DenseVectorField","vectorDimension":4,"similarityFunction":"cosine"}}' \
    "http://localhost:${SOLR_PORT}/solr/chunks_extraction/schema"
  curl -s -X POST -H 'Content-type:application/json' \
    -d '{"add-field":{"name":"vector","type":"knn_vector","indexed":true,"stored":true}}' \
    "http://localhost:${SOLR_PORT}/solr/chunks_extraction/schema"
  curl -s -X POST -H 'Content-type:application/json' \
    -d '{"add-field":{"name":"chunk_parent_id","type":"string","indexed":true,"stored":true}}' \
    "http://localhost:${SOLR_PORT}/solr/chunks_extraction/schema"

  local sample="${BATS_TEST_TMPDIR}/sample.md"
  printf '# Report\n\nRevenue grew 15%% year over year in the last quarter.\n\n# Costs\n\nOperating costs remained flat compared to prior periods and did not change much.\n' > "$sample"

  # resource.name is required so TikaServer detects this upload as Markdown (curl's multipart
  # Content-Type guess for .md is unreliable); chunking only splits on headings for Markdown content.
  curl -s "http://localhost:${SOLR_PORT}/solr/chunks_extraction/update/extract?tikaserver.chunks=true&resource.name=sample.md&literal.id=doc1&commit=true" \
    -F "myfile=@${sample}"

  run curl -s "http://localhost:${SOLR_PORT}/solr/chunks_extraction/select?q=chunk_parent_id:doc1"
  assert_output --partial '"numFound":2'

  run curl -s "http://localhost:${SOLR_PORT}/solr/chunks_extraction/select?q=%7B!knn+f=vector+topK=3%7D%5B0.7,0.8,0.9,0.0%5D&fl=id,vector"
  assert_output --partial '"numFound":2'
  refute_output --partial '"numFound":0'

  kill "$embed_pid" >/dev/null 2>&1 || true
  docker stop bats_tika_chunks >/dev/null 2>&1 || true
}
