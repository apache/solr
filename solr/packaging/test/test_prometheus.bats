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

  solr stop --all >/dev/null 2>&1
  
  shutdown_exporter
}

@test "should start solr prometheus exporter that scrapes solr for metrics" {
  solr start
  solr assert --started http://localhost:${SOLR_PORT}/solr --timeout 5000
  
  run solr create -c COLL_NAME
  assert_output --partial "Created new core 'COLL_NAME'"
 
  echo "# starting solr-exporter on ${SOLR_EXPORTER_PORT}" >&3
  run solr-exporter -p $SOLR_EXPORTER_PORT -b http://localhost:${SOLR_PORT}/solr >&3 &

  sleep 5

  run curl "http:/localhost:$SOLR_EXPORTER_PORT/"
  
  assert_output --partial 'solr_metrics_core_query_requests_total'
  assert_output --partial 'core="COLL_NAME"'
}
