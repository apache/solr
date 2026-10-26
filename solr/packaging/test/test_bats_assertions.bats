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
  common_setup
}

@test "absent child requires a successful listing" {
  solr() {
    echo 'lookup unavailable' >&2
    return 1
  }
  run ! assert_zk_child_absent /collections missing
  assert_output --partial 'lookup unavailable'
}

@test "absent child matches a whole name" {
  solr() {
    echo 'COLL_NAME_OTHER'
  }
  assert_zk_child_absent /collections COLL_NAME
  run ! assert_zk_child_absent /collections COLL_NAME_OTHER
}

@test "health requires the collection status rather than a healthy shard" {
  report() {
    echo '{"collection":"c","status":"degraded","shards":[{"status":"healthy"}]}'
  }
  run ! assert_healthy report
  assert_output --partial 'degraded'
}

@test "healthy output from a failed command is rejected" {
  report() {
    echo '{"collection":"c","status":"healthy"}'
    return 1
  }
  run ! assert_healthy report
}

@test "healthy collection output is accepted" {
  report() {
    echo '{"collection":"c","status":"healthy","shards":[{"status":"healthy"}]}'
  }
  assert_healthy report
}

@test "connection refusal cannot satisfy a TLS assertion" {
  failure() {
    echo 'ERROR: Server refused connection at: https://localhost:1'
    return 1
  }
  check_failure() {
    run failure
    assert_tls_failure 'certificate_required'
  }
  run ! check_failure
}

@test "a closed channel alone cannot satisfy a TLS assertion" {
  failure() {
    echo 'java.nio.channels.ClosedChannelException'
    return 1
  }
  check_failure() {
    run failure
    assert_tls_failure 'certificate_required'
  }
  run ! check_failure
}

@test "a handshake failure satisfies a TLS assertion" {
  failure() {
    echo 'javax.net.ssl.SSLHandshakeException: certificate_required'
    return 1
  }
  run failure
  assert_tls_failure 'certificate_required'
}

@test "an unrelated TLS failure cannot satisfy a certificate assertion" {
  failure() {
    echo 'javax.net.ssl.SSLHandshakeException: protocol_version'
    return 1
  }
  check_failure() {
    run failure
    assert_tls_failure 'certificate_required'
  }
  run ! check_failure
}
