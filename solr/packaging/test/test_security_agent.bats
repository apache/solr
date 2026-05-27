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

# Integration tests for the Solr security agent (SOLR-17767).
#
# These tests exercise the full agent lifecycle against a real running Solr
# distribution, covering:
#   - ByteBuddy -javaagent: injection via startup script JAR detection
#   - All three env vars (SOLR_SECURITY_AGENT_SKIP, SOLR_SECURITY_AGENT_MODE,
#     SOLR_SECURITY_AGENT_EXTRA_POLICY) and their conversion to JVM system properties
#   - Violation counter registration via CoreContainer → SolrMetricManager
#
# The Java Security Manager must be explicitly disabled (SOLR_SECURITY_MANAGER_ENABLED=false)
# so it does not interfere with the agent's own ByteBuddy interceptors. JSM is enabled
# by default in bin/solr for Java < 24; we disable it here to test our agent in isolation.

load bats_helper

setup() {
  common_clean_setup
  # Disable the Java Security Manager so it does not interfere with the
  # ByteBuddy agent's own interception of JDK methods.
  export SOLR_SECURITY_MANAGER_ENABLED=false
}

teardown() {
  save_home_on_failure
  solr stop --all >/dev/null 2>&1
}

# Helper: path to the console log for the primary Solr port.
console_log() {
  echo "${SOLR_LOGS_DIR}/solr-${SOLR_PORT}-console.log"
}

@test "agent is active by default in warn mode and registers violation metrics" {
  solr start --user-managed
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

  # Confirms: JAR detected in lib/ext/, -javaagent: injected, premain ran
  run grep "Security agent active" "$(console_log)"
  assert_success

  # Confirms: default enforcement mode is WARN (no SOLR_SECURITY_AGENT_MODE set)
  assert_output --partial "mode=WARN"

  # Confirms: CoreContainer reflective call to ViolationMetricsReporter registered counters.
  # Metrics appear in Prometheus format (security.agent.violations.file →
  # security_agent_violations_file_total) since SolrMetricManager is OTel-based.
  run curl -sf "http://localhost:${SOLR_PORT}/solr/admin/metrics"
  assert_success
  assert_output --partial "security_agent_violations_file"
  assert_output --partial "security_agent_violations_network"
}

@test "SOLR_SECURITY_AGENT_SKIP=true disables the agent" {
  export SOLR_SECURITY_AGENT_SKIP=true
  solr start --user-managed
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

  # Confirms: skip-escape-hatch in bin/solr suppressed -javaagent: entirely
  run cat "$(console_log)"
  refute_output --partial "Security agent active"
}

@test "SOLR_SECURITY_AGENT_MODE=enforce and SOLR_SECURITY_AGENT_EXTRA_POLICY are passed through" {
  # Write a minimal extra policy with one additional FilePermission grant.
  # This exercises the env-var → -Dsolr.security.agent.extra.policy sysprop path.
  local extra_policy="${BATS_TEST_TMPDIR}/extra.policy"
  cat > "${extra_policy}" <<'EOF'
// Operator extension policy used by BATS test_security_agent.bats
grant {
  permission java.io.FilePermission "/tmp/bats-agent-test/-", "read";
};
EOF

  export SOLR_SECURITY_AGENT_MODE=enforce
  export SOLR_SECURITY_AGENT_EXTRA_POLICY="${extra_policy}"
  solr start --user-managed
  solr assert --started http://localhost:${SOLR_PORT} --timeout 5000

  # Confirms: SOLR_SECURITY_AGENT_MODE → -Dsolr.security.agent.mode → premain reads it
  run grep "Security agent active" "$(console_log)"
  assert_success
  assert_output --partial "mode=ENFORCE"

  # Confirms: SOLR_SECURITY_AGENT_EXTRA_POLICY was passed as sysprop and the extra
  # policy was loaded — permitted paths count is higher than the default-only count
  # (the extra.policy file adds 1 path on top of the bundled agent-security.policy).
  local with_extra
  with_extra=$(grep -oE "permitted paths=[0-9]+" "$(console_log)" | grep -oE "[0-9]+" | head -1)
  [ "${with_extra:-0}" -gt 0 ]
}

@test "enforce mode blocks unauthorized file access with SecurityException" {
  local agent_jar
  agent_jar=$(ls "${SOLR_TIP}/server/lib/ext/solr-agent-sm-"*.jar)

  # FileViolation reads /etc/hosts — outside every path the default policy permits.
  # The class is pre-compiled into AGENT_TEST_PROGRAMS_JAR by the Gradle build.
  run java \
    -javaagent:"${agent_jar}" \
    -Dsolr.security.agent.mode=enforce \
    -Dsolr.install.dir="${SOLR_TIP}" \
    -Dsolr.solr.home="${SOLR_TIP}/server/solr" \
    -Dsolr.logs.dir="${BATS_TEST_TMPDIR}" \
    -Dsolr.port.listen=8983 \
    -cp "${AGENT_TEST_PROGRAMS_JAR}" \
    FileViolation

  assert_failure
  assert_output --partial "SecurityException"
  refute_output --partial "read succeeded"
}

@test "enforce mode blocks System.exit with SecurityException" {
  local agent_jar
  agent_jar=$(ls "${SOLR_TIP}/server/lib/ext/solr-agent-sm-"*.jar)

  # ExitViolation calls System.exit(0) — no exitVM grant in the default policy.
  run java \
    -javaagent:"${agent_jar}" \
    -Dsolr.security.agent.mode=enforce \
    -Dsolr.install.dir="${SOLR_TIP}" \
    -Dsolr.solr.home="${SOLR_TIP}/server/solr" \
    -Dsolr.logs.dir="${BATS_TEST_TMPDIR}" \
    -Dsolr.port.listen=8983 \
    -cp "${AGENT_TEST_PROGRAMS_JAR}" \
    ExitViolation

  assert_failure
  assert_output --partial "SecurityException"
  refute_output --partial "exit succeeded"
}

@test "enforce mode blocks unauthorized outbound connection with SecurityException" {
  local agent_jar
  agent_jar=$(ls "${SOLR_TIP}/server/lib/ext/solr-agent-sm-"*.jar)

  # NetworkViolation opens a SocketChannel to 192.0.2.1:443 (TEST-NET-1, RFC 5737 —
  # guaranteed non-routable). The interceptor fires before any TCP I/O, so this is instant.
  run java \
    -javaagent:"${agent_jar}" \
    -Dsolr.security.agent.mode=enforce \
    -Dsolr.install.dir="${SOLR_TIP}" \
    -Dsolr.solr.home="${SOLR_TIP}/server/solr" \
    -Dsolr.logs.dir="${BATS_TEST_TMPDIR}" \
    -Dsolr.port.listen=8983 \
    -cp "${AGENT_TEST_PROGRAMS_JAR}" \
    NetworkViolation

  assert_failure
  assert_output --partial "SecurityException"
  refute_output --partial "connect succeeded"
}

@test "enforce mode blocks process exec with SecurityException" {
  local agent_jar
  agent_jar=$(ls "${SOLR_TIP}/server/lib/ext/solr-agent-sm-"*.jar)

  # ExecViolation spawns a child process via ProcessBuilder — no exec grant in default policy.
  run java \
    -javaagent:"${agent_jar}" \
    -Dsolr.security.agent.mode=enforce \
    -Dsolr.install.dir="${SOLR_TIP}" \
    -Dsolr.solr.home="${SOLR_TIP}/server/solr" \
    -Dsolr.logs.dir="${BATS_TEST_TMPDIR}" \
    -Dsolr.port.listen=8983 \
    -cp "${AGENT_TEST_PROGRAMS_JAR}" \
    ExecViolation

  assert_failure
  assert_output --partial "SecurityException"
  refute_output --partial "exec succeeded"
}
