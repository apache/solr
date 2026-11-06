# Secure Solr Tutorial Implementation Plan

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

> **For agentic workers:** Use rubypowers:executing-plans or rubypowers:subagent-driven-development. Track the steps below as they complete.

**Goal:** Teach and automate existing configuration for two HTTPS Solr nodes, Basic Auth, and ZooKeeper digest ACLs.

**Architecture:** A standalone reference-guide walkthrough explains all commands. `RunExampleTool.runSecureExample` executes the same existing commands through the normal example dispatch. Solr runtime and general security tooling remain unchanged.

**Tech Stack:** Java, Bash, OpenSSL, JDK keytool, existing Solr CLI and HTTP APIs, AsciiDoc, BATS.

## Tasks

- [x] Add `solr/packaging/test/test_secure_example.bats` exercising `solr start -e secure --no-prompt --solr-home "$SOLR_HOME" -p "$SOLR_PORT"`. First run must fail because `secure` is not an existing example. Assert verified HTTPS on both nodes, distributed results and HTTPS shard addresses, anonymous rejection, read-only HTTP denial, ZooKeeper ACL behavior, unchanged global include, and restart with retained certificates/data.
- [x] Implement `runSecureExample` in `RunExampleTool.java` using the existing command executor and normal example dispatch. Keep the manual commands in the tutorial; do not ship a setup shell script. Support existing `--solr-home`, `-p`, `--no-prompt`, and `--force`; reject unsupported options explicitly. Use default homes under `example/secure`, and isolated includes, PID files, and logs. Create PKCS12 stores with keytool and OpenSSL, bootstrap Basic Auth with the existing auth tool, configure roles with security APIs, configure both digest accounts with existing JVM properties, migrate old ACLs, create two shards on two nodes, and post techproducts documents. Restart reuses state; incomplete state must fail clearly.
- [x] Add `solr/solr-ref-guide/modules/getting-started/pages/tutorial-securing-solr.adoc` and link it from `getting-started-nav.adoc`, `solr-tutorial.adoc`, and the existing HTTPS/control-script documentation where useful. Begin with `bin/solr start`, prepare the second home manually, and verify both nodes. Explain stores, include settings, cluster URL scheme, both authentication layers, distributed HTTPS, and a reversible failed-trust exercise. The shortcut appears only after the independent manual walkthrough.
- [x] Run `bash -n solr/bin/solr`, `./gradlew iTest --tests test_secure_example.bats`, and the relevant existing CLI/example tests. Execute the manual walkthrough against the distribution and verify failed outbound trust plus recovery.
- [x] Run `./gradlew check -x test`, reference-guide checks, and `git diff --check`. Review the final diff for scope, spec compliance, and code quality; fix material findings. Leave the finished work on `SOLR-8442-secure-tutorial` with a concise validation report.

## Baseline

Before implementation, the distribution built successfully and `TestSolrCLIRunExample` passed all six tests. The original exploration remains in the main working tree on `SOLR-8442`; all implementation uses `/private/tmp/solr-secure-tutorial`.

## Initial implementation verification

- The secure-example BATS suite passed both integration tests, covering both nodes, roles, ACLs, restart, failed outbound trust, and recovery.
- Existing help and cloud-example BATS suites passed 16 tests.
- The manual walkthrough returned 31 documents across two HTTPS shards (local counts 14 and 17), denied unauthorized HTTP and ZooKeeper operations, and demonstrated a PKIX trust failure followed by recovery.
- A separate distribution verified the default example homes, separate logs, private credential files, and distributed queries through both nodes. Verification nodes were stopped.
- Bash syntax, shellcheck, and whitespace checks passed.
- Repository checks and guide link validation passed with `./gradlew check -x test -x :solr:solr-ref-guide:buildLocalAntoraSite`. The site was built separately from a temporary local clone because Antora rejects a Git worktree's `.git` file; no project build changes were needed.
- The initial shell implementation did not change Java source, runtime policy, dependencies, or SSL tooling. The revised implementation moves the shortcut into `RunExampleTool.runSecureExample` at the user's request; runtime policy, dependencies, and SSL tooling remain unchanged.
