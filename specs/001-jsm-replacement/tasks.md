# Tasks: Java Security Manager Replacement

**Input**: Design documents from `specs/001-jsm-replacement/`
**Branch**: `15868-java-security-manager` | **JIRA**: SOLR-17767
**Plan**: plan.md | **Spec**: spec.md (incl. clarifications 2026-04-28, 2026-04-29, 2026-04-30)

**New since previous version**: FR-016 (reference guide docs), FR-017 (violation metrics), SC-007/SC-008 (metrics + enforce-mode tests), old policy file deprecation, enforce mode scoped to agent-sm tests only, Decision 9 (port-wildcard intra-cluster network policy), T044/T045 (module outbound network — extraction NOTICE box only; 5 modules pre-permitted with wildcard in bundled policy per clarification 2026-04-29), T050/T047b (configurable extra-policy path via `SOLR_SECURITY_AGENT_EXTRA_POLICY` per clarification 2026-04-30), T048b (new agent env vars/sysprops documented in `solr-properties.adoc` per clarification 2026-04-30).

**Organization**: Tasks grouped by user story (US1–US5) to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no incomplete-task dependencies)
- **[Story]**: Which user story this implements ([US1]–[US5])

---

## Phase 1: Setup (Gradle Subproject Scaffold)

**Purpose**: Create the `solr/agent-sm/` Gradle subproject and wire it into the build. All subsequent work depends on this.

- [X] T001 Register `solr/agent-sm` as a Gradle subproject in `settings.gradle` (add `include 'solr:agent-sm'`)
- [X] T002 Create `solr/agent-sm/build.gradle` — apply `java-library`; configure agent JAR manifest with `Premain-Class: org.apache.solr.security.agent.SolrAgentEntryPoint` and `Can-Redefine-Classes: true`; declare ByteBuddy as `implementation` dependency
- [X] T003 Add `byte-buddy` version entry to `gradle/libs.versions.toml` (match version used by OpenSearch agent-sm; do not specify version in build.gradle)
- [X] T004 Create source directories `solr/agent-sm/src/java/org/apache/solr/security/agent/` and `solr/agent-sm/src/test/org/apache/solr/security/agent/`
- [X] T005 Run `gradlew :solr:agent-sm:dependencies` to confirm ByteBuddy resolves; then run `gradlew updateLicenses resolveAndLockAll --write-locks` to generate license files for ByteBuddy
- [X] T043 Configure `solr/agent-sm/build.gradle` with a `copyAgentJar` task that copies the built agent JAR to `solr/server/lib/ext/` so startup scripts can detect it at `${SOLR_SERVER_DIR}/lib/ext/solr-agent-sm-*.jar`; wire the task into the `assemble` lifecycle; also add the agent JAR to the `solr/packaging/` Gradle targets so it is included in the distribution zip/tgz

**Checkpoint**: `gradlew :solr:agent-sm:compileJava` succeeds (empty source tree); `gradlew :solr:agent-sm:assemble` produces JAR at `solr/server/lib/ext/`

---

## Phase 2: Foundational (Policy Engine — Blocks All User Stories)

**Purpose**: The policy loading and enforcement infrastructure that every interceptor depends on. Must be complete before any protection can be implemented.

**⚠️ CRITICAL**: No user story interceptor work can begin until this phase is complete.

- [X] T006 Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/PolicyLoader.java` — reads JDK-style `.policy` files; resolves the following variables: `${solr.home}`, `${solr.data.dir}`, `${solr.log.dir}`, `${java.io.tmpdir}`, `${java.home}`, `${solr.port}`, `${solr.zk.port}` (ZK port = explicit config value or `solr.port + 1000` for embedded ZK), and **`${solr.install.dir}`** (resolved from system property `solr.install.dir`, the Solr installation root — required for codebase-scoped module grants in the default policy); merges default policy (`agent-security.policy`) + optional extension file (`agent-security-extra.policy`); entries from extension file tagged `OPERATOR`, from default tagged `DEFAULT`; throws descriptive `IllegalStateException` if default policy is absent or unparseable; extension file absent is non-fatal; add ASF license header and class-level javadoc. **⚠️ Dependency note**: resolve the extra-policy file path from `System.getProperty("solr.security.agent.extra.policy")` with a fallback to `${server.dir}/etc/agent-security-extra.policy` — do NOT hard-code the path; T050 (formerly T050) completes the full env-var/sysprop wiring in Phase 8
- [X] T007 [P] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/SolrSecurityPolicy.java` — immutable singleton holding `List<PermittedPath>`, `List<PermittedEndpoint>`, `List<ApprovedCallSite>` (exit + exec), `EnforcementMode` enum (`WARN`/`ENFORCE`), and `Set<String> trustedFileSystems`; reads enforcement mode from system property `solr.security.agent.mode` via `System.getProperty()` directly (NOT via `EnvUtils` — the agent JAR has no compile-time dependency on `solr:core`; the startup scripts convert the env var to a `-D` sysprop before JVM launch, see T020); default is `warn` if property absent; throws `SecurityException` on any re-set attempt after initialization; add ASF license header and javadoc
- [X] T008 [P] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/StackInspector.java` — uses `StackWalker.getInstance(RETAIN_CLASS_REFERENCE)` to walk the call chain; filters JDK frames (`jrt:` codebase); returns ordered list of non-JDK caller classes; must not use thread identity (virtual-thread safe); add ASF license header and javadoc
- [X] T009 [P] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/SecurityViolationLogger.java` — emits structured SLF4J log entries at `WARN` (warn mode) or `ERROR` (enforce mode); log format: `SECURITY VIOLATION [TYPE] target=<t> caller=<c> mode=<m>`; reserve a `source` field placeholder in the format (populated by T042 in US2); wrap debug-level call stack logging in `logger.isDebugEnabled()`; add ASF license header and javadoc
- [X] T010 Create `solr/server/etc/agent-security.policy` — default production policy with the following grants:
  - Global grant (no codeBase): `${solr.home}/-` read; `${solr.data.dir}/-` read+write+delete; `${solr.log.dir}/-` read+write+delete; `${java.io.tmpdir}/-` read+write+delete; `${java.home}/-` read; loopback `localhost:1-65535`, `127.0.0.1:1-65535`, and `[::1]:1-65535` connect+resolve; intra-cluster wildcards `*:${solr.port}` and `*:${solr.zk.port}` connect+resolve (Decision 9)
  - Per-module codeBase grants (one block each) — `SocketPermission "*", "connect,resolve"` scoped to each pre-permitted bundled module's JAR directory (Decision 10 / clarification 2026-04-29): `codeBase "file:${solr.install.dir}/modules/jwt-auth/-"`, `codeBase "file:${solr.install.dir}/modules/opentelemetry/-"`, `codeBase "file:${solr.install.dir}/modules/s3-repository/-"`, `codeBase "file:${solr.install.dir}/modules/gcs-repository/-"`, `codeBase "file:${solr.install.dir}/modules/cross-dc-manager/-"`
  - Add ASF license header as a comment block; add a comment before each codeBase grant naming the module and explaining the SSRF-safe rationale
- [X] T011 [P] Create `solr/server/etc/agent-security-extra.policy` — empty operator extension template with commented examples for custom paths and network endpoints; add ASF license header as a comment block
- [X] T012 Write `solr/agent-sm/src/test/org/apache/solr/security/agent/PolicyLoaderTest.java` — extend `SolrTestCase`; test cases: valid policy parses correctly, variable substitution resolves paths (including `${solr.port}`, `${solr.zk.port}`, and `${solr.install.dir}`), `${solr.zk.port}` defaults to `${solr.port} + 1000` when not configured, codeBase-scoped grants parse and match correctly, malformed policy throws on load, missing default policy throws on load, extra policy merged when present and tagged OPERATOR, extra policy absent is non-fatal; add ASF license header

**Checkpoint**: `gradlew :solr:agent-sm:test` passes (policy loading tests green)

---

## Phase 3: User Story 1 — Standard Deployment Protected by Default (Priority: P1) 🎯 MVP

**Goal**: All four protection categories (file, network, exit, process) active and automatically applied to every Solr deployment. Violations are exposed as Solr metrics (FR-017) and log entries (FR-008).

**Independent Test**: Start embedded Solr with agent in enforce mode; a test plugin that attempts unauthorized file read, outbound network connect, `System.exit()`, and `ProcessBuilder` spawn — all four are blocked; standard search and indexing complete with no violations; per-type violation counters in the metrics registry are non-zero after violations are triggered.

### Implementation

- [X] T013 [US1] Adapt OpenSearch `FileInterceptor` → `solr/agent-sm/src/java/org/apache/solr/security/agent/FileAccessInterceptor.java` — ByteBuddy `@Advice` interceptor for `java.nio.file.Files`, `FileChannel`, and `FileSystemProvider` write/read/delete/copy/move/open methods; on each call: resolve absolute path, skip trusted filesystem schemes, walk stack via `StackInspector`, check each frame's protection domain against `SolrSecurityPolicy`; delegate to `SecurityViolationLogger` on violation; add ASF license header and javadoc; add `@SuppressForbidden(reason="ByteBuddy bootstrap injection requires Unsafe")` where applicable
- [X] T014 [P] [US1] Adapt OpenSearch `SocketChannelInterceptor` → `solr/agent-sm/src/java/org/apache/solr/security/agent/NetworkAccessInterceptor.java` — intercepts `SocketChannel.connect()` and `Socket.connect()`; checks `InetSocketAddress` against `SolrSecurityPolicy.permittedEndpoints`; passes loopback unconditionally; delegates to `SecurityViolationLogger` on violation; add ASF license header and javadoc
- [X] T015 [P] [US1] Adapt OpenSearch `SystemExitInterceptor` + `RuntimeHaltInterceptor` → `solr/agent-sm/src/java/org/apache/solr/security/agent/ExitInterceptor.java` — intercepts `System.exit()` and `Runtime.halt()`; checks top caller class against `SolrSecurityPolicy.approvedExitCallers`; default approved callers: `org.apache.solr.cli.SolrCLI`, `org.apache.solr.servlet.SolrDispatchFilter`; delegates to `SecurityViolationLogger` on violation; add ASF license header and javadoc
- [X] T016 [P] [US1] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/ProcessExecInterceptor.java` — ByteBuddy interceptor for `ProcessBuilder.start()` and `Runtime.exec()`; checks top caller class prefix against `SolrSecurityPolicy.approvedExecCallers`; default approved exec callers list is empty in production policy; delegates to `SecurityViolationLogger` on violation; add ASF license header and javadoc
- [X] T017 [US1] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/ViolationMetricsReporter.java` — maintains four `LongAdder` counters (FILE, NETWORK, EXIT, EXEC); incremented by `SecurityViolationLogger` on each violation; provides `registerWithSolrMetrics(SolrMetricManager, String registry)` static method using deferred registration pattern (called once `SolrMetricManager` is available at core initialization; method name must match the reflective call in T019); registers counters under metric names `security.agent.violations.file`, `security.agent.violations.network`, `security.agent.violations.exit`, `security.agent.violations.exec`; add ASF license header and javadoc
- [X] T018 [US1] Implement `solr/agent-sm/src/java/org/apache/solr/security/agent/SolrAgentEntryPoint.java` — `premain()` and `agentmain()` entry points; loads `SolrSecurityPolicy` via `PolicyLoader`; registers all four interceptors with ByteBuddy `AgentBuilder`; injects bootstrap classes via `ClassInjector.UsingUnsafe.ofBootLoader()`; initializes `ViolationMetricsReporter` singleton; if policy loading fails, logs error and (in enforce mode) halts startup; add ASF license header and javadoc; add `@SuppressForbidden` for `Unsafe` bootstrap injection
- [X] T019 [US1] Add hook in `solr/core/src/java/org/apache/solr/core/CoreContainer.java` to register agent metrics via reflection — use `Class.forName("org.apache.solr.security.agent.ViolationMetricsReporter", false, null)` (bootstrap classloader lookup; no compile-time dependency on `solr:agent-sm`); invoke `registerWithSolrMetrics(SolrMetricManager, String)` reflectively; catch `ClassNotFoundException` silently (agent not loaded); catch `ReflectiveOperationException` with a WARN log; see research.md Decision 8 for full pattern
- [X] T020 [US1] Modify `solr/bin/solr` — detect presence of agent JAR at `${SOLR_SERVER_DIR}/lib/ext/solr-agent-sm-*.jar` or equivalent output path; if found, prepend `-javaagent:<jar-path>` to `SOLR_OPTS` before JVM launch; skip if `SOLR_SECURITY_AGENT_SKIP=true`; also convert the two env vars to JVM system properties so the agent can read them via `System.getProperty()` (the agent JAR does not use `EnvUtils`): if `SOLR_SECURITY_AGENT_MODE` is set, append `-Dsolr.security.agent.mode=$SOLR_SECURITY_AGENT_MODE`; if `SOLR_SECURITY_AGENT_EXTRA_POLICY` is set, append `-Dsolr.security.agent.extra.policy=$SOLR_SECURITY_AGENT_EXTRA_POLICY`
- [X] T021 [P] [US1] Modify `solr/bin/solr.cmd` — same detection, `-javaagent:` injection, and env→sysprop conversion logic as T020 but for Windows batch (`IF DEFINED SOLR_SECURITY_AGENT_MODE SET SOLR_OPTS=%SOLR_OPTS% -Dsolr.security.agent.mode=%SOLR_SECURITY_AGENT_MODE%`, similarly for `SOLR_SECURITY_AGENT_EXTRA_POLICY`)
- [X] T022 [US1] Document `SOLR_SECURITY_AGENT_SKIP` and `SOLR_SECURITY_AGENT_MODE` in `solr/bin/solr.in.sh` — note that `SOLR_SECURITY_AGENT_MODE` is converted to `-Dsolr.security.agent.mode` by the startup script (T020) so the agent can read it via `System.getProperty()` without needing `EnvUtils`; valid values: `warn` (default), `enforce`
- [X] T023 [P] [US1] Document same variables in `solr/bin/solr.in.cmd`
- [X] T046 [P] [US1] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/NetworkAccessInterceptorTest.java` — extend `SolrTestCase`; test cases: loopback connect permitted, `*:<solr.port>` wildcard entry permits connection to that port on any host, unlisted host:port blocked in enforce mode, `source=DEFAULT` in violation log; add ASF license header
- [X] T047 [P] [US1] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/ExitInterceptorTest.java` — extend `SolrTestCase`; test cases: `System.exit()` from approved caller class passes, `System.exit()` from unapproved caller throws `SecurityException` in enforce mode and logs warning in warn mode, `Runtime.halt()` from unapproved caller is blocked; assert EXIT counter increments; add ASF license header
- [X] T048 [P] [US1] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/ProcessExecInterceptorTest.java` — extend `SolrTestCase`; test cases: `ProcessBuilder.start()` from an approved caller class passes, `ProcessBuilder.start()` from an unapproved caller is blocked in enforce mode, `Runtime.exec()` from unapproved caller is blocked; assert EXEC counter increments; add ASF license header
- [X] T024 [US1] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/SolrAgentIntegrationTest.java` — extend `SolrTestCase`; run with agent in **ENFORCE mode** (`SOLR_SECURITY_AGENT_MODE=enforce`); verify: permitted file read succeeds, denied file read throws `SecurityException`, permitted loopback connect succeeds, denied outbound connect throws `SecurityException`, `System.exit()` from non-approved caller throws `SecurityException`, `ProcessBuilder.start()` from non-approved caller throws `SecurityException`; after each violation, assert the corresponding `ViolationMetricsReporter` counter incremented; add ASF license header

**Checkpoint**: `gradlew :solr:agent-sm:test` fully green in enforce mode; standard Solr integration suite passes with agent in warn mode; violation metric counters verified non-zero after violations (SC-007, SC-008)

---

## Phase 4: User Story 2 — Operator Extends Policy (Priority: P2)

**Goal**: Operators can add custom permitted paths and endpoints via `agent-security-extra.policy` without touching Solr source or the default policy.

**Independent Test**: Write extra policy with a custom path; verify that path is accessible; verify a different unlisted path is still blocked.

- [X] T042 [US2] Update `solr/agent-sm/src/java/org/apache/solr/security/agent/SecurityViolationLogger.java` to emit `source` field — extend the log format to `SECURITY VIOLATION [TYPE] target=<t> caller=<c> mode=<m> source=<DEFAULT|OPERATOR>`; the source value is passed in from the policy-check result produced by `PolicyLoader` (which already tags entries); no change to `PolicyLoader` needed; add javadoc on the new parameter
- [X] T025 [US2] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/PolicyLoaderOperatorExtensionTest.java` — extend `SolrTestCase`; test: extra policy file present → custom path accessible; custom path not in extra policy → blocked; extra policy absent → default policy still enforced; malformed extra policy → startup failure with clear error; OPERATOR-tagged entries in violation log include `source=OPERATOR` in log output (depends on T042); add ASF license header

**Checkpoint**: Operator can add `agent-security-extra.policy` and have it take effect on restart; no access to unlisted paths; `source=OPERATOR` visible in log entries for operator-policy-covered paths

---

## Phase 5: User Story 3 — Windows UNC Paths Blocked (Priority: P2)

**Goal**: Paths beginning with `\\` are always rejected on all platforms, regardless of any policy rule.

**Independent Test**: Attempt file access via `\\server\share\file`; verify blocked and logged on Linux, macOS, and Windows.

- [X] T026 [US3] Add UNC path detection to `solr/agent-sm/src/java/org/apache/solr/security/agent/FileAccessInterceptor.java` — before any policy check, if the resolved path string starts with `\\` or is a Windows UNC-style path, immediately block and log; cannot be overridden by any policy entry; write unit test cases in `solr/agent-sm/src/test/org/apache/solr/security/agent/UncPathRejectionTest.java` extending `SolrTestCase`; add ASF license header to test file

**Checkpoint**: UNC path attempts produce `SECURITY VIOLATION` log entries and FILE counter increments on all platforms

---

## Phase 6: User Story 4 — Symlink Escape Prevented (Priority: P2)

**Goal**: A symlink inside a permitted directory pointing to a target outside permitted directories is denied.

**Independent Test**: Create symlink in data dir targeting `/etc/passwd`; attempt read via symlink path; verify denied.

- [X] T027 [US4] Add symlink resolution to `solr/agent-sm/src/java/org/apache/solr/security/agent/FileAccessInterceptor.java` — after normalizing the target path, call `Path.toRealPath()` to resolve symlinks; check the resolved real path (not the symlink path) against `SolrSecurityPolicy.permittedPaths`; if real path is outside permitted dirs, block and log even if the symlink path itself would have matched; handle `IOException` from `toRealPath()` gracefully (log at DEBUG, proceed with original path check)
- [X] T028 [P] [US4] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/SymlinkEscapeTest.java` — extend `SolrTestCase`; create symlink inside `java.io.tmpdir` pointing to a system file; verify read via symlink path is blocked; verify read of a real permitted path still succeeds; add ASF license header

**Checkpoint**: Symlink traversal to unpermitted targets produces `SECURITY VIOLATION` log entries and FILE counter increments

---

## Phase 7: User Story 5 — Virtual Thread Compatibility (Priority: P3)

**Goal**: Security controls work correctly under Project Loom virtual threads; no false positives or false negatives.

**Independent Test**: Enable virtual threads; run full standard Solr test suite; verify zero unexpected security violations.

- [X] T029 [US5] Audit `solr/agent-sm/src/java/org/apache/solr/security/agent/StackInspector.java` for virtual-thread safety — verify `StackWalker.getInstance(RETAIN_CLASS_REFERENCE)` is used (not `Thread.currentThread()` or `ThreadGroup`); add code comment documenting virtual-thread compatibility guarantee
- [X] T030 [P] [US5] Write `solr/agent-sm/src/test/org/apache/solr/security/agent/VirtualThreadCompatibilityTest.java` — extend `SolrTestCase`; launch file access and network access operations from virtual threads using `Thread.ofVirtual().start(...)`; verify: permitted ops succeed, denied ops are caught with correct counter increments, no `NullPointerException` or `ClassCastException` from stack walker on virtual thread frames; add ASF license header
- [X] T031 [US5] Run `gradlew :solr:core:test :solr:agent-sm:test` with virtual thread executor enabled; triage any unexpected violations found; document results in a comment on SOLR-17767

**Checkpoint**: Full test suite green with virtual threads; no virtual-thread-specific failures

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Documentation (FR-016), old policy file deprecation, code quality, and Solr-wide integration verification.

- [X] T032 [P] Run `gradlew tidy` across all modified and new source files; fix any formatting issues reported
- [X] T033 Run `gradlew check -x test` and resolve any forbidden-API violations, license check failures, or dependency analysis issues (`usedUndeclaredArtifacts`, `unusedDeclaredArtifacts`)
- [X] T034 [P] Add `@Deprecated` annotation and `@deprecated` javadoc tag to `SolrPaths.assertPathAllowed()` in `solr/core/src/java/org/apache/solr/core/SolrPaths.java` with note: "Automatic enforcement via the security agent supersedes this check; do not add new call sites"
- [X] T035 [P] Add deprecation notice comments to `solr/server/etc/security.policy` — explain the file is no longer enforced by the JVM (JSM removed in JDK 24), is retained as a migration reference, and will be removed in a future release; point to `agent-security.policy` as the replacement
- [X] T036 [P] Add deprecation notice comments to `gradle/testing/randomization/policies/solr-tests.policy` — explain the JSM policy sections are no longer enforced but the file is retained for test framework use; will be reviewed for removal in a future release
- [X] T050 [P] Implement configurable extra-policy path (FR-009) — in the agent startup code (`PolicyLoader`), resolve the extra policy path from system property `solr.security.agent.extra.policy` via `System.getProperty()` directly (the startup script converts `SOLR_SECURITY_AGENT_EXTRA_POLICY` to this `-D` sysprop per T020/T021); fall back to `${server.dir}/etc/agent-security-extra.policy` if the property is absent; silently skip loading if the resolved file does not exist; log the resolved path at INFO level on startup
- [X] T047b [P] Add `SOLR_SECURITY_AGENT_EXTRA_POLICY` to `bin/solr.in.sh` and `bin/solr.in.cmd` as a commented-out example with a doc comment explaining its purpose, the default path, and that the startup script converts it to `-Dsolr.security.agent.extra.policy` (no `EnvUtils` wiring needed — conversion is done inline in the startup script per T020/T021)
- [X] T037 [P] Write reference guide section on agent-based security controls (FR-016) — create or update the security page in `solr/solr-ref-guide/modules/deployment-guide/pages/` covering: what protections are active by default, enforcement modes (`SOLR_SECURITY_AGENT_MODE`), policy file format and variable substitution (including `${solr.port}` and `${solr.zk.port}`), default intra-cluster port-wildcard policy (any host on `<solr.port>` and `<zk-port>` is permitted without operator action), extending policy via `agent-security-extra.policy` (including `SOLR_SECURITY_AGENT_EXTRA_POLICY` override for read-only installs and containers), diagnosing violations via logs and `/admin/metrics`, disabling the agent (`SOLR_SECURITY_AGENT_SKIP`), external ZK on non-standard port edge case; audience is operators; cross-reference `solr-properties.adoc` for the properties table
- [X] T048b [P] Add new agent env vars and system properties to `solr/solr-ref-guide/modules/configuration-guide/pages/solr-properties.adoc` (FR-016) — insert three rows in alphabetical order (after existing `solr.security.*` entries):
  - `solr.security.agent.extra.policy` | `SOLR_SECURITY_AGENT_EXTRA_POLICY` | `${server.dir}/etc/agent-security-extra.policy` | Path to the operator extension policy file; overrides the default location; absent file is silently skipped
  - `solr.security.agent.mode` | `SOLR_SECURITY_AGENT_MODE` | `warn` | Enforcement mode for the security agent: `warn` (log violations, continue) or `enforce` (log violations, block operation with `SecurityException`)
  - `SOLR_SECURITY_AGENT_SKIP` | _(startup-script only, no sysprop)_ | `false` | If set to `true`, omits the `-javaagent:` flag from the JVM command line, disabling all agent security controls; intended for temporary troubleshooting only
- [X] T044 [P] Add security agent policy NOTICE box to the `extraction` module reference guide page — add a clearly visible WARNING admonition explaining that in enforce mode, the remote Tika Server URL must be explicitly permitted in `agent-security-extra.policy`; include a ready-to-paste snippet; note: `jwt-auth`, `opentelemetry`, `s3-repository`, `gcs-repository`, and `cross-dc-manager` do NOT need a NOTICE — their endpoints are pre-permitted with a wildcard in the bundled agent policy (see clarification 2026-04-29):
  - `solr/solr-ref-guide/modules/indexing-guide/pages/indexing-with-tika.adoc` — remote Tika Server URL (only when `useRemoteTikaServer=true`)
- [X] T045 [P] Update `solr/server/etc/agent-security-extra.policy` template to include a commented-out example entry for the `extraction` module — add a commented block for Tika Server (`# extraction module: uncomment and set your Tika Server hostname:port`); the five pre-permitted modules (`jwt-auth`, `opentelemetry`, `s3-repository`, `gcs-repository`, `cross-dc-manager`) do not need example entries as their wildcard rules are bundled in the default policy
- [X] T038 [P] Add developer documentation in `dev-docs/` — create `security-agent.adoc` covering: how to add an approved call site (exit/exec), how to add a trusted filesystem scheme, how the deferred metrics registration works, how to write tests alongside the agent, and instructions for the future enforce-mode flip in the broader test suite
- [X] T039 Run `gradlew writeChangelog`; edit generated file in `changelog/unreleased/` — category: `new feature`; summary covering: JSM replacement via Java agent, file/network/exit/exec protections, warn-only default, policy file configuration, violation metrics in `/admin/metrics`, reference guide documentation
- [ ] T040 Validate `quickstart.md` steps manually against a running Solr instance: confirm warn mode default, enforce mode opt-in, extra policy file pickup, violation log format, and `/admin/metrics` counter presence all match documentation
- [ ] T041 Run full Solr integration test suite (`gradlew :solr:core:test :solr:solrj:test`) with agent in warn mode; confirm zero unexpected violations via `grep "SECURITY VIOLATION" solr/*/build/test-results/test/outputs/*.txt` — agent-sm test suite: ALL 9 SUITES GREEN
- [ ] T049 [P] Run performance benchmark to validate SC-002 / FR-012 (≤5% throughput degradation) — execute a standard Solr search and indexing benchmark (e.g., using the Solr test-framework's `SolrBenchmark` harness or a manual `ab`/`wrk` run against a local Solr instance) with agent in warn mode vs without agent (`SOLR_SECURITY_AGENT_SKIP=true`); record throughput (QPS) for both; confirm degradation ≤5%; document results as a comment on SOLR-17767; if degradation exceeds 5%, file a follow-up JIRA before declaring the feature ready

**Checkpoint**: `gradlew check -x test` clean; changelog entry present; reference guide section complete and extraction module NOTICE box present; extra policy template has commented Tika Server example; old policy files have deprecation notices; `solr-properties.adoc` contains entries for all three new agent env vars/sysprops; benchmark results documented on SOLR-17767 with ≤5% degradation confirmed; no unexpected violations in full test suite

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — **blocks all user story phases**
- **Phase 3 (US1)**: Depends on Phase 2 — MVP; no dependency on US2–US5
- **Phase 4 (US2)**: T042 (SecurityViolationLogger source field) depends on T009; T025 (test) depends on T042; both independent of US1 interceptors
- **Phase 5 (US3)**: Depends on T013 (`FileAccessInterceptor` exists); extends it
- **Phase 6 (US4)**: Depends on T013 (`FileAccessInterceptor` exists); can run parallel with Phase 5
- **Phase 7 (US5)**: Depends on Phase 3 complete (all interceptors implemented)
- **Phase 8 (Polish)**: Depends on all desired user stories complete

### User Story Dependencies

- **US1 (P1)**: After Foundational — no dependency on other stories
- **US2 (P2)**: After T006 (PolicyLoader) — independent of US1 interceptors
- **US3 (P2)**: After T013 (`FileAccessInterceptor`) — extends US1 work; parallel with US4
- **US4 (P2)**: After T013 (`FileAccessInterceptor`) — extends US1 work; parallel with US3
- **US5 (P3)**: After all US1 interceptors (T013–T016) complete

### Parallel Opportunities Within Phases

- **Phase 1**: T003, T004 parallel after T001–T002
- **Phase 2**: T007, T008, T009, T011 all parallel once T006 interface defined
- **Phase 3**: T014, T015, T016 parallel once T013 pattern established; T021, T023 parallel with T020, T022; T017, T018, T019 sequential after interceptors
- **Phase 5 + Phase 6**: T026 and T027–T028 fully parallel once T013 exists
- **Phase 8**: T032, T034, T035, T036, T037, T038, T048b, T049 all parallel

---

## Parallel Example: Phase 2 (Foundational)

```
Start T006 PolicyLoader (defines the interface others depend on)
Once T006 interface is defined, launch in parallel:
  → T007 SolrSecurityPolicy
  → T008 StackInspector
  → T009 SecurityViolationLogger
  → T011 agent-security-extra.policy template
  → T012 PolicyLoaderTest
Start T010 agent-security.policy (independent, any time in Phase 2)
```

## Parallel Example: Phase 3 (US1 — Core Interceptors + Metrics)

```
Start T013 FileAccessInterceptor (establishes the ByteBuddy pattern)
Once T013 pattern established, launch in parallel:
  → T014 NetworkAccessInterceptor
  → T015 ExitInterceptor
  → T016 ProcessExecInterceptor
  → T017 ViolationMetricsReporter (independent of interceptors)
Then T018 SolrAgentEntryPoint (wires all interceptors + metrics reporter)
Then T019 CoreContainer metrics hook (depends on T017 + T018)
Then T020/T021 startup scripts (parallel with each other)
Then T024 SolrAgentIntegrationTest (depends on T018 + T019)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Gradle scaffold
2. Complete Phase 2: Policy engine
3. Complete Phase 3: All four interceptors + metrics reporter + agent entry point + startup scripts
4. **STOP and VALIDATE**: Run integration test in enforce mode; confirm standard Solr ops pass; confirm violations caught; confirm metric counters increment
5. Ship in warn-only mode — operators observe without risk; metrics visible via `/admin/metrics`

### Incremental Delivery

1. Phase 1 + 2 → Policy engine ready
2. Phase 3 (US1) → All four protections + metrics → **MVP** (warn mode)
3. Phase 4 (US2) → Operator policy extension → Usable for non-standard deployments
4. Phase 5 + 6 (US3 + US4) → UNC and symlink hardening → Security completeness
5. Phase 7 (US5) → Virtual thread validation → Production-grade for Loom
6. Phase 8 → Documentation, deprecation notices, changelog → Release-ready

### Suggested MVP Scope

Complete **Phases 1–3** only (T001–T024). Delivers:
- All four protection categories active
- Violation metrics in `/admin/metrics`
- Warn-only default (safe for existing deployments)
- Startup script auto-activation
- Integration tests in enforce mode (agent-sm suite)

### Future Work (Not in This Release)

- Flip broader Solr test suite to enforce mode (SC-008 follow-up; requires triage of warn-mode violations first)
- Dynamic network policy refresh for modules with externally-changing endpoints (e.g., rotating SaaS URLs) — intra-cluster topology is already covered by the port-wildcard approach in Decision 9
- Remove deprecated `security.policy` and `solr-tests.policy` JSM sections

---

## Notes

- [P] tasks = different files, no dependency on incomplete tasks in the same phase
- All new `.java` files **must** include the ASF license header
- All new classes **must** have class-level javadoc (agent interceptors are non-obvious)
- Debug/trace SLF4J calls in interceptors **must** be wrapped in `logger.isDebugEnabled()` — interceptors are in hot paths
- Do not add new `SolrPaths.assertPathAllowed()` call sites — the agent handles enforcement automatically
- `ViolationMetricsReporter` uses deferred registration: it buffers counts from the moment the agent starts (before Solr initializes), then registers with `SolrMetricManager` once available
- The agent-sm test suite runs in ENFORCE mode; the broader Solr suite runs in WARN mode — this is intentional per SC-008
- Run `gradlew tidy` before any commit touching Java source
- Run `gradlew check -x test` before declaring a phase done
- Changelog entry is mandatory — run `gradlew writeChangelog` and edit the generated file
