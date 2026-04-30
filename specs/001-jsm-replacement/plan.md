# Implementation Plan: Java Security Manager Replacement

**Branch**: `15868-java-security-manager` | **Date**: 2026-04-28 | **Spec**: [spec.md](spec.md)
**JIRA**: SOLR-17767

---

## Summary

Solr's Java Security Manager protection disappeared when JDK 24 removed the JSM API. This plan delivers a replacement using a Java agent (forked/adapted from the OpenSearch `agent-sm` module, Apache 2.0 licensed) that intercepts file access, network connections, `System.exit()`, and process spawning at the bytecode level via ByteBuddy instrumentation. The agent is activated automatically via `-javaagent:` in Solr's startup scripts and enforces a policy derived from Solr's configured directory layout. The initial release ships in **warn-only mode** to allow operator policy tuning before enabling blocking enforcement.

---

## Technical Context

**Language/Version**: Java 21 (minimum), must work on Java 24+ without deprecated APIs
**Primary Dependencies**: ByteBuddy (for bytecode instrumentation), forked from `opensearch-project/OpenSearch libs/agent-sm` (Apache 2.0)
**Storage**: No persistent storage; policy loaded from `.policy` files at JVM startup
**Testing**: JUnit 4 via Solr's randomized test framework; `SolrTestCase` / `SolrCloudTestCase` base classes
**Target Platform**: Linux, macOS, Windows (cross-platform; OS-level hardening is complementary only)
**Project Type**: Standalone Gradle subproject (`solr/agent-sm/`) + startup script integration
**Performance Goals**: ≤5% throughput degradation on standard search/indexing benchmarks
**Constraints**: Virtual-thread compatible; no `java.security.SecurityManager` API; no deprecated/removed JVM APIs
**Scale/Scope**: Applies to all code in the Solr JVM, including all loaded plugins

---

## Constitution Check (AGENTS.md)

*Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| Apache License on all new source files | REQUIRED | Every new `.java` file and config file must include the ASF license header |
| All dependency versions in `gradle/libs.versions.toml` | REQUIRED | ByteBuddy version must be declared there, never in `build.gradle` directly |
| Run `gradlew updateLicenses resolveAndLockAll --write-locks` after adding ByteBuddy | REQUIRED | ByteBuddy is a new dependency; license files must be generated |
| Run `gradlew tidy` before committing Java source | REQUIRED | Code formatting enforced by Spotless |
| Run `gradlew check -x test` before declaring done | REQUIRED | Forbidden API checks, license checks, etc. |
| New test suites extend `SolrTestCase` (not `SolrTestCaseJ4`) | REQUIRED | `SolrTestCaseJ4` is deprecated for new tests |
| New classes must have javadoc | REQUIRED | Agent interceptor classes are non-obvious; javadoc is especially important |
| Wrap debug/trace log calls in `logger.isDebugEnabled()` | REQUIRED | Security interceptors are in hot paths; logging guards are essential |
| Call `coreContainer.assertPathAllowed()` for user-supplied paths | N/A | This feature replaces that pattern; new code must NOT add new call sites |
| Use `EnvUtils` to read system properties | REQUIRED | env var `SOLR_SECURITY_AGENT_MODE` → sysprop `solr.security.agent.mode` must be read via `EnvUtils` (auto-conversion is the Solr convention) |
| Use `@SuppressForbidden` for any `com.sun.*` API | LIKELY REQUIRED | ByteBuddy's bootstrap classloader injection may require `Unsafe`; needs `@SuppressForbidden` with reason |
| Changelog entry via `gradlew writeChangelog` | REQUIRED | Add after JIRA/PR is assigned |
| Reference guide update | REQUIRED | Security reference guide page needs a new section on agent-based controls |

**No constitution violations** — all applicable principles are satisfiable within the planned approach.

---

## Project Structure

### Documentation (this feature)

```text
specs/001-jsm-replacement/
├── plan.md              # This file
├── spec.md              # Feature specification
├── research.md          # Phase 0 decisions
├── data-model.md        # Security policy entity model
├── quickstart.md        # Operator quickstart guide
├── contracts/
│   └── policy-file-format.md   # Policy file syntax contract
└── tasks.md             # Task list (52 tasks across 8 phases)
```

### Source Code Layout

```text
solr/agent-sm/
├── build.gradle                         # Standalone subproject build: produces agent-sm.jar with premain manifest
├── src/
│   ├── java/org/apache/solr/security/agent/
│   │   ├── SolrAgentEntryPoint.java     # premain() + agentmain(); configures ByteBuddy
│   │   ├── SolrSecurityPolicy.java      # Immutable policy singleton; loaded at startup
│   │   ├── PolicyLoader.java            # Reads .policy files; variable substitution (incl. ${solr.port}, ${solr.zk.port})
│   │   ├── FileAccessInterceptor.java   # Intercepts NIO file operations; UNC + symlink handling
│   │   ├── NetworkAccessInterceptor.java # Intercepts SocketChannel.connect() / Socket.connect()
│   │   ├── ExitInterceptor.java         # Intercepts System.exit() / Runtime.halt()
│   │   ├── ProcessExecInterceptor.java  # Intercepts ProcessBuilder.start() / Runtime.exec()
│   │   ├── StackInspector.java          # StackWalker-based call chain analysis; virtual-thread safe
│   │   ├── SecurityViolationLogger.java # Structured violation log emitter (type, target, caller, source)
│   │   └── ViolationMetricsReporter.java # LongAdder counters per type; bootstrap-safe deferred registration
│   └── test/org/apache/solr/security/agent/
│       ├── PolicyLoaderTest.java
│       ├── FileAccessInterceptorTest.java   # embedded in T026 (UNC) and T027-T028 (symlink)
│       ├── NetworkAccessInterceptorTest.java # T046
│       ├── ExitInterceptorTest.java          # T047
│       ├── ProcessExecInterceptorTest.java   # T048
│       ├── PolicyLoaderOperatorExtensionTest.java  # T025
│       ├── UncPathRejectionTest.java         # T026
│       ├── SymlinkEscapeTest.java            # T028
│       ├── VirtualThreadCompatibilityTest.java # T030
│       └── SolrAgentIntegrationTest.java    # T024 — full-stack test with embedded Solr in enforce mode

solr/core/src/java/org/apache/solr/core/
└── SolrPaths.java                       # EXISTING: retain; add @deprecated on assertPathAllowed

solr/server/etc/
├── agent-security.policy                # NEW: default production policy
└── agent-security-extra.policy          # NEW: empty operator extension file (template)

solr/bin/
├── solr                                 # MODIFY: detect agent-sm.jar, append -javaagent:
├── solr.cmd                             # MODIFY: same for Windows
└── solr.in.sh                           # MODIFY: document SOLR_SECURITY_AGENT_MODE, SOLR_SECURITY_AGENT_SKIP, SOLR_SECURITY_AGENT_EXTRA_POLICY

solr/server/lib/ext/
└── solr-agent-sm-*.jar                  # NEW: agent JAR copied here by Gradle build; detected by startup scripts

solr/packaging/src/main/package/
└── (MODIFY)                             # Add agent JAR to distribution zip/tgz

solr/server/resources/log4j2.xml
└── (no change needed)                   # SecurityViolationLogger uses SLF4J → existing logging

gradle/libs.versions.toml               # ADD: byte-buddy version
```

---

## Phase 0: Research ✅

**Output**: [research.md](research.md)

Key decisions made:
1. **Fork OpenSearch `agent-sm`** (Apache 2.0; same problem; same tech stack)
2. **New subproject `solr/agent-sm/`** (agent must be a standalone JAR; not a Solr module loaded inside the JVM)
3. **JDK-style `.policy` files** with Solr variable substitution
4. **Warn-only default** → enforce mode via opt-in config
5. **Auto-activation** via startup script detection of agent JAR
6. **Retain `assertPathAllowed`** as defense-in-depth; deprecate for new callers
7. **Add `ProcessExecInterceptor`** (not in OpenSearch's agent; needed for Solr)

---

## Phase 1: Design & Contracts ✅

### Data Model

See [data-model.md](data-model.md) for full entity definitions:
- `SecurityPolicy` — immutable root; loaded at startup
- `PermittedPath` — file system access rule (path + operations + recursive flag)
- `PermittedEndpoint` — outbound network rule (host + port range)
- `ApprovedCallSite` — allowed class for `System.exit()` or `ProcessBuilder`
- `SecurityViolation` — transient log record for blocked/warned operations

### Contracts

See [contracts/policy-file-format.md](contracts/policy-file-format.md):
- JDK-style `.policy` file syntax with Solr variable substitution
- Two-file approach: default policy + operator extension file
- Enforcement mode via env var `SOLR_SECURITY_AGENT_MODE` (auto-converted by `EnvUtils` to sysprop `solr.security.agent.mode`)

### Quickstart

See [quickstart.md](quickstart.md) — operator-facing guide covering:
- Default warn-only mode and log monitoring
- Switching to enforce mode
- Adding custom policy entries
- Diagnosing violations
- Disabling the feature (emergency only)

### Agent Context

See CLAUDE.md for current plan reference.

---

## Implementation Phases (for /speckit-tasks)

### Phase A: Subproject Scaffold & Policy Loading
1. Create `solr/agent-sm/build.gradle` — agent JAR with `premain`/`agentmain` manifest entries; register in `settings.gradle`; configure `copyAgentJar` task to copy output JAR to `solr/server/lib/ext/`; add agent JAR to `solr/packaging/` distribution targets
2. Add `byte-buddy` to `gradle/libs.versions.toml`; run `gradlew updateLicenses resolveAndLockAll --write-locks`
3. Implement `PolicyLoader` — reads and parses `.policy` files; Solr variable substitution
4. Implement `SolrSecurityPolicy` — immutable singleton; merges default + extra policy
5. Write unit tests for `PolicyLoader` (valid policy, malformed policy, missing file → startup failure)

### Phase B: File Access Enforcement
1. Adapt OpenSearch `FileInterceptor` → `FileAccessInterceptor` for Solr package structure
2. Add ASF license headers; write javadoc on all public classes
3. Handle UNC path rejection and symlink resolution
4. Write unit tests covering: permitted read, denied read, UNC path, symlink escape, warn vs. enforce mode

### Phase C: Network Access Enforcement
1. Adapt OpenSearch `SocketChannelInterceptor` → `NetworkAccessInterceptor`
2. Add `${solr.port}` and `${solr.zk.port}` variable support to `PolicyLoader`; include `*:${solr.port}` and `*:${solr.zk.port}` wildcard entries in the default policy (Decision 9 — covers all cluster nodes regardless of join order; ZK port = explicit config or `solr.port + 1000`)
3. Write unit tests covering: loopback permitted, port-wildcard entry permits any host on that port, unlisted host:port blocked

### Phase D: Exit & Process Enforcement
1. Adapt OpenSearch `SystemExitInterceptor` + `RuntimeHaltInterceptor` → `ExitInterceptor`
2. Implement `ProcessExecInterceptor` (new — not in OpenSearch agent) for `ProcessBuilder`
3. Define default approved exit callers; define empty default exec allow-list
4. Write unit tests for each interceptor

### Phase E: Agent Entry Point & Startup Integration
1. Implement `SolrAgentEntryPoint.premain()` — registers all ByteBuddy interceptors; loads policy
2. Modify `solr/bin/solr` and `solr/bin/solr.cmd` to detect agent JAR and add `-javaagent:`
3. Create `solr/server/etc/agent-security.policy` (default) and `agent-security-extra.policy` (empty template)
4. Write integration test: start embedded Solr with agent active; verify normal ops succeed; verify test-plugin violations are caught

### Phase F: Documentation & Cleanup
1. Add changelog entry via `gradlew writeChangelog`
2. Update reference guide: security section with new agent-based controls
3. `@Deprecated` on `SolrPaths.assertPathAllowed` with migration note
4. Run `gradlew tidy check -x test` and fix any issues
5. Update `dev-docs/` with developer notes on adding new approved call sites

---

## Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| ByteBuddy instrumentation breaks virtual threads | Low | OpenSearch already validated this; Solr tests will run with virtual threads enabled |
| Default policy too restrictive → test failures | Medium | All existing tests run with agent in warn mode first; violations triaged before switching to enforce |
| ByteBuddy bootstrap classloader injection rejected by JVM security flags | Low | Same technique used by OpenSearch 3.0 on Java 21/24; tested against Solr's startup flags |
| Performance regression >5% | Low | File access on hot search paths is read-only; agent interceptor is a lightweight stack walk |
| Plugin ecosystem breakage | Medium | Warn-only default gives plugin authors time to add policy extension documentation |
