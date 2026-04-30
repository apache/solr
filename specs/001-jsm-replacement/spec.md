# Feature Specification: Java Security Manager Replacement

**Feature Branch**: `15868-java-security-manager`
**Created**: 2026-04-28
**Status**: Draft
**JIRA**: SOLR-17767

## Background

Java Security Manager (JSM) was removed in JDK 24. Solr historically relied on it to enforce boundaries around file access, network traffic, process execution, and JVM lifecycle operations. Solr currently has a partial mitigation (`SolrPaths.assertPathAllowed`) for file paths, but it requires developers to call it explicitly — nothing enforces that they do. There is no equivalent mitigation for network access, Windows UNC path attacks, `System.exit()` abuse, or arbitrary process spawning.

This feature introduces automatic, cross-platform runtime security controls covering the highest-risk categories: file system access, outbound network connections, JVM shutdown prevention, and process execution restriction.

---

## User Scenarios & Testing

### User Story 1 — Standard Solr Deployment Is Protected by Default (Priority: P1)

A Solr operator upgrades to a Java 24+ JVM (or any supported JVM). Without any configuration change, the new security controls automatically enforce boundaries around file access, network calls, `System.exit()`, and process spawning. Plugins loaded by Solr are also subject to these controls.

**Why this priority**: This is the core goal of the feature. All existing Solr deployments should gain protection automatically — no manual action required.

**Independent Test**: Deploy a standard Solr instance. Load a test plugin that attempts to read a file outside Solr's permitted directories. Verify the read is blocked and a warning is logged. Verify all standard Solr operations (search, indexing, admin) continue to function normally.

**Acceptance Scenarios**:

1. **Given** a Solr server running with default configuration, **When** any code (including a plugin) attempts to read a file outside the permitted directory tree, **Then** the operation is denied and a log entry identifies the blocked path and the call origin.
2. **Given** a Solr server running with default configuration, **When** any code attempts to open an outbound network connection to a host not in the permitted list, **Then** the connection is refused and logged.
3. **Given** a Solr server running with default configuration, **When** plugin code calls `System.exit()`, **Then** the JVM does not terminate and the violation is logged.
4. **Given** a Solr server running with default configuration, **When** code attempts to spawn a child process from an unauthorized call site, **Then** the process is not created and the violation is logged.
5. **Given** a Solr server running with default configuration, **When** a standard search or indexing request is processed, **Then** all operations complete successfully with no security violations triggered.

---

### User Story 2 — Operator Extends Policy for Custom Deployment (Priority: P2)

A Solr operator runs a non-standard deployment: for example, a plugin that legitimately reads from an external NFS mount, or a connector that makes outbound HTTP calls to a specific SaaS endpoint. The operator needs to authorize these operations without disabling security controls globally.

**Why this priority**: Real-world deployments have legitimate needs beyond the default policy. Without this, operators would be forced to disable the feature entirely.

**Independent Test**: Configure a custom policy entry that permits a specific additional file path. Verify that access to that path succeeds. Verify that access to a different unauthorized path is still blocked.

**Acceptance Scenarios**:

1. **Given** an operator has added a permitted file path to the policy configuration, **When** Solr code reads a file at that path, **Then** the read succeeds.
2. **Given** an operator has permitted a specific outbound host, **When** a plugin connects to that host, **Then** the connection succeeds.
3. **Given** an operator has not permitted a path or host, **When** code attempts to access it, **Then** the access is denied regardless of any custom entries.

---

### User Story 3 — Windows UNC Paths Are Blocked (Priority: P2)

An attacker or misconfigured plugin constructs a Windows UNC path (`\\attacker-host\share\...`) to exfiltrate data or probe internal networks. On a Windows host, the OS would initiate an outbound SMB connection; on any host, UNC paths bypass standard path validation.

**Why this priority**: UNC paths are a well-known bypass vector for path-based access controls. The current `SolrPaths.assertPathAllowed` does not protect against them.

**Independent Test**: Attempt a file read using a UNC-style path. Verify the operation is denied and logged on all platforms.

**Acceptance Scenarios**:

1. **Given** any Solr deployment on any supported platform, **When** code attempts to access a path beginning with `\\`, **Then** the access is denied and logged.
2. **Given** a legitimate file read using a standard absolute path, **When** the path resolves within permitted directories, **Then** the read succeeds normally.

---

### User Story 4 — Symlink Escape Is Prevented (Priority: P2)

An attacker or plugin creates a symlink inside a permitted directory that points to a sensitive path outside it (e.g., `/etc/passwd`). Under the old JSM, following symlinks into unauthorized directories was blocked. This protection must continue.

**Why this priority**: Symlink attacks are a classic privilege escalation path that specifically targets path-based access controls.

**Independent Test**: Create a symlink inside the Solr data directory pointing to `/etc/passwd`. Attempt to read via the symlink. Verify the read is denied.

**Acceptance Scenarios**:

1. **Given** a symlink inside a permitted directory pointing to a path outside permitted directories, **When** code follows the symlink and reads the target, **Then** the read is denied and logged.

---

### User Story 5 — Security Controls Are Compatible with Virtual Threads (Priority: P3)

A Solr deployment uses Project Loom virtual threads. The security controls must not interfere with virtual thread scheduling or produce incorrect enforcement decisions based on thread identity.

**Why this priority**: Virtual thread compatibility is a hard constraint — Solr is moving toward Loom and JSM itself was incompatible with it.

**Independent Test**: Enable virtual threads; run a standard workload. Verify no false positives (legitimate operations blocked) or false negatives (violations not caught).

**Acceptance Scenarios**:

1. **Given** Solr is running with virtual threads enabled, **When** standard operations execute, **Then** no security violations are incorrectly triggered.
2. **Given** a virtual thread executes unauthorized file access, **When** the access is attempted, **Then** it is blocked just as it would be on a platform thread.

---

### Edge Cases

- What happens when a security violation occurs in a critical path during startup? The violation must be logged and startup must fail fast with a clear error rather than silently continuing.
- How does the system behave when the policy configuration file is absent or malformed? Solr must refuse to start and log a clear error rather than running without any security controls.
- What happens when a permitted path is deleted at runtime? Access to paths that were permitted but no longer exist must still be governed by the policy (i.e., a missing permitted path does not grant access to its parent).
- What happens on platforms where process spawning is used internally by Solr (e.g., the `bin/solr` script launcher)? Approved internal call sites must be explicitly listed in the default policy.

---

## Requirements

### Functional Requirements

- **FR-001**: The system MUST automatically apply security controls to all code running in the Solr JVM, including third-party plugins, without requiring developers to add per-call-site checks.
- **FR-002**: The system MUST deny file reads and writes to paths outside the set of permitted directories derived from Solr's configured home, data, log, and temporary directories.
- **FR-003**: The system MUST deny file access to Windows UNC paths (`\\host\share\...`) on all platforms by default.
- **FR-004**: The system MUST deny file access when a symlink resolves to a target outside the permitted directories.
- **FR-005**: The system MUST deny outbound network connections to hosts and ports not declared in the permitted endpoint list. The default list MUST include: loopback addresses, any host on the configured Solr HTTP port (`*:<solr.port>`), any host on the ZooKeeper port (explicit config or `<solr.port> + 1000` for embedded ZK), and a wildcard `SocketPermission "*", "connect,resolve"` scoped to the codebases of the five pre-permitted bundled modules (`jwt-auth`, `opentelemetry`, `s3-repository`, `gcs-repository`, `cross-dc-manager`), so that all current and future cluster nodes and these trusted modules are reachable without operator intervention.
- **FR-006**: The system MUST prevent `System.exit()` from terminating the JVM except when called from a small, explicitly approved set of Solr shutdown call sites.
- **FR-007**: The system MUST prevent spawning of child processes except from an explicitly approved, auditable set of call sites in Solr core.
- **FR-008**: Every security violation MUST produce a log entry at WARN level or higher that identifies: the blocked operation type, the target (path, host, or operation), and sufficient call-site context to locate the offending code.
- **FR-009**: Operators MUST be able to extend the default security policy by adding permitted file paths, network endpoints, or approved call sites via the `agent-security-extra.policy` file, without modifying Solr source code. The path to this file MUST default to `${server.dir}/etc/agent-security-extra.policy` and MUST be overridable via the `SOLR_SECURITY_AGENT_EXTRA_POLICY` environment variable (auto-converted to system property `solr.security.agent.extra.policy`). An absent extra policy file MUST be silently skipped; it is not a startup error.
- **FR-010**: The security controls MUST function correctly on Java 21 and Java 24+, without relying on any deprecated or removed JVM API.
- **FR-011**: The security controls MUST be compatible with Project Loom virtual threads; enforcement decisions MUST NOT depend on thread identity.
- **FR-012**: The security controls MUST NOT cause a measurable throughput degradation of more than 5% on standard Solr search and indexing workloads compared to an equivalent deployment without controls.
- **FR-013**: All Solr functionality that works in a standard deployment today MUST continue to work without modification when security controls are enabled.
- **FR-014**: The security controls MUST be cross-platform; they MUST apply on Linux, macOS, and Windows without relying on OS-specific mechanisms as the sole enforcement layer.
- **FR-015**: The system MUST refuse to start if the security policy configuration is absent or invalid, and MUST log a clear diagnostic message.
- **FR-016**: The agent security controls and their configuration (policy file format, enforcement mode, operator extension mechanism, and startup options) MUST be documented in the Solr reference guide as a dedicated section visible to end users and operators. Additionally, all new environment variables and system properties introduced by this feature (`SOLR_SECURITY_AGENT_MODE` / `solr.security.agent.mode`, `SOLR_SECURITY_AGENT_SKIP`, `SOLR_SECURITY_AGENT_EXTRA_POLICY` / `solr.security.agent.extra.policy`) MUST be added to `solr-properties.adoc` with their default values and descriptions, consistent with the format of existing entries in that file.
- **FR-017**: The system MUST expose security violation counts, broken down by type (file access, network, exit, process exec), in Solr's metrics registry so operators can monitor and alert on violations without log parsing.

### Key Entities

- **Security Policy**: The complete set of rules defining what file paths, network endpoints, and process operations are permitted. Has a default configuration and supports operator-defined extensions.
- **Permitted Path**: A directory (or file pattern) that code running in the Solr JVM is allowed to read from or write to. Derived from Solr's configured layout by default.
- **Permitted Endpoint**: A host-and-port pair (or pattern) that code is allowed to connect to outbound. The default list covers intra-cluster traffic via port-based wildcards (`*:<solr.port>`, `*:<zk-port>`); all other external traffic requires explicit operator policy entries.
- **Approved Call Site**: A specific class (or class pattern) in Solr core that is permitted to call `System.exit()` or spawn a child process.
- **Security Violation**: A blocked operation. Carries operation type, target, timestamp, and call-site context. Emitted as a log entry.

---

## Success Criteria

### Measurable Outcomes

- **SC-001**: 100% of standard Solr integration tests pass without modification when security controls are enabled, on both Java 21 and Java 24+.
- **SC-002**: Throughput on standard search and indexing benchmarks degrades by no more than 5% with controls enabled versus disabled.
- **SC-003**: Every attempted unauthorized file access, outbound connection, `System.exit()` call, and unauthorized process spawn produces a log entry within the same operation, on 100% of attempts in automated tests.
- **SC-004**: A plugin attempting to read a file outside the permitted directory tree is blocked on 100% of attempts, including when the path uses symlinks or Windows UNC notation.
- **SC-005**: An operator can configure a custom permitted file path or network endpoint and have it take effect after a Solr restart, following only the published documentation.
- **SC-006**: Solr starts and operates normally under virtual threads with security controls enabled, with no false-positive security violations in the standard test suite.
- **SC-007**: After at least one security violation occurs, the corresponding per-type counter in the metrics registry is non-zero and readable via the standard metrics API, confirming metric registration succeeded.
- **SC-008**: The `solr/agent-sm/` module test suite runs with the agent in enforce mode and passes with zero unexpected violations; the broader Solr test suite runs in warn mode with zero unexpected violations logged.

---

## Clarifications

### Session 2026-04-28

- Q: Must the agent and its configuration be documented in the Solr reference guide? → A: Yes — FR-016 added requiring a dedicated reference guide section covering policy file format, enforcement mode, operator extension mechanism, and startup options.
- Q: Should security violations be exposed as Solr metrics in addition to log entries? → A: Yes, broken down by type (FILE, NETWORK, EXIT, EXEC) in the metrics registry — FR-017 added.
- Q: Should the agent run in enforce mode during the Solr test suite? → A: Enforce mode within the `solr/agent-sm/` module tests only; warn mode for the broader Solr test suite initially, with a planned follow-up to flip to enforce once the violation log is clean.
- Q: Should the network policy refresh when SolrCloud cluster topology changes at runtime? → A: No — startup-only; topology changes require a restart. Documented as a known limitation; workaround is permitting cluster subnet via the extension policy file. *(Superseded by Q7 below.)*
- Q: How should the default network policy handle SolrCloud inter-node connectivity, given that the full node set is not known at agent startup? → A: Permit any host on the configured Solr HTTP port (`*:<solr.port>`) and on the ZooKeeper port (explicit config or `solr.port + 1000` for embedded ZK). No host restriction for intra-cluster traffic; port restriction prevents connections to arbitrary external services on other ports. The startup-only topology derivation assumption is removed; no restart is required when new nodes join the cluster.
- Q: What happens to the existing JSM-era policy files (`security.policy`, `solr-tests.policy`)? → A: Retain both with deprecation notices in this release; schedule removal in a future release.
- Q: When does warn mode become enforce-by-default, and should warn mode log a startup advisory? → A: No committed timeline; the flip is a future decision. Warn mode does not log an advisory message.

### Session 2026-04-29

- Q: Should bundled modules that make outbound network connections have their endpoint host pre-permitted in the default agent policy, or should each require an explicit operator `agent-security-extra.policy` entry? → A: Pre-permit with a `SocketPermission "*", "connect,resolve"` wildcard in the **bundled agent policy** for the five modules whose external endpoint is exclusively controlled by a node/cluster admin (`jwt-auth`, `opentelemetry`, `s3-repository`, `gcs-repository`, `cross-dc-manager`). The `extraction` module is excluded from this pre-permitting because its Tika Server URL is configurable via `solrconfig.xml` at `CONFIG_EDIT` privilege level, creating an SSRF risk with a wildcard grant; operators must add an explicit policy entry for it. T044 and T045 scope is reduced to the `extraction` module only.

### Session 2026-04-30

- Q: Must the new env vars/sysprops introduced by this feature (`SOLR_SECURITY_AGENT_MODE`, `SOLR_SECURITY_AGENT_SKIP`, `SOLR_SECURITY_AGENT_EXTRA_POLICY` and their sysprop equivalents) be documented in the ref-guide `solr-properties.adoc` properties reference page? → A: Yes — FR-016 extended to explicitly require `solr-properties.adoc` entries for all three new variables, in addition to the dedicated security section.
- Q: Should the path to `agent-security-extra.policy` be fixed or configurable (to support read-only installs, container images, and config-management tooling)? → A: Fixed default (`${server.dir}/etc/agent-security-extra.policy`) with an env/sysprop override: `SOLR_SECURITY_AGENT_EXTRA_POLICY` env var, auto-converted to system property `solr.security.agent.extra.policy`. An absent file is silently skipped (not a startup error). `solr.xml` is not a supported source for this setting (the agent loads before `solr.xml` is parsed).

---

## Assumptions

- Solr's configured home, data, log, and temporary directories are the authoritative source for the default permitted file path set; no additional paths are permitted by default.
- The default permitted network endpoint list covers intra-cluster communication via port-based wildcards: any host on `<solr.port>` (inter-node HTTP) and any host on the ZooKeeper port (explicit config or `<solr.port> + 1000` for embedded ZK). This ensures all cluster nodes — including those that join after startup — are reachable without operator intervention or a restart.
- Loopback addresses (`localhost`, `127.0.0.1`, `::1`) are unconditionally permitted in the default policy.
- External ZooKeeper ensemble hosts on non-standard ports (i.e., a ZK ensemble not co-located with Solr on the standard port offset) are added to the permitted list from the ZK connection string configuration at startup.
- OS-level hardening (e.g., `systemd` unit file restrictions on Linux) is treated as a complementary layer and is documented but not required for the in-JVM controls to function.
- Removing `SolrPaths.assertPathAllowed` call sites is out of scope for this feature; the automatic controls supersede them, but legacy call sites are retained for defense-in-depth.
- Fine-grained access controls (reflection, class loading, runtime permissions beyond the four categories above) are explicitly out of scope.
- The OpenSearch `agent-sm` module (Apache 2.0 licensed) is a candidate for adoption or forking and will be evaluated during planning; this specification is technology-agnostic with respect to that choice.
- Security controls are enabled by default in warn mode in new installations and when upgrading, with a documented opt-out mechanism (`SOLR_SECURITY_AGENT_SKIP=true`) for operators who need to disable them temporarily. The timeline for making enforce mode the default is not committed; it will be decided in a future release based on test suite and community readiness. Warn mode does not emit a startup advisory message.
- The existing `solr/server/etc/security.policy` and `gradle/testing/randomization/policies/solr-tests.policy` files are retained in this release with deprecation notices (added as comments); they are no longer enforced by the JVM and serve as migration references only. Removal is planned for a future release.
- The `solr-tests.policy` file continues to be used by the randomized test framework for purposes unrelated to the agent; it must not be deleted in this release.

- The `agent-security-extra.policy` file path defaults to `${server.dir}/etc/agent-security-extra.policy`. It can be overridden via `SOLR_SECURITY_AGENT_EXTRA_POLICY` env var / `solr.security.agent.extra.policy` sysprop to support read-only install trees, container images, and config-management tooling. The file's absence is not an error. The `solr.xml` element is not supported for this setting because the agent initialises before `solr.xml` is parsed.
- Several optional modules make outbound connections to operator-configured external services. For five of them (`jwt-auth`, `opentelemetry`, `s3-repository`, `gcs-repository`, `cross-dc-manager`), the external endpoint address is exclusively configurable by a node or cluster administrator, so a `SocketPermission "*", "connect,resolve"` wildcard is pre-permitted in the bundled agent policy — operators need no manual policy entry to use these modules. The `extraction` module is excluded from pre-permitting because its Tika Server URL is configurable via `solrconfig.xml` at `CONFIG_EDIT` privilege (collection-admin level), which would allow a collection administrator to redirect the permission to an arbitrary SSRF target; operators who use remote Tika must add an explicit policy entry. The `extraction` module's reference guide page carries a policy NOTICE box with a ready-to-paste snippet (T044/T045). See [research.md — Module Outbound Network Connections](research.md) for the full per-module analysis.
