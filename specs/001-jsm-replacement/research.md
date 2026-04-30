# Phase 0 Research: Java Security Manager Replacement

**Date**: 2026-04-28
**Feature**: specs/001-jsm-replacement/spec.md

---

## Decision 1: Adopt, Fork, or Build the Java Agent

**Decision**: Fork/adapt the OpenSearch `agent-sm` module rather than building from scratch.

**Rationale**:
- The OpenSearch agent is Apache 2.0 licensed — fully compatible with Apache Solr's licensing.
- It directly solves the same problem (JSM removal in JDK 24), with the same four protection categories required by this spec: file access, network access, System.exit(), process execution.
- It uses ByteBuddy (already widely used in the Java ecosystem) and StackWalker (standard since Java 9), both compatible with Java 21+.
- It is virtual-thread compatible (no thread-identity assumptions).
- Building equivalent infrastructure from scratch would take significantly longer with no functional advantage.
- Solr can strip out OpenSearch-specific policy entries (e.g., BouncyCastle, script permissions) and adapt the policy defaults to Solr's directory layout.

**Alternatives considered**:
- **Build from scratch**: Higher effort, same outcome, no benefit.
- **Use a third-party security library (e.g., Byte Buddy agent SPI)**: No existing library covers the full set of interceptions needed (file + network + exit + exec). Would still require significant custom work.
- **Rely solely on OS-level hardening (systemd)**: Fails the cross-platform constraint (not available on Windows/macOS).

---

## Decision 2: Subproject Location

**Decision**: Create a new Gradle subproject `solr/agent-sm/` (directly under `solr/`, not under `solr/modules/`).

**Rationale**:
- The agent JAR must be on the JVM command line as `-javaagent:`, loaded by the JVM *before* any application classloader. It is fundamentally different from a Solr module (which is loaded inside the running Solr JVM by Solr's own classloading machinery).
- Placing it under `solr/modules/` would be misleading — Solr modules are components that Solr itself loads and manages at runtime. The agent is an external JVM concern that Solr does not load; the JVM does.
- `solr/agent-sm/` is structurally parallel to other non-module subprojects like `solr/core/`, `solr/solrj/`, and `solr/test-framework/`, which is the correct category for a build artifact that is JVM-level infrastructure.
- Mirroring the OpenSearch layout (`libs/agent-sm/`) is closest in spirit, but Solr does not use a top-level `libs/` directory; `solr/` is the equivalent container.

**Alternatives considered**:
- **`solr/modules/agent-sm/`**: Misleading — modules are Solr-level, not JVM-level. Would also imply the agent is optional/pluggable in the same sense as, e.g., `jwt-auth`, which it is not.
- **`solr/core/` integration**: Not possible — agent must be a standalone fat JAR with `Premain-Class` manifest entry, produced independently of `solr-core.jar`.
- **Top-level `agent-sm/`**: Would sit outside the `solr/` tree, inconsistent with where all Solr Java source lives.

---

## Decision 3: Policy Configuration Format

**Decision**: Use JDK-style `.policy` file syntax (same format as the existing `solr/server/etc/security.policy`), extended with Solr-specific variable substitution (e.g., `${solr.home}`, `${solr.data.dir}`).

**Rationale**:
- Solr already ships a `security.policy` file; operators are familiar with the syntax.
- OpenSearch's `agent-sm` already implements a `PolicyFile` parser that reads JDK-style `.policy` files — this can be adapted directly.
- Solr-specific variable substitution (`${solr.home}` etc.) avoids hardcoding paths and makes the default policy portable across installations.

**Alternatives considered**:
- **Custom YAML/JSON config**: More readable but requires a new parser, new documentation, and breaks familiarity with JSM policy syntax.
- **Inline configuration in `solr.xml`**: Too verbose; policy files can be large; separating concerns is cleaner.

---

## Decision 4: Enforcement Mode — Enforce vs. Warn-Only

**Decision**: Ship with a **warn-only mode** as the initial default, with an **enforce mode** opt-in via configuration. A future release will flip the default to enforce.

**Rationale**:
- Solr has a large plugin ecosystem; some plugins may legitimately access paths or hosts not covered by the default policy.
- A hard-blocking default would cause regressions for valid third-party plugins, violating FR-013.
- Warn-only allows operators to identify gaps in their custom policy before flipping to enforce.
- This matches how the old JSM was typically introduced: test first, enforce later.

**Alternatives considered**:
- **Enforce by default immediately**: Higher risk of regressions for plugin authors; may delay adoption.
- **Warn-only permanently**: Defeats the security purpose; not acceptable long term.

---

## Decision 9: Default Network Policy for Intra-Cluster Connectivity

**Decision**: The default permitted network endpoint list uses **port-based wildcards** rather than a host-derived list: permit any host on `<solr.port>` (inter-node HTTP) and any host on the ZooKeeper port (explicit config, or `<solr.port> + 1000` for embedded ZK).

**Rationale**:
- SolrCloud nodes join the cluster incrementally after ZooKeeper registers them. A node list derived at agent startup will always be incomplete, causing legitimate inter-node connections to be blocked in enforce mode for nodes that join later.
- Port-based wildcards allow all current and future cluster nodes to communicate without any operator action or restart.
- The port restriction still prevents connections to arbitrary external services — an attacker or rogue plugin cannot use the intra-cluster policy to reach, e.g., a database on an unexpected port.
- Inter-node Solr HTTP is already authenticated via PKI, so the broadened host permission does not weaken authentication.
- This removes the previously documented "known limitation" that topology changes require a restart.

**Alternatives considered**:
- **Dynamic ZK watcher**: Subscribe to ZK node join/leave events and update permitted endpoints at runtime. Tightest security, but adds significant complexity to the agent and creates a dependency on ZK client libraries in the agent JAR.
- **Auto-detect local subnet**: Derive `/24` from the primary network interface. Avoids needing to know the port but may be ambiguous on multi-homed hosts or containers.
- **Operator-configured subnet** (`SOLR_SECURITY_AGENT_CLUSTER_SUBNET`): Explicit but adds operator friction; incorrect configuration silently breaks the cluster.
- **Startup-only host derivation**: Rejected because in practice the ZK-known node list is always incomplete at startup.

**Default permitted entries added by this decision**:
- `*:<solr.port>` — connect,resolve (inter-node Solr HTTP; default port 8983)
- `*:<zk-port>` — connect,resolve (ZK port from config, or `solr.port + 1000` for embedded ZK)
- External ZK ensemble hosts:ports from the ZK connection string (specific hosts, added at startup)

---

## Decision 5: Activation Mechanism

**Decision**: The agent JAR is added to Solr's startup scripts (`solr.in.sh` / `solr.in.cmd`) as a `-javaagent:` JVM argument, automatically, when the module is present.

**Rationale**:
- Java agents must be loaded via `-javaagent:` before the application starts — they cannot be loaded on demand at runtime.
- Solr's startup scripts already have a pattern for conditionally appending JVM args (e.g., for GC logging, heap dumps).
- The module JAR being present in `server/modules/agent-sm/` is sufficient signal; the startup script detects it and adds the flag.

**Alternatives considered**:
- **Always-on (bundled in core)**: Not technically feasible for a Java agent.
- **Manual operator configuration of JVM args**: Too error-prone; operators would forget; protections would silently not apply.

---

## Decision 6: Disposition of SolrPaths.assertPathAllowed

**Decision**: Retain all existing `SolrPaths.assertPathAllowed` call sites as defense-in-depth, but document them as no longer the primary enforcement mechanism.

**Rationale**:
- Removing 35 call sites is unnecessary churn and removes defense-in-depth for code paths that may not be instrumented.
- The agent-based enforcement is the primary layer; `assertPathAllowed` becomes a secondary redundant check.
- New code MUST NOT add new `assertPathAllowed` call sites — the agent handles it automatically.

---

## Decision 7: Process Execution Control Implementation

**Decision**: Instrument `ProcessBuilder` and `Runtime.exec()` at the bytecode level (via agent) to restrict spawning to an allow-listed set of call-site class name prefixes. The default allow-list covers Solr's known legitimate process-spawning call sites.

**Rationale**:
- Existing `ProcessBuilder` usage in Solr is minimal (3 sites: SolrProcessManager, IpTables test helper).
- OpenSearch's `agent-sm` does not include a ProcessBuilder interceptor — Solr will need to add one.
- Class-prefix allow-listing is simple, auditable, and requires no runtime performance cost for non-spawning code paths.

**Known approved call sites**:
- `org.apache.solr.cli.SolrProcessManager` (JVM discovery via ProcessHandle — does not use ProcessBuilder directly; kept as approved for future compatibility)
- `org.apache.solr.cloud.IpTables` (test-only; listed in test policy, not production policy)

---

## Decision 8: Metrics Registration Bridge (Classloader Boundary)

**Decision**: Use reflection-based late binding from `CoreContainer` to `ViolationMetricsReporter`. No compile-time dependency between `solr:core` and `solr:agent-sm` is introduced.

**Rationale**:
- The Java agent runs in the bootstrap classloader (injected via `ClassInjector.UsingUnsafe.ofBootLoader()`). Classes loaded there are visible to all classloaders, including the application classloader used by `solr:core`.
- A compile-time dependency `solr:core → solr:agent-sm` would be circular if agent-sm ever needs core types, and would also put agent classes on the application classpath rather than the bootstrap classpath — defeating the instrumentation model.
- Reflection avoids the compile-time coupling while still allowing CoreContainer to call `ViolationMetricsReporter.registerWithSolrMetrics()` at runtime if and only if the agent is loaded.
- `ClassNotFoundException` is caught silently: if the agent JAR is not present (e.g., `SOLR_SECURITY_AGENT_SKIP=true`), metrics registration is simply skipped. No NPE, no startup failure.

**Implementation pattern** (in `CoreContainer`):
```java
try {
    Class<?> reporter = Class.forName(
        "org.apache.solr.security.agent.ViolationMetricsReporter",
        false,
        null   // null = bootstrap classloader
    );
    reporter.getMethod("registerWithSolrMetrics", SolrMetricManager.class, String.class)
            .invoke(null, metricManager, "solr.jvm");
} catch (ClassNotFoundException ignored) {
    // Agent not loaded; metrics registration skipped
} catch (ReflectiveOperationException e) {
    log.warn("Failed to register security agent metrics", e);
}
```

**Alternatives considered**:
- **Shared interface module `solr:agent-sm-api`**: Both `solr:core` and `solr:agent-sm` would depend on it. Cleaner but adds a new module with a single interface. Overkill for one method; deferred to future if the API grows.
- **ServiceLoader SPI**: Requires the agent JAR on the application classpath, not bootstrap. Architecturally incorrect for a Java agent.
- **Direct compile-time dependency**: Circular; breaks bootstrap classloader model. Rejected.

---

## Codebase Findings Summary

| Area | Finding |
|------|---------|
| Java minimum | 21 (`gradle/libs.versions.toml`: `java-min = "21"`) |
| Existing policy files | `solr/server/etc/security.policy`, `gradle/testing/randomization/policies/solr-tests.policy` |
| SolrPaths.assertPathAllowed | `solr/core/src/java/org/apache/solr/core/SolrPaths.java`; 35 call sites |
| ProcessBuilder usage | 3 sites (SolrProcessManager, IpTables test helper, ProcessManager test) |
| Subproject pattern reference | `solr/core/`, `solr/solrj/` (agent is JVM-level, not a Solr module) |
| Test base classes | `SolrTestCase`, `SolrCloudTestCase` |
| No SecurityManager active | Policy files exist but no `System.setSecurityManager()` calls |
| No existing security module | Closest is `solr/modules/jwt-auth/` |

---

## Codebase Finding: Module Outbound Network Connections

Analysis of server-side outbound network calls (excluding ZooKeeper and inter-Solr-node HTTP, which are covered by the default policy). This informs T044 (module ref guide NOTICE boxes) and T045 (commented examples in `agent-security-extra.policy`).

| Module | External Service | Protocol | Component | Notes |
|--------|----------------|----------|-----------|-------|
| `jwt-auth` | OIDC Identity Provider (well-known + JWKS) | HTTPS | `JWTIssuerConfig`, `HttpsJwksFactory` | Uses `jose4j`; host is operator-configured; may refresh JWKS at runtime |
| `extraction` | Apache Tika Server | HTTP/HTTPS | `TikaServerExtractionBackend` | Uses Jetty `HttpClient`; only in remote-Tika mode — embedded Tika makes no outbound calls |
| `opentelemetry` | OTLP collector (Jaeger, Grafana Tempo, etc.) | gRPC or HTTP | `OtlpExporterFactory` | Host from `OTEL_EXPORTER_OTLP_ENDPOINT`; continuous background export |
| `s3-repository` | Amazon S3 API (or S3-compatible) | HTTPS | `S3StorageClient` | AWS SDK v2; uses `*.amazonaws.com` or operator-configured custom endpoint |
| `gcs-repository` | Google Cloud Storage API | HTTPS | `GCSBackupRepository` | GCS SDK; uses `storage.googleapis.com` |
| `cross-dc-manager` | Apache Kafka brokers | TCP (Kafka protocol) | `KafkaCrossDcConsumer`, `KafkaRequestMirroringHandler` | Broker addresses fully operator-configured |
| `core` (ZK status admin) | ZooKeeper four-letter-word socket | TCP (raw socket, port 2181) | `ZookeeperStatusHandler` | Same ZK hosts as cluster; already covered by the default ZK policy entry |

**No outbound network calls found in**: `langid` (local NLP only), `ltr` (local feature processing), `BasicAuthPlugin` (in-memory), `analytics`.

### Handling approach — superseded by Decision 10

See Decision 10 below. The original plan (NOTICE boxes for all 6 modules) was refined after per-module trust assessment to pre-permit 5 of the 6 modules directly in the bundled agent policy.

---

## Decision 10: Per-Module Wildcard Network Pre-Permitting

**Decision**: For five of the six outbound-connecting bundled modules, add a `SocketPermission "*", "connect,resolve"` grant scoped to that module's codebase in the bundled agent policy (`agent-security.policy`). The `extraction` module is excluded.

**Pre-permitted modules and rationale**:

| Module | External Service | Who Configures Endpoint | Trust Assessment |
|--------|----------------|------------------------|-----------------|
| `jwt-auth` | OIDC IdP + JWKS | `security.json`, requires `SECURITY_EDIT` (Solr admin) | **Safe** — admin-only |
| `opentelemetry` | OTLP collector | Env var / system property / solr.xml — node admin only | **Safe** — node admin only |
| `s3-repository` | Amazon S3 / S3-compat | solr.xml backup handler config — node admin | **Safe** — node admin only |
| `gcs-repository` | Google Cloud Storage | solr.xml backup handler config — node admin | **Safe** — node admin only |
| `cross-dc-manager` | Apache Kafka | Env var / system property / ZooKeeper — node + cluster admin | **Safe** — admin-only |
| `extraction` | Remote Tika Server | `solrconfig.xml` `requestHandler` — `CONFIG_EDIT` privilege (collection admin) | **SSRF risk** — excluded |

**Why `extraction` is excluded**: The Tika Server URL is set in `solrconfig.xml` via `TikaConfig`, which is modifiable by collection administrators with `CONFIG_EDIT` privilege — a broader group than node administrators. A wildcard `SocketPermission` would allow a collection admin to point the Tika client at any internal host, enabling SSRF attacks against the cluster's internal network from within the Solr JVM. Operators who use remote Tika must add an explicit host-locked policy entry to `agent-security-extra.policy`.

**Implementation**: Each pre-permitted module's bundled policy grant uses a `codeBase` restriction to limit the wildcard to that module's JAR:
```
grant codeBase "file:${solr.install.dir}/modules/jwt-auth/-" {
    permission java.net.SocketPermission "*", "connect,resolve";
};
```
This prevents other code from benefiting from the module's wildcard grant.

**Effect on T044/T045**: Scope is reduced to the `extraction` module only. No NOTICE boxes or `agent-security-extra.policy` examples are needed for the five pre-permitted modules.

**Alternatives considered**:
- **NOTICE boxes for all 6 modules**: Higher operator friction; legitimate use cases (e.g., OTel metrics export) require manual action on every new cluster. Rejected.
- **Wildcard grant for all 6 including extraction**: Creates SSRF risk for the extraction module. Rejected.
- **Per-hostname grants auto-derived from config at startup**: Config is not available to the agent at JVM launch time; would require complex late-binding. Rejected.
