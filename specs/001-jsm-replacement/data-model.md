# Data Model: Java Security Manager Replacement

**Date**: 2026-04-28
**Feature**: specs/001-jsm-replacement/spec.md

This feature introduces a runtime security policy system. There is no persistent data store — all entities exist in memory (loaded at startup from configuration files) or as transient log records.

---

## Entity: SecurityPolicy

The root entity. Loaded once at JVM startup and immutable thereafter.

| Field | Type | Description |
|-------|------|-------------|
| `permittedPaths` | `List<PermittedPath>` | File system access rules |
| `permittedEndpoints` | `List<PermittedEndpoint>` | Outbound network access rules |
| `approvedExitCallers` | `List<ApprovedCallSite>` | Classes allowed to call `System.exit()` / `Runtime.halt()` |
| `approvedExecCallers` | `List<ApprovedCallSite>` | Classes allowed to spawn child processes |
| `enforcementMode` | `Enum {WARN, ENFORCE}` | Whether violations block (ENFORCE) or only log (WARN) |
| `trustedFileSystems` | `Set<String>` | Filesystem scheme names exempt from path checks (e.g., in-memory FS used in tests) |

**Validation rules**:
- `permittedPaths` must be non-empty (Solr home must always be in the list)
- `enforcementMode` defaults to `WARN` in the initial release
- Loaded from the active `.policy` file and Solr's runtime directory configuration
- Immutable after startup; violations of this invariant throw `SecurityException`

**State transitions**:
- `UNINITIALIZED` → `LOADED` (at startup, before application code runs)
- No runtime modification is permitted

---

## Entity: PermittedPath

A single file system access rule.

| Field | Type | Description |
|-------|------|-------------|
| `path` | `String` | Absolute path or path prefix (supports `${solr.home}` and similar variables) |
| `operations` | `Set<Enum {READ, WRITE, DELETE}>` | Permitted operations on matching paths |
| `recursive` | `boolean` | Whether the rule applies to all descendants (true) or only the exact path (false) |
| `source` | `Enum {DEFAULT, OPERATOR}` | Whether the rule is from the default policy or operator extension |

**Validation rules**:
- `path` must be an absolute path after variable substitution
- UNC paths (`\\...`) are never permitted, regardless of rules
- Symlink-resolved path must also match a `PermittedPath` rule (no escape via symlinks)
- Windows drive-relative paths (e.g., `C:relative`) are rejected

---

## Entity: PermittedEndpoint

A single outbound network access rule.

| Field | Type | Description |
|-------|------|-------------|
| `host` | `String` | Hostname, IP address, or wildcard pattern |
| `portRange` | `String` | Port or range (e.g., `"2181"`, `"8983-8985"`, `"*"`) |
| `operations` | `Set<Enum {CONNECT, RESOLVE}>` | Permitted socket operations |
| `source` | `Enum {DEFAULT, OPERATOR}` | Origin of the rule |

**Validation rules**:
- Loopback addresses (`localhost`, `127.0.0.1`, `::1`) are unconditionally permitted by default
- Any host on `<solr.port>` is permitted by default (inter-node HTTP; covers nodes that join after startup)
- Any host on the ZooKeeper port is permitted by default: explicit config value, or `<solr.port> + 1000` for embedded ZK
- External ZooKeeper ensemble hosts:ports are added at startup from the ZK connection string configuration

---

## Entity: ApprovedCallSite

A class (or class name prefix) allowed to perform a restricted operation.

| Field | Type | Description |
|-------|------|-------------|
| `classNamePattern` | `String` | Fully-qualified class name or prefix pattern (e.g., `org.apache.solr.cli.*`) |
| `operation` | `Enum {EXIT, EXEC}` | The restricted operation this approval covers |
| `description` | `String` | Human-readable explanation of why this call site is approved |
| `source` | `Enum {DEFAULT, OPERATOR}` | Origin of the entry |

**Default approved EXIT callers**:
- `org.apache.solr.cli.SolrCLI` (CLI shutdown commands)
- `org.apache.solr.servlet.SolrDispatchFilter` (servlet shutdown hook)

**Default approved EXEC callers**:
- _(none by default in production policy; test policy includes `org.apache.solr.cloud.IpTables`)_

---

## Entity: SecurityViolation

A transient record of a blocked (or warn-mode logged) operation. Not persisted; emitted as a structured log entry.

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | `Instant` | When the violation occurred |
| `operationType` | `Enum {FILE_READ, FILE_WRITE, FILE_DELETE, NETWORK_CONNECT, SYSTEM_EXIT, PROCESS_EXEC}` | What was attempted |
| `target` | `String` | The path, host:port, or operation descriptor |
| `callSiteClass` | `String` | Top non-JDK class in the call stack at the point of violation |
| `callStack` | `List<String>` | Abbreviated call stack for debugging |
| `mode` | `Enum {WARN, BLOCK}` | Whether this violation was logged-only or blocked |

**Log format**: Emitted at `WARN` level (warn mode) or `ERROR` level (enforce/block mode) via SLF4J.

---

## Configuration Files

### Production Policy File

**Location**: `${solr.home}/server/etc/agent-security.policy`
**Format**: JDK-style `.policy` syntax with Solr variable substitution
**Loaded by**: `SecurityPolicy` at agent startup

### Operator Extension Policy

**Location**: `${solr.home}/server/etc/agent-security-extra.policy` (optional)
**Purpose**: Operator-provided additions to the default policy
**Loaded by**: Merged with production policy at startup; operator entries tagged with `source = OPERATOR`

### Startup Configuration

**Location**: `${solr.home}/bin/solr.in.sh` (Linux/macOS), `bin/solr.in.cmd` (Windows)
**New variable**: `SOLR_SECURITY_AGENT_MODE` — sets enforcement mode; read by `EnvUtils`, which auto-converts env var `SOLR_SECURITY_AGENT_MODE` to system property `solr.security.agent.mode`
**Example**:
```
SOLR_SECURITY_AGENT_MODE=warn
```
