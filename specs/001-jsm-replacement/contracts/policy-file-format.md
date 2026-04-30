# Contract: Security Policy File Format

**Type**: Configuration file format
**Operator-facing**: Yes
**Files**: `${solr.home}/server/etc/agent-security.policy`, `agent-security-extra.policy`

**Extra policy file path**: Defaults to `${server.dir}/etc/agent-security-extra.policy`. Override via:
- Environment variable: `SOLR_SECURITY_AGENT_EXTRA_POLICY=/path/to/my.policy`
- System property: `-Dsolr.security.agent.extra.policy=/path/to/my.policy`

An absent file is silently skipped (not a startup error). This override supports read-only install trees, container images, and config-management tooling where the Solr server directory is not writable. Note: `solr.xml` is not a supported source for this setting — the agent initialises before `solr.xml` is parsed.

---

## Overview

Solr's agent-based security controls are configured via JDK-style `.policy` files, the same syntax used by the historical Java Security Manager. This allows operators familiar with JSM policy files to reuse their knowledge.

Two files are loaded at startup:
1. **`agent-security.policy`** — Solr's default policy (shipped with Solr, not meant to be edited).
2. **`agent-security-extra.policy`** — Operator extension policy (optional, empty by default).

---

## File Syntax

Standard JDK policy file syntax:

```
// Line comments are supported

grant [codeBase "url"] {
    permission <PermissionClass> ["target"] [, "actions"];
    ...
};
```

### Supported Permission Types

| Permission Class | Target | Actions | Effect |
|-----------------|--------|---------|--------|
| `java.io.FilePermission` | Absolute path or `path/-` (recursive) | `"read"`, `"write"`, `"delete"`, `"read,write"` | Permits file operations on matching paths |
| `java.net.SocketPermission` | `"host:port"` | `"connect,resolve"` | Permits outbound connection to host:port |
| `java.lang.RuntimePermission` | `"exitVM"` | _(none)_ | Grants permission to call `System.exit()` |
| `java.lang.RuntimePermission` | `"exec"` | _(none)_ | Grants permission to spawn child processes |

### Variable Substitution

The following variables are expanded in path targets:

| Variable | Resolved Value |
|----------|---------------|
| `${solr.home}` | Solr home directory |
| `${solr.data.dir}` | Solr data directory |
| `${solr.log.dir}` | Solr log directory |
| `${solr.install.dir}` | Solr installation root (parent of `server/`); used in `codeBase` paths for module JARs |
| `${java.io.tmpdir}` | JVM temporary directory |
| `${java.home}` | JDK installation directory |
| `${user.home}` | OS user home directory |

---

## Default Policy (Conceptual)

The default `agent-security.policy` permits:

```
grant {
    // Solr home (read-only for config files)
    permission java.io.FilePermission "${solr.home}/-", "read";

    // Solr data and index directories (read + write)
    permission java.io.FilePermission "${solr.data.dir}/-", "read,write,delete";

    // Log directory (write)
    permission java.io.FilePermission "${solr.log.dir}/-", "read,write,delete";

    // Temporary files
    permission java.io.FilePermission "${java.io.tmpdir}/-", "read,write,delete";

    // JDK runtime libraries (read-only)
    permission java.io.FilePermission "${java.home}/-", "read";

    // Loopback network (inter-thread, localhost HTTP)
    permission java.net.SocketPermission "localhost:1-65535", "connect,resolve";
    permission java.net.SocketPermission "127.0.0.1:1-65535", "connect,resolve";

    // ZooKeeper ensemble (populated at startup from cluster config)
    // permission java.net.SocketPermission "<zk-host>:<zk-port>", "connect,resolve";

    // Approved System.exit() callers
    permission java.lang.RuntimePermission "exitVM";
    // (codeBase-restricted to Solr CLI and shutdown hooks in full default policy)
};
```

---

## Operator Extension Example

To permit a plugin that reads from `/mnt/nfs-data` and connects to `analytics.internal:443`:

```
// ${solr.home}/server/etc/agent-security-extra.policy

grant {
    permission java.io.FilePermission "/mnt/nfs-data/-", "read";
    permission java.net.SocketPermission "analytics.internal:443", "connect,resolve";
};
```

---

## Enforcement Mode

Controlled via the `SOLR_SECURITY_AGENT_MODE` environment variable (auto-converted by `EnvUtils` to system property `solr.security.agent.mode`):

| Value | Behaviour |
|-------|-----------|
| `warn` (default) | Violations are logged at WARN; operation proceeds |
| `enforce` | Violations are logged at ERROR; operation is blocked with `SecurityException` |

Set in `solr.in.sh`:
```bash
SOLR_SECURITY_AGENT_MODE=enforce
```

---

## Violation Log Format

When a violation is detected, Solr logs a structured message:

```
[WARN ] SecurityAgent - SECURITY VIOLATION [FILE_READ] target=/etc/passwd caller=com.example.BadPlugin mode=WARN
    at com.example.BadPlugin.readConfig(BadPlugin.java:42)
    at org.apache.solr.core.CoreContainer.loadCore(CoreContainer.java:...)
```

Fields always present: operation type, target, top caller class, enforcement mode.
