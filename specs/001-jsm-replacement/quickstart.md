# Quickstart: Java Security Manager Replacement

**Audience**: Solr operators and plugin developers
**Feature**: Agent-based runtime security controls replacing the removed Java Security Manager

---

## What This Feature Does

Starting with this release, Solr automatically enforces boundaries around:
- **File system access** — code can only read/write directories Solr is configured to use
- **Network connections** — outbound connections are restricted to known cluster endpoints
- **JVM shutdown** — `System.exit()` can only be called from Solr's own shutdown paths
- **Process spawning** — child process creation is restricted to approved Solr components

These protections apply to all code in the JVM, including third-party plugins, **without any code changes required**.

---

## Default Behaviour

### Enforcement mode at startup

In this release, the default mode is **warn-only**: violations are logged but operations are not blocked. This allows operators to identify any gaps in the default policy before switching to enforce mode.

Check for violations in `solr.log`:
```
grep "SECURITY VIOLATION" logs/solr.log
```

### Switching to enforce mode

Once satisfied that no legitimate operations are being flagged, enable enforcement:

In `bin/solr.in.sh` (Linux/macOS):
```bash
SOLR_SECURITY_AGENT_MODE=enforce
```

In `bin/solr.in.cmd` (Windows):
```bat
set SOLR_SECURITY_AGENT_MODE=enforce
```

Then restart Solr.

---

## Extending the Policy for Custom Plugins

If your plugin reads from a non-standard directory or connects to an external service, add a policy extension file:

**Default file**: `server/etc/agent-security-extra.policy`

**Custom location** (read-only installs, containers, config-management):
```bash
# In bin/solr.in.sh
SOLR_SECURITY_AGENT_EXTRA_POLICY=/etc/solr/my-security-extra.policy
```
An absent file is silently skipped — no need to create it if no extensions are required.

**Example** — plugin reads from `/data/external` and connects to `reporting.myco.com:8080`:
```
grant {
    permission java.io.FilePermission "/data/external/-", "read";
    permission java.net.SocketPermission "reporting.myco.com:8080", "connect,resolve";
};
```

Restart Solr after editing. The extension file is merged with the default policy at startup.

---

## Diagnosing Violations

A violation log entry looks like:
```
[WARN ] SecurityAgent - SECURITY VIOLATION [FILE_READ] target=/tmp/evil.txt caller=com.example.MyPlugin mode=WARN
```

**To identify the source**: The `caller` field shows the top non-JDK class in the stack. If it belongs to a known plugin, add a policy entry for that plugin's required access.

**To see the full call stack**: Enable DEBUG logging for `org.apache.solr.security.agent`.

---

## Disabling the Feature (Not Recommended)

If you need to temporarily disable security controls during troubleshooting:

```bash
# In bin/solr.in.sh — removes the -javaagent flag
SOLR_SECURITY_AGENT_SKIP=true
```

This is a temporary measure only. Running without security controls on Java 24+ provides no runtime enforcement of any kind.

---

## Plugin Author Guide

**No code changes required** for plugins that access standard Solr-managed paths and cluster endpoints.

**If your plugin accesses external resources**, document the required policy entries in your plugin's README so operators can add them to `agent-security-extra.policy`.

**Do not** call `SolrPaths.assertPathAllowed()` in new plugin code — the agent enforces this automatically. The method is retained for internal Solr use only.
