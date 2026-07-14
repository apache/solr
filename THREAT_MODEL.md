<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache Solr — Threat Model

## §1 Header

- **Project:** Apache Solr (`apache/solr`) — a Lucene-based search server.
  Companion repos in this round: `solr-operator` (Kubernetes operator),
  `solr-sandbox` (incubating/experimental), `solr-mcp` (MCP server) — modelled
  at their own trust level or placed out of scope (§2/§3).
- **Written against:** `main` @ HEAD (2026-06).
- **Author:** ASF Security team, via the threat-model-producer rubric (Scovetta
  rubric) at the Solr PMC's request (path 3).
- **Status:** DRAFT — under maintainer review (2026-06-10). Not yet ratified.
- **Version binding:** versioned with the project.
- **Canonical role:** This document is the project's **canonical, scanner-facing
  security model** — the authoritative basis for triage when an apparent
  contradiction arises (confirmed by the Solr PMC, 2026-07). It is deliberately
  *not* comprehensive of all security information; the operator-facing
  **"Securing Solr"** reference guide and the published advisories remain the
  human source of truth for deploying and hardening Solr. The two are
  complementary — this model is "for scanners", Securing Solr is "for operators"
  — and cross-reference each other (see §15). Where they appear to conflict, this
  document governs for triage and the PMC addresses the contradiction.
- **Reporting cross-reference:** §8-violating findings via the ASF security
  process ([`SECURITY.md`](SECURITY.md)) — reach the Solr PMC security group at
  security@solr.apache.org (see <https://solr.apache.org/security-reporting.html>);
  §3/§9 findings closed citing this doc.
- **Provenance legend:** *(documented)* / *(maintainer)* / *(inferred)* — each
  *(inferred)* has a §14 open question.
- **Draft confidence:** ~22 documented / 0 maintainer / 24 inferred.

**What it is.** Solr is a **search server** (HTTP/HTTP2 API) that
indexes documents and serves queries, deployable as a single node or a
**SolrCloud** cluster coordinated by ZooKeeper. It exposes query, update
(indexing), and **admin/config** APIs; supports pluggable **authentication**
and **authorization**; and can load custom code (the package manager, config
API, modules). The defining operational fact is Solr's long-standing
official stance: **Solr is meant to run inside a trusted environment with
authentication enabled — an unauthenticated Solr must never be exposed to an
untrusted network.**

## §2 Scope and intended use

Solr is a **network service**, not a library. (Solr does ship a client
library, **SolrJ** — a separate trust surface, out of scope for this model; a
later pass can model it on its own.) Roles:

- **Untrusted HTTP client** — only in scope *if Solr is intentionally exposed
  with authentication enabled*; an unauthenticated Solr is assumed network-
  isolated (§5a).
- **Authenticated user** — holds credentials; trusted to authenticate, **not**
  to exceed their authorization (RuleBasedAuthorizationPlugin / permissions).
- **Operator/admin** — trusted: owns **all configuration** (auth/authz plus any
  config / configSet manipulation), locks down admin APIs, controls ZooKeeper,
  network, and the JVM. There is deliberately no configuration role separate from
  admin — configuration *is* an admin-level privilege.

**Component families.**

| Family | Entry point | Privilege / exposure | In model? |
| --- | --- | --- | --- |
| Query API | `/select`, request handlers | read; **SSRF surface** (`shards`, streaming expressions) | **Yes** |
| Update/indexing API | `/update` | write to index | **Yes** |
| **Admin / Config / Collections API** | `/admin/*`, ConfigSet/Config API | **high — changes config, loads code** | **Yes (highest sensitivity)** |
| Package manager / blob store | package API | **custom code loading** | **Yes** |
| Auth/authz plugins | BasicAuth/JWT/Kerberos, RuleBasedAuthz | the security mechanism | **Yes** |
| ZooKeeper coupling (SolrCloud) | ZK ensemble | cluster state/config store | **Yes (trust ZK)** |
| `solr-operator` | k8s CRDs/controller | in-cluster operator | partial — Q |
| `solr-mcp` | MCP server | LLM-tool bridge | partial — Q |
| `solr-sandbox` | experimental | unreleased | No — §3 (unsupported) |

## §3 Out of scope (explicit non-goals)

- **Unauthenticated Solr exposed to an untrusted network.** Solr's documented
  posture is that it runs in a trusted environment / behind authentication; an
  operator who exposes an unauthenticated instance to the internet has violated
  the deployment contract. Findings whose precondition is "reach an
  unauthenticated admin/config endpoint over an untrusted network" are
  `OUT-OF-MODEL: non-default-config` / operator misconfiguration (§5a/§9). *(documented
  — the canonical Solr security guidance; Q-trustenv confirms.)*
- **`solr-sandbox`** — experimental/incubating, not a supported release. Threat-
  model separately. *(inferred — Q-scope.)*
- **ZooKeeper security** — Solr trusts its ZK ensemble (it stores config + cluster
  state); securing/ACLing ZK is the operator's. *(inferred — Q-zk.)*
- **The JVM / OS / network the operator runs Solr on.**

## §4 Trust boundaries and data flow

The boundary is the **Solr HTTP API**, gated (in the supported posture) by an
authentication plugin and an authorization plugin. Once past auth+authz, the
request is trusted to the extent of the caller's permissions.

```
HTTP client ─► [auth plugin] ─► [authz plugin: permission for this path/collection] ─► request handler
                                                                   │
   query: shards/streaming-expression params can cause Solr to fetch URLs  ──► SSRF surface (§9)
   admin/config/package: can change config + load custom code ──► privileged, must be authz-restricted
SolrCloud: config + cluster state come from ZooKeeper (trusted) ; inter-node calls (PKI/auth)
```

**Reachability precondition (triager's test):** a finding is in-model only if
reachable **with authentication + authorization enabled** by an actor who
*should not* be able to do it — an unauthenticated request to an auth-protected
endpoint, or an authenticated user crossing their authorization. A finding that
requires an unauthenticated, internet-exposed Solr is out of model (§5a).

## §5 Assumptions about the environment

- **Trusted environment:** in the supported posture Solr runs where only
  authorized clients reach the admin/config/update APIs (auth enabled and/or
  network isolation). *(documented — Q-trustenv.)*
- **JVM/JEXL/scripting:** risky features (e.g. the historic Velocity response
  writer, scripting) are disabled by default and must stay disabled unless the
  operator accepts the risk. *(documented — post-CVE hardening; Q-features.)*
- **ZooKeeper** (SolrCloud) is a trusted config/state store. *(inferred — Q-zk.)*
- Solr opens network listeners and (via packages/config) can load code by design.

## §5a Configuration variants — **the central knobs**

1. **Authentication + Authorization** — pluggable, **off by default**. Enabling
   them (BasicAuth/JWT/Kerberos + RuleBasedAuthorizationPlugin) is what makes the
   §8 properties hold. **Off-by-default is the insecure default**; the supported
   production posture is auth+authz on (or strict network isolation).
2. **Risky feature toggles** — Velocity/scripting/JEXL, remote streaming, the
   ability to load custom code via the config/package API. Defaults are the
   hardened (off/restricted) values after Solr's CVE history. *(documented.)*
3. **`-Dsolr.*` system properties** controlling streaming/remote URL access and
   admin-UI exposure.

**Wave-1 ruling needed (Q-trustenv/Q-features):** confirm that auth+authz-on (or
network isolation) is the supported posture, so unauthenticated-exposure findings
are `OUT-OF-MODEL: non-default-config`; and which risky toggles, if flipped on,
move a finding to `OUT-OF-MODEL: non-default-config` vs remain `VALID`.

## §6 Assumptions about inputs

| Boundary | Input | Attacker-controllable? | Enforced by / caller must |
| --- | --- | --- | --- |
| any API | credentials / auth token | **yes** | auth plugin verifies |
| any API | request path + collection | **yes** | authz plugin checks permission |
| query | `shards` / `stream.url` / streaming-expression source URLs | **yes** | restrict remote streaming; network controls (SSRF) |
| update | document content | **yes** | treated as data; size/rate limits = operator |
| admin/config/package | config, custom code, package definitions | **yes (privileged)** | authz must restrict to admins |

## §7 Adversary model

- **Untrusted HTTP client** — in scope *only* against an intentionally-exposed,
  auth-enabled Solr (tries to bypass auth, or hit an unprotected path).
- **Authenticated-but-unauthorized user** — has credentials, tries to read/write
  collections or hit admin/config APIs beyond their permissions. In scope —
  authorization is the defence.
- **Out of scope:** an attacker who can reach an unauthenticated Solr over an
  untrusted network (operator violated the deployment contract, §3/§5a); the ZK
  ensemble operator; the host/JVM.

## §8 Security properties the project provides (auth + authz enabled)

1. **Authentication.** API requests require valid credentials when an auth
   plugin is configured. *Violation:* auth bypass on a protected endpoint.
   *Severity:* critical. *(documented; Q for default.)*
2. **Authorization.** RuleBasedAuthorizationPlugin gates paths/collections/admin
   actions by permission. *Violation:* an authenticated user performs an action
   outside their permissions (esp. admin/config/package). *Severity:* critical.
   *(inferred — Q-authz.)*
3. **Risky features off by default.** Code-execution-adjacent features
   (Velocity/scripting, arbitrary remote streaming) are disabled unless the
   operator opts in. *Violation:* RCE/SSRF reachable in a default config.
   *Severity:* critical. *(documented — CVE-driven hardening.)*
4. **Inter-node auth (SolrCloud).** Node-to-node calls are authenticated
   (PKI/auth). *Violation:* a rogue actor injecting inter-node requests.
   *Severity:* high. *(inferred — Q-internode.)*
5. **Query/index correctness** — results reflect the indexed data and query
   (a correctness property, not a security one unless it leaks across an authz
   boundary). *Severity:* correctness. *(inferred.)*

## §9 Security properties the project does *not* provide

- **It is not safe to expose unauthenticated to an untrusted network.** Without
  auth, the admin/config/package APIs let a caller reconfigure Solr and load
  code — by design, for a trusted operator. This is the #1 real-world Solr
  incident class and is **out of model** (operator contract, §3/§5a).
  - *False friend:* "the admin API let me change config / load a package" is
    **not** a vulnerability when reached by an authorized admin; it is the
    feature. It is only `VALID` if reachable across an auth/authz boundary that
    should have stopped it.
- **It does not author your authorization rules.** An over-broad permission is
  an operator decision.
- **SSRF is bounded by operator network controls.** `shards`/streaming-expression
  remote fetches can reach internal URLs; restricting which hosts Solr may
  contact is operator/network config (§10). *(documented — SSRF guidance.)*
- **No protection of ZooKeeper / the JVM / the host** (§3).
- **Well-known classes the operator owns:** SSRF via streaming/shards, RCE via
  enabling risky features, XXE in document/config parsing (mitigated but
  config-dependent), and exposure-without-auth.

## §10 Downstream responsibilities (operator)

- **Enable authentication + authorization** (or strictly network-isolate Solr);
  never expose an unauthenticated instance.
- **Never place Solr in a public-facing network zone / DMZ.** Keep it in a
  firewalled internal zone, the same way you would a database — Solr is not
  designed to be internet-facing. *(maintainer — janhoy.)*
- **Always enable TLS/SSL** for client and inter-node traffic, to avoid leaking
  sensitive data or authentication credentials on the wire. *(maintainer — janhoy.)*
- **Lock down the admin/config/Collections/package APIs** to admins via authz.
- **Keep risky features disabled** unless you accept the risk; restrict remote
  streaming / `shards` to known hosts (SSRF).
- **Secure ZooKeeper** (ACLs, auth) in SolrCloud.
- Apply resource/rate limits at the boundary; keep Solr patched.

## §11 Known misuse patterns

- **Exposing an unauthenticated Solr to the internet** (or a shared network).
- **Leaving the admin/config/package APIs reachable** by non-admins.
- **Enabling Velocity/scripting/remote-streaming** on an exposed instance.
- **Unsecured ZooKeeper** holding Solr config (a backdoor into the cluster).

## §11a Known non-findings (recurring false positives)

- **"Admin/Config/Package API allows configuration change / code load"** reached
  by an authorized admin (or on an unauthenticated dev instance) — non-finding:
  it is the feature; `VALID` only across an authz boundary (§8/§9).
  `OUT-OF-MODEL: non-default-config` when the precondition is unauthenticated
  exposure.
- **SSRF via `shards`/streaming** without the operator's host restrictions —
  the network-control responsibility is the operator's (§9/§10).
- **Velocity/scripting RCE** when the feature is enabled — `OUT-OF-MODEL:
  non-default-config` (off by default, §5a).
- **Findings in `solr-sandbox`** — `OUT-OF-MODEL: unsupported-component` (§3).
- **ZooKeeper exposure** — operator-owned (§3/§10).
- **Third-party dependency issues (including Lucene)** — case-by-case: in model
  only when there's a reasonable Solr-side mitigation; otherwise route to the
  dependency's own project.

## §12 Conditions that would change this model

- A change to auth/authz defaults, or to the default-disabled risky features.
- A new code-loading or remote-fetch surface.
- Promotion of `solr-sandbox` to a release, or `solr-operator`/`solr-mcp` taking
  on an untrusted-input role.
- A report unroutable to a §13 disposition → revise §8/§9.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | A §8 property breaks with auth+authz on, via an in-scope actor. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 break, but a §11 misuse is too easy. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of operator config/authz rules. | §6/§10 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Needs ZK/host/JVM compromise. | §7 |
| `OUT-OF-MODEL: non-default-config` | Unauthenticated exposure, or an opted-in risky feature. | §5a |
| `OUT-OF-MODEL: unsupported-component` | `solr-sandbox` / test code. | §3 |
| `BY-DESIGN: property-disclaimed` | Admin power, SSRF-needs-network-controls, exposure-without-auth. | §9 |
| `KNOWN-NON-FINDING` | Matches §11a. | §11a |
| `MODEL-GAP` | Unroutable. | triggers §12 |

## §14 Open questions for the maintainers

**Wave 1 — load-bearing.**

- **Q-trustenv.** Confirm the deployment contract: Solr runs in a trusted
  environment with auth+authz enabled (or network-isolated), so
  unauthenticated-exposure findings are `OUT-OF-MODEL: non-default-config`. Point
  at the canonical guidance you want cited when closing such reports. (§3/§5a/§9.)
- **Q-features.** Which risky features are off by default, and for each: is "on"
  a supported posture (finding `VALID`) or operator-accepted-risk (finding
  `OUT-OF-MODEL: non-default-config`)? (Velocity/scripting, remote streaming,
  package/code loading.) (§5a/§8.)
- **Q-authz.** Is the §8 authorization claim made for RuleBasedAuthorizationPlugin
  specifically, and what is the default-permission posture? (§8.)

**Wave 2 — surface.**

- **Q-ssrf.** Confirm the SSRF position (shards/streaming remote fetch bounded by
  operator host controls, not by Solr) and any built-in allow-list. (§9/§10.)
- **Q-internode / Q-zk.** Inter-node auth guarantees (PKI), and the ZooKeeper
  trust assumption. (§8/§5.)

**Wave 3 — scope & coexistence.**

- **Q-scope.** Are `solr-operator`, `solr-mcp`, and `solr-sandbox` in scope for
  this round; at what trust level? (`solr-sandbox` we placed out.) (§2/§3.)
- **Q-doc.** *(Resolved — Solr PMC, 2026-07.)* Disclosure channel confirmed:
  `security@solr.apache.org` (§1). The model is **canonical** and scanner-facing —
  the authoritative basis for resolving apparent contradictions — and complementary
  to the operator-facing "Securing Solr" docs; cross-links added (§1/§15). This PR
  adds `THREAT_MODEL.md` + `SECURITY.md` and a `## Security` section appended to the
  existing `AGENTS.md` (the rest preserved). *(maintainer)*

## §15 Appendix — document roles and existing-policy back-map

**Document roles (confirmed by the Solr PMC, 2026-07).** This `THREAT_MODEL.md`
is the project's **canonical** security model for automated-scanner purposes —
the authoritative basis when an apparent contradiction arises. It is intentionally
scanner-facing (in/out-of-scope, the §8/§9/§11a property lists, §13 triage
dispositions) rather than a comprehensive operator handbook. Solr's published,
**operator-facing** guidance remains the human source of truth for deploying and
hardening Solr, and the two cross-reference each other — this model is "for
scanners", the guidance below is "for operators":

- **"Securing Solr"** (Solr Reference Guide, deployment guide) —
  <https://solr.apache.org/guide/solr/latest/deployment-guide/securing-solr.html>
- **Security advisories + reporting** — <https://solr.apache.org/security.html>
  and <https://solr.apache.org/security-reporting.html>

Where the two appear to conflict, this document governs for triage purposes and
the PMC addresses the contradiction. This is a role split, not a redundancy: the
scanner model constrains what counts as an in-model finding; Securing Solr tells
operators how to run Solr safely.

This PR adds `SECURITY.md` (ASF security-process pointer + a link to this model)
plus a `## Security` section in the existing `AGENTS.md` (a coding-agent knowledge
base — preserved). Each historical CVE maps to a §5a toggle or the
trusted-environment contract; §8/§9/§11a get refined against the Securing Solr
docs + advisories in a later pass.
