# SOLR-13097 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

`solr/core/.../servlet/HttpSolrCall.java` — `getAuthorizationCollectionsList()`:
in standalone (non-ZK) mode with a resolved core, it now returns the serving
core's name instead of the empty `getCollectionsList()`. Collection/core-scoped
`RuleBasedAuthorizationPlugin` rules (e.g. `{"collection":"biblio", ...}`) can
therefore match standalone cores; previously the list was always empty
standalone, so such rules could never match and the auth log showed
`collections: []`. SolrCloud behavior is untouched.

Also added: `changelog/unreleased/SOLR-13097.yml`.

## Design note (deviation from research note)

The research note suggested populating `collectionsList` in `init()`'s
standalone branch. Against current main that would trip `assert
cores.isZooKeeperAware()` in `addCollectionParamIfNeeded()` (called with the
list from both `HttpSolrCall` and `V2HttpCall`) under tests with assertions
enabled. Fixing `getAuthorizationCollectionsList()` directly achieves the
identical user-visible outcome (auth decision + log line both consume it) with
a smaller blast radius; `getCollectionsList()` semantics are unchanged.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true
./gradlew :solr:core:test --tests "org.apache.solr.security.*"
```

Suggested new coverage (not included): standalone-mode test with a
`security.json` containing a `RuleBasedAuthorizationPlugin` with a core-scoped
permission — assert an authorized user can query `/solr/<core>/select` and an
unauthorized user is denied; assert `getAuthorizationCollectionsList()`
contains the core name instead of being empty.

## Limits / risks

- The core name (not a collection) is what rules must reference standalone —
  this matches the reporter's expected fix.
- `core == null` (admin/container paths) still falls back to the empty list.
- No new tests were added in this phase per the contribution workflow.
