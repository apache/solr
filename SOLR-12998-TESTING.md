# SOLR-12998 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

1. `solr/core/.../handler/admin/CoreAdminOperation.java` — `REQUESTRECOVERY_OP`:
   when the target core isn't loaded, it now checks
   `coreContainer.isCoreLoading(cname)`. Still loading → `503 SERVICE_UNAVAILABLE
   "Core <cname> is still loading"` (explicit, retriable transient); genuinely
   unknown core → the existing `400 BAD_REQUEST "Unable to locate core"`.
2. `solr/core/.../cloud/SyncStrategy.java` — `requestRecovery()` now retries the
   recovery request once, after a 30s delay, when the replica answers `503`
   (`RemoteSolrException` with code 503, i.e. the still-loading signal above).
   The send logic was extracted into `sendRecoveryRequest(...)`. The delay runs
   on `updateExecutor`, which is an unbounded cached pool, so the sleep doesn't
   starve other tasks; `isClosed` is re-checked after the sleep.

Also added: `changelog/unreleased/SOLR-12998.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true
./gradlew :solr:core:test --tests "org.apache.solr.cloud.*Recovery*"
```

Suggested new coverage (not included): issue
`/admin/cores?action=REQUESTRECOVERY&core=<missing>` against a node and assert
400 for a never-existent core name vs 503 while a core with that name is
loading (the true race is timing-sensitive; at minimum assert the
code/message distinction). For the retry: unit-test `requestRecovery`'s
runnable against a stub that 503s once then succeeds — awkward without
refactoring, left to the reviewer.

## Limits / risks

- One retry after a fixed 30s is a heuristic, not a redesign: very slow core
  loads can still miss the nudge. The durable improvement is the 503 signal
  itself, which also makes the condition visible/monitorable instead of a
  misleading 400.
- The 30s sleep happens on the shared `updateExecutor` thread for that request;
  the pool is unbounded so other recovery requests aren't blocked.
- No new tests were added in this phase per the contribution workflow.
