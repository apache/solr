# SOLR-11431 testing handoff

This is a branch-local handoff note, not an upstream documentation change. Leave it out of any
eventual Solr pull request.

## Scope

The branch changes `SolrCoreInitializationException` from HTTP 500 to HTTP 503. It represents a
core that is temporarily unavailable because initialization failed; it does not make PeerSync
accept arbitrary 500 responses.

The historical lucene-solr pull request was closed stale, so this is a current, narrowly scoped
SOLR-11431 successor rather than a direct revival of that old branch.

## Existing focused coverage

`org.apache.solr.core.TestCoreContainer` now verifies 503 for three direct `getCore()` paths that
surface a failed core initialization:

- a bogus configured core;
- a bad core after reload;
- a failed dynamically created core.

Run the focused test on Linux:

```bash
./gradlew :solr:core:test --tests org.apache.solr.core.TestCoreContainer
```

Then run the adjacent generic-failure coverage, which intentionally remains a 500 path:

```bash
./gradlew :solr:core:test --tests org.apache.solr.core.TestLazyCores
```

The leader-election classification is covered separately:

```bash
./gradlew :solr:core:test --tests org.apache.solr.update.PeerSyncLeaderElectionTest
```

## Acceptance checks

1. On the unpatched base, the three `TestCoreContainer` assertions should observe 500.
2. On this branch, each should observe 503 and preserve the original initialization failure as
   the exception cause.
3. A request for a nonexistent core must still return `null`; it is not an initialization
   failure.
4. Do not broaden the change to unrelated `SERVER_ERROR` uses or to generic CoreAdmin failures.
   `TestLazyCores` is the guard against that accidental scope expansion.
5. A failed core's 503 version request is tolerated only during the PeerSync version-request path;
   generic 500 responses, update requests, and non-tolerant calls must still fail.

Record the Gradle seed and test-output path for any failure. No Gradle task was run while
preparing this branch.

## Base-discrimination check — completed 2026-10-01

A throwaway worktree was created at the unpatched base `70c1a28995d` and only the branch's
updated `solr/core/src/test/org/apache/solr/core/TestCoreContainer.java` was copied in (no
production-code changes). Ran the two modified methods with `-Ptests.seed=CA115731`:

```bash
./gradlew :solr:core:test \
  --tests "org.apache.solr.core.TestCoreContainer.testCoreInitFailuresFromEmptyContainer" \
  --tests "org.apache.solr.core.TestCoreContainer.testCoreInitFailuresOnReload" \
  -Ptests.seed=CA115731
```

Both failed on the unpatched base exactly as required for discrimination:

- `testCoreInitFailuresFromEmptyContainer`: `java.lang.AssertionError: expected:<503> but was:<500>`
- `testCoreInitFailuresOnReload`: `java.lang.AssertionError: expected:<503> but was:<500>`

(The accompanying `classMethod` ObjectTracker failure is a cascade of the aborted tests, not a
separate issue.) This confirms the new 503 assertions exercise the fix: unpatched production code
returns 500, patched code returns 503.

The throwaway worktree was deleted afterwards. The latest branch also contains the direct
`PeerSyncLeaderElectionTest` coverage added in commit `3911124a57c`. `TestLazyCores` (6 tests)
remains green on the branch and guards against scope expansion.
