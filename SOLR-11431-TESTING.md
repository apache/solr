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

## Acceptance checks

1. On the unpatched base, the three `TestCoreContainer` assertions should observe 500.
2. On this branch, each should observe 503 and preserve the original initialization failure as
   the exception cause.
3. A request for a nonexistent core must still return `null`; it is not an initialization
   failure.
4. Do not broaden the change to unrelated `SERVER_ERROR` uses or to generic CoreAdmin failures.
   `TestLazyCores` is the guard against that accidental scope expansion.

Record the Gradle seed and test-output path for any failure. No Gradle task was run while
preparing this branch.
