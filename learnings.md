# Learnings History

> Add a new dated entry for every contribution, rerun, or discovery so future agents can read the evolving context without scanning every PR.

## 2026-03-15
- Created and pushed branch `SOLR-18124-updatelog-replay-tracing`; added tracing spans around UpdateLog replay, per-log spans, and metadata along with the `UpdateLogReplayTracingTest` verification.
- Documented instrumentation details and test failures/mitigations in AGENTS.md for future readers.
- Opened PR #4216 targeting SOLR-18124 and noted the need to rerun the new tracing tests before merge.

## 2026-03-15 (follow-up)
- Resolved test setup issues in `UpdateLogReplayTracingTest` by wiring leader-distributed params, `jsonAdd` calls, and run metadata in isolation; test iterations now compile.
- Installed Java 21 locally, set `GRADLE_USER_HOME` to avoid permission issues, and ran:
  - `./gradlew :solr:core:test --tests org.apache.solr.update.UpdateLogReplayTracingTest`
  - `./gradlew :solr:core:test --tests org.apache.solr.update.PeerSyncWithBufferUpdatesTest`
  both succeeded after the fix.
- Pushed update to PR #4216 and left a comment summarizing the completed local checks.

## 2026-03-16
- Reintroduced an automated `solr.security.allow.paths` preload inside `SolrTestCase.beforeSolrTestCase()` so tests that reference `ExternalPaths.SERVER_HOME` no longer need to set the system property manually, and cleaned up the few remaining test files accordingly (including the `RenameCoreAPITest` formatting fix).  
- Verified `RenameCoreAPITest` again under Java 21 using `./gradlew :solr:core:test --tests org.apache.solr.handler.admin.api.RenameCoreAPITest` (build succeeded).
