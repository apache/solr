# SOLR-13265 - hypothetical-reproduction handoff

**Read this first: the fix and test on this branch were written without being compiled or run.** The audit pipeline has no Gradle access. Treat both as hypotheses.

- JIRA: https://issues.apache.org/jira/browse/SOLR-13265 - "TLOG replica, updateHandler errors in metrics, no logs" (2019). Reopened in audit round audit-1 (Tier 1 batch 14); the old skip note said "root cause unidentified, needs repro to locate the counter site".
- Branch: `solr-13265-submit` off `apache/solr` main `9b3a84b1c46`

## The bug, as understood
`DirectUpdateHandler2.addDoc0` counts an update as an error when `rc != 1` in its `finally` block. TLOG followers receive updates with `UpdateCommand.IGNORE_INDEXWRITER` set; that branch writes to the update log and `return 1`s *before* `rc = 1` is assigned, so the `finally` block sees `rc == -1` and increments the error counter for every document. Nothing is logged because no exception is involved. This matches the report exactly: errors rise per indexed document on TLOG followers only, not on NRT replicas or the TLOG leader.

## What the branch changes
- `DirectUpdateHandler2.addDoc0`: a `bypassedIndexWriter` flag set in the `IGNORE_INDEXWRITER` branch; `finally` counts an error only when `rc != 1` and the IndexWriter was not bypassed. `numDocsPending` is unchanged (it is not bumped for bypassed updates, as before).
- Test: `SolrIndexMetricsTest.testUpdatesBypassingIndexWriterAreNotErrors` adds five docs with `IGNORE_INDEXWRITER` and expects the `solr_core_update_errors` counter to be absent or 0.

## Guesses to verify first
1. The Prometheus name `solr_core_update_errors` and the `category=UPDATE` label match what the OTel counter `solr.core.update.errors` is exported as (other tests use `solr_core_..._<name>` with a `category` label; the counter is created with `Attributes.empty()` plus the factory's base attributes).
2. `AddUpdateCommand.clear()` does not reset flags in a way that conflicts with calling `setFlags` afterwards (the test sets flags after `clear()`).
3. `uh.addDoc(add)` for a command without a version works on a standalone core with `IGNORE_INDEXWRITER` (the early branch skips versioning and the IndexWriter).
4. The delete paths (`delete`, `deleteByQuery`) already use a separate `madeIt` flag and are not affected.

## Verify (Gradle required; not run here)
```
.\gradlew :solr:core:spotlessApply
.\gradlew :solr:core:test --tests "org.apache.solr.update.SolrIndexMetricsTest"
```
Fail-before: revert the `bypassedIndexWriter` condition in `finally` (use `rc != 1` alone); the new test should then see 5 errors.

## Not done
No JIRA comment, no PR.
