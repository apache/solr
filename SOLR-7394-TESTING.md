# SOLR-7394 testing handoff

This is a branch-local handoff note, not an upstream documentation change. Leave it out of any
eventual Solr pull request.

## Scope

This branch addresses the current ShardTerms analogue of the old SOLR-7394 report. A
leader-eligible replica can publish `RECOVERING`, fail every recovery attempt, and publish
`RECOVERY_FAILED` while its `<coreNodeName>_recovering` ShardTerms entry remains. That entry
prevents it from participating in leader election indefinitely.

The branch clears the marker and resets that replica's term to `0` when recovery is finally
abandoned. It deliberately leaves an existing leader's higher term intact, so a failed replica
does not become leader while a more current replica is available.

## Existing focused coverage

`org.apache.solr.cloud.ShardTermsTest` covers the in-memory state transitions:

- failed recovery removes the marker and resets the failed replica's term;
- calling failure cleanup without a recovery marker changes nothing;
- an isolated replica becomes leader-eligible only after the marker is gone and no higher term
  remains.

Run it first on Linux:

```bash
./gradlew :solr:core:test --tests org.apache.solr.cloud.ShardTermsTest
```

## Required cloud-level repro

The unit test does not drive `RecoveryStrategy` through exhausted retries. Before proposing this
upstream, add or adapt a deterministic `SolrCloudTestCase`.

`LeaderVoteWaitTimeoutTest` is the closest harness: it creates a one-shard, three-node cluster
and uses socket proxies to control recovery and leader-election connectivity. In contrast,
`ZkShardTermsRecoveryTest` exercises successful recovery and is not a repro for this bug.

The cloud test should:

1. Create a leader and an NRT replica, then ensure the replica has published `RECOVERING`.
2. Block the replica's recovery path to the leader with the proxy harness.
3. Make exhaustion deterministic. `RecoveryStrategy` defaults to 500 retries, so do not wait for
   production retry timing; introduce a narrowly scoped test seam to use a low retry limit.
4. Wait for the replica to publish `RECOVERY_FAILED`.
5. Read `ZkShardTerms` and assert that the failed replica has no `_recovering` entry and a term
   of `0`.
6. Remove or make unavailable every higher-term replica, then assert that the failed replica can
   become leader. While a higher-term replica remains, it must not be eligible.

Run the new cloud test against the unpatched base as well. The expected base failure is a
remaining `_recovering` entry after `RECOVERY_FAILED`; the patched branch should pass. Record the
Gradle seed and test-output path for any failure.

No Gradle task was run while preparing this branch.

## Cloud-level repro — implemented 2026-10-01

The deterministic cloud test exists and is green on the patched branch.

**Test seams** (commit `9da1f191cc4`, test-only, narrowly scoped):

- `RecoveryStrategy.testing_maxRetriesOverride`: public static volatile `Integer`; when non-null,
  takes precedence over `maxRetries` so tests exhaust retries quickly. Follows the in-file
  precedent of `testing_beforeReplayBufferingUpdates`. Tests must reset it to null.
- `TestInjection.failRecovery` + `injectFailRecovery()` hook in `RecoveryStrategy`, placed right
  after `sendPrepRecoveryCmd` in the recovery path — i.e. it can only fire once recovery actually
  runs, which is after the replica has published `RECOVERING`.

**Test** (commit `15d88653d70`): new
`solr/core/src/test/org/apache/solr/cloud/ZkShardTermsRecoveryFailureTest.java`, a
`SolrCloudTestCase` modeled on `LeaderVoteWaitTimeoutTest`. It creates a 3-node cluster, elects a
leader on node 0, arms `failRecovery` with a retry limit of 3, adds an NRT replica on another node,
then asserts, in order:

1. the replica publishes `RECOVERING`;
2. the replica publishes `RECOVERY_FAILED`;
3. `ZkShardTerms` shows no `<coreNodeName>_recovering` entry and a term of `0` for the failed
   replica;
4. `canBecomeLeader` is false for the failed replica while the higher-term leader remains;
5. after stopping the leader's node, the failed replica wins the election (eligibility restored).

**Validation (patched branch, 2026-10-01):** `ZkShardTermsRecoveryFailureTest` green on 5 runs
across different seeds (1 test, 0 failures each; last serial run seed `DEADBEEF12345678`,
`tests="1" failures="0" errors="0"`). `ShardTermsTest` still green (5 tests, 0 failures).
Test-output XMLs: `solr/core/build/test-results/test/TEST-org.apache.solr.cloud.ZkShardTermsRecoveryFailureTest.xml`.

**Incident note:** one intermediate `BUILD FAILED` on seed `DEADBEEF12345678` produced no test
results at all (on-disk XML was stale from the earlier run); it overlapped with a second
backgrounded Gradle build, i.e. the Gradle cache-lock contention scenario — an infrastructure
failure, not a test failure. A serial re-run on the same seed was `BUILD SUCCESSFUL`. The
"never run two Gradle builds concurrently" lesson in `~/AGENTS.md` was extended to note that
backgrounded execs count as concurrent.

The unpatched-base run (expected: `_recovering` entry retained after `RECOVERY_FAILED`) has not
been executed yet; run it before proposing upstream.
