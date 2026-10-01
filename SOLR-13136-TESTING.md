# SOLR-13136 — Testing Handoff

> **UNCOMPILED / UNTESTED.** This patch was written against `apache/solr` main
> at `c3e18f1e455` without running a build or any tests. The reviewer (or CI)
> must compile and validate before merge.

## What changed

1. `solr/core/.../cloud/overseer/CollectionMutator.java` — `createShard()`
   now defaults a new slice's state to `CONSTRUCTION` when the message carries
   no `SHARD_STATE_PROP` (previously the `Slice` constructor defaulted it to
   `ACTIVE`). This is the single funnel for both the overseer and the
   distributed state-update paths. A slice with no replicas is therefore no
   longer in `activeSlices` while `AddReplicaCmd` is still adding replicas,
   closing the window where concurrent distributed queries failed with
   `SERVICE_UNAVAILABLE "no active servers hosting shard"`.
2. `solr/core/.../cloud/api/collections/CreateShardCmd.java` — after
   `AddReplicaCmd.addReplica` completes, the new slice is flipped to `ACTIVE`
   via an `UPDATESHARDSTATE` message (overseer queue or
   `DistributedClusterStateUpdater.MutatingCommand.SliceUpdateShardState`,
   mirroring `SplitShardCmd`). The flip is skipped when the slice already
   existed (the mutator `NO_OP`s in that case, so a pre-existing slice keeps
   whatever state it had).

Also added: `changelog/unreleased/SOLR-13136.yml`.

## Suggested reviewer validation

```bash
./gradlew :solr:core:compileJava -Pvalidation.errorprone=true
./gradlew :solr:core:test --tests "org.apache.solr.cloud.api.collections.CreateShardTest"
```

Suggested new coverage (not included): port the reporter's reproducer — create
an implicit-router collection, run `CREATESHARD` in one thread while hammering
`*:*` queries from another, assert no `SERVICE_UNAVAILABLE` failures; assert
slice state reads `CONSTRUCTION` between creation and replica availability and
`ACTIVE` after the request completes.

## Limits / risks

- The `UPDATESHARDSTATE` flip is fire-and-forget on the overseer path (same as
  `SplitShardCmd`); the slice becomes query-visible as soon as the overseer
  applies it. With `waitForFinalState=true`, `AddReplicaCmd` already waited
  for replicas to go active before the flip is sent.
- Failure cleanup is unaffected: `DeleteShardCmd` already permits deleting
  `CONSTRUCTION` slices, and the flip is skipped on the `AssignmentException`
  path (it rethrows before reaching the flip).
- A shard created with `createNodeSet=EMPTY` (no replicas) still ends up
  `ACTIVE` with zero replicas after `addReplica` returns — same steady state
  as before; only the transient window changed.
- No new tests were added in this phase per the contribution workflow.
