# SOLR-18391: graceful collection creation errors (handoff)

Branch `solr-18391-graceful-create-submit`, cut from `upstream/main` `c3cdf7b46e8`.
This file is the gate handoff and is removed before submission.

**Nothing on this branch has been compiled, formatted or run.** Every statement about
behavior below comes from reading the code. Line numbers are those of `upstream/main`
at the base commit, before this change.

## 1. Design

### What was wrong

`CreateCollectionCmd.call` writes the new collection's state early and then has a long
list of later steps that can fail. Only two of those failures cleaned up:

- an `Assign.AssignmentException` from replica assignment (line 258), and
- a core that reported a failure, or PRS replicas that did not become active (line 451).

Every other failure after the state write left the collection behind, usually with no
replicas. Anything that was not a `SolrException` was also rethrown with a null message
(line 491), so the client saw an error with no text. The NullPointerException in the
ticket is one instance: wrong exception type for the cleanup, and no message.

### What this change does

1. **Any exception after the state write deletes the collection.** The command records
   that it has written (or tried to write) the collection state. The two special-case
   cleanups are replaced by one `catch` at the end of the method that runs the same
   `DeleteCollectionCmd` cleanup for every exception once that flag is set. (The catch
   is on `Exception`; an `Error` out of a plugin, such as a `NoClassDefFoundError`,
   still skips the cleanup.) This is the
   shape `SplitShardCmd` already uses (cleanup keyed on "did not succeed", not on an
   exception type). Nothing is cleaned up for a failure before the state write, and
   just before that write the command checks ZooKeeper itself for an existing
   `state.json` (section 4, item 7), so the command never deletes a collection it did
   not create.
2. **The error names the cause.** The catch-all message is now
   `Could not create collection <name>: <exception>`. `SolrException`s pass through
   unchanged. `AssignmentException` is still reported as a 400 with its own message.
   The core-failure message keeps its existing text and gains the per-node failures.
   If the cleanup itself fails, that is logged and attached as a suppressed exception;
   the client still gets the original error (before, a cleanup failure replaced it).
3. **The create path cannot hand a missing collection to the placement plugin.** See
   the next section. This is the part that differs from the first analysis.
4. **The check from #4997 is kept** in `PlacementPluginAssignStrategy`, for callers
   other than create (add replica, move, replace node, restore, split), where a
   missing collection means it really is gone. It now also covers the system
   collection branch, which had the same dereference.

Two production files change: `CreateCollectionCmd` and `PlacementPluginAssignStrategy`
(about 85 lines added, 33 removed between them). No API, ZooKeeper layout or wire
format change.

### The race, and why this is not "wait on the other view"

The first analysis said the placement strategy reads its own snapshot from
`SolrCloudManager`, separate from the `ZkStateReader` the command waits on, and
proposed waiting on that second view. Reading the code, that is not what happens:

- `buildReplicaPositions` wraps the cloud manager (`wrapCloudManager`, line 550) so that
  its cluster state is the `clusterState` variable the command passes in.
  `DelegatingCloudManager` does not override `getClusterState()`, and the interface
  default goes through `getClusterStateProvider()`. So the placement context reads
  exactly the command's own `clusterState`. There is no second snapshot to wait on.
- In the PRS branch that variable is built with
  `clusterState.copyWith(collectionName, command.collection)` (line 204). The
  collection cannot be missing there.
- In the non-PRS branch the command waits with `zkStateReader.waitForState(...)`
  (line 235), which returns the collection it saw, and then **throws that away and
  reads the cluster state again** (line 245). The second read is where the collection
  can be missing. This matches the ticket's own wording ("the race between
  `waitForState` and `getClusterState()`").

So the two views are the collection the watch delivered and a later re-read of the
reader's map, both from the same `ZkStateReader`. The fix is to stop depending on the
re-read: if the re-read state lacks the collection, the command puts in the one the
wait returned (`copyWith`), which is what the PRS branch already does. No new wait, no
timeout, no polling. With this the create path cannot present a missing collection,
whatever the reader does internally.

Why the re-read can miss the collection (**hypothesis, not reproduced**): a collection
that is only watched for the duration of `waitForState` is moved from the watched set
to the lazy set when the watch is released (`ZkStateReader.removeDocCollectionWatcher`,
line 1860). The lazy set is rewritten by `refreshCollectionList` (line 689,
`retainAll(children)`) under a different lock. If that method holds a children list
fetched before the collection's node was created and applies it after the watch was
released, the collection is in neither set until the next child event re-adds it. I
could not confirm this is the interleaving the reporter hit, and I did not change
`ZkStateReader`: it is shared by everything, and a change there on an unreproduced
reading is a different size of patch. It is listed under "Left unfixed".

## 2. Failure-point map

"Before" is `upstream/main`. Client text is what a synchronous caller receives.

| # | Step (line) | Failure | Before: left behind / client sees | After |
|---|---|---|---|---|
| 1 | Validation (127-151) | exists, alias exists, no config, bad params | nothing / 400 with message | same |
| 2 | `createCollectionZkNode` (171) | ZK error | maybe an empty `/collections/<name>` node / 500 with message | same (see Left unfixed 2) |
| 3 | PRS state build + create (200-203) | bad router/shards, ZK error, node exists | `/collections/<name>` node, empty or holding a full `state.json` if the write landed but its reply was lost / message, or **null message** for a ZK exception | not cleaned (the flag is set only when `create` returns, so a lost reply can leave that `state.json`); message now present |
| 4 | Submit state update, non-PRS (222, 229) | ZK or queue error | node, possibly state / null message for non-Solr exceptions | cleaned up; message |
| 5 | Wait for collection, 30 s (211, 235) | timeout | node; in Overseer mode the queued create can still land later and produce an empty collection / 500 "Could not fully create collection" | cleaned up; same message (see Left unfixed 5) |
| 6 | Re-read state (245) | collection missing (the ticket) | leads to 7b | cannot happen for create |
| 7a | Assignment (250) | `AssignmentException` (not enough nodes, plugin rejection) | cleaned up / 400 with message; delete output mixed into the response | cleaned up; 400, same message; delete output no longer in the response |
| 7b | Assignment (250) | anything else: NPE, plugin bug, `IOException` | **collection with no replicas / null message** | cleaned up; "Could not create collection ..." |
| 8 | Replica loop (302-382) | core-name counter, base URL, state update errors | **collection with partial replica entries** / message or null | cleaned up; message |
| 9 | PRS `setData`, distributed batch (388, 396) | ZK error | **collection with replica entries, no cores** / null message | cleaned up; message |
| 10 | Wait for replica entries, 120 s (412) | timeout | **collection with DOWN replicas, no cores** / 500 with message | cleaned up; same message |
| 11 | Core creation (419-427) | a core fails | cleaned up / 400 "Underlying core creation failed ..." | cleaned up; same text plus the per-node failures |
| 12 | PRS wait for active (431-450) | timeout | cleaned up / same 400 | cleaned up; "... (not all replicas became active)" |
| 12b | same | interrupt | **collection stays** / null message, interrupt flag lost | stays (see Left unfixed 3); message; interrupt flag restored |
| 13 | Cleanup in 7a/11/12 | cleanup throws | whatever cleanup did not reach / **cleanup's error replaces the real one** | original error returned; cleanup error logged and suppressed |
| 14 | Alias (482) | ZK error | working collection, no alias / null message | collection deleted; message (judgment call, see below) |

Async requests go through the same method; the failure is stored as the request's
`exception` entry (`CollectionHandlingUtils.addExceptionToNamedList`, line 484), whose
`msg` was null in the null-message rows above.

## 3. Sibling commands (researched, not changed)

- **`CreateShardCmd` line 151**: same exception-type-keyed cleanup. `DeleteShardCmd`
  runs only for `AssignmentException`; any other failure from `addReplica` leaves the
  new empty shard. Substantiated by reading; a natural follow-up with the same shape
  of fix.
- **`SplitShardCmd` line 829**: same null-message wrap. Its cleanup is already in a
  `finally` keyed on a success flag, so only the message is affected.
- **Wait, then re-read** also appears in `CollectionHandlingUtils.waitForNewShard`
  (line 285-302, used by `CreateShardCmd` and `SplitShardCmd`), `RestoreCmd`
  (lines 261-267, under a comment saying there is no race) and
  `ReindexCollectionCmd` (line 372). In those the collection already exists and only
  a shard or the collection's presence is re-read; I found no path where the re-read
  loses it, but did not prove there is none.
- Other `assign()` callers (`ReplaceNodeCmd` 93, `MigrateReplicasCmd` 106,
  `RestoreCmd` 435, `SplitShardCmd` 565, `Assign.getNodesForNewReplicas` 305) pass the
  real cloud manager with no preceding wait. For them the strategy check gives a clear
  `AssignmentException` in place of an NPE.

## 4. Left unfixed, with reasons

1. The `ZkStateReader` window itself (hypothesis above). Not reproduced; out of
   proportion for this change.
2. Failures before the state write can leave an empty `/collections/<name>` node, and
   an auto-created configset copy when no configset was named. Before the state write
   the command cannot tell its own node from one that was already there, so it does
   not delete.
3. Interrupt, node shutdown or Overseer failover in the middle of a create still
   leaves the collection. The cleanup needs ZooKeeper calls an interrupted thread
   cannot make, so it is skipped rather than attempted.
4. If the cleanup fails (a node is down for the unload, a placement plugin vetoes the
   delete) the collection can remain. It is logged at ERROR.
5. Row 5 in Overseer mode: after the cleanup removes the node, a create message still
   in the queue fails with `NoNode`. By reading `Overseer.ClusterStateUpdater`
   (lines 283-290, 406-408) that is treated as a bad message and dropped. Not run;
   worth a look by someone who knows that loop.
6. A missing or rejected placement is still a 400. Not changed here.
7. Non-PRS only: if the up-front "already exists" check misses an existing collection
   (this node's state view lags ZooKeeper), the two state-update modes diverge. With
   Overseer updates the state update is a no-op and the command continues against the
   existing collection; with distributed updates the state update creates `state.json`
   with a plain `create`, which throws `NodeExistsException`. Either way, a failure
   after that point would once have run the cleanup and deleted a collection this
   command did not create: already true for rows 7a and 11 in the Overseer case, and
   new with this change in the distributed case, where the exception itself triggered
   the cleanup. The command now checks ZooKeeper directly for the collection's
   `state.json` just before the state write is submitted and fails with "collection
   already exists" if one is there, so neither variant reaches the cleanup. The
   command holds the collection lock, so state present at that point is not its own.
   The guard is verified by reading; reaching it needs the reader lag described in
   section 5, which has no light test seam.
8. Row 14: deleting a working collection because its alias could not be created is
   the consistent reading of "a failed create leaves nothing", but it is a choice.
   Say so if you would rather keep the collection in that one case.

## 5. Tests

All on a real `MiniSolrCloudCluster`, no mocks. Failures are injected through
`DelegatingPlacementPluginFactory.setDelegate`, an existing public method, with a
small plugin that throws.

| Test | Window | Expected on `main` |
|---|---|---|
| `CreateCollectionCleanupTest.testCleanupAfterUnexpectedPlacementFailure` | row 7b, PRS and non-PRS at random; also that the same name can be created afterwards | **fails**: null message, collection stays |
| `CreateCollectionCleanupTest.testCleanupAfterPlacementException` | row 7a | passes (guards the reshaped handling) |
| `CreateCollectionCleanupTest.testCreateCollectionCleanup`, `testAsyncCreateCollectionCleanup` (existing) | rows 11, 12, sync and async | pass |
| `PlacementPluginIntegrationTest.testAssignForMissingCollection` | strategy check, non-create caller | **fails**: NPE |

Not covered, and why:

- **The stale re-read (row 6).** Exercising it needs either a stub cloud manager or
  state reader (the mock setup that was rejected on #4997) or a timing-dependent
  cluster test. Neither is written. The `copyWith` line is therefore untested; it is
  three lines and mirrors the PRS branch.
- Timeouts (rows 5, 10), ZK write errors (rows 4, 8, 9), alias failure (row 14) and a
  failing cleanup (row 13). Each needs fault injection into ZooKeeper or the Overseer
  that does not exist as a light seam. They share the one `catch` that row 7b
  exercises.

## 6. Gate

Premise run (expect the two **fails** above on `main` with only the test files
overlaid, for the stated reasons):

```
./gradlew :solr:core:test --tests "org.apache.solr.cloud.CreateCollectionCleanupTest" --tests "org.apache.solr.cluster.placement.impl.PlacementPluginIntegrationTest"
```

Existing suites most likely to notice this change:

```
./gradlew :solr:core:test --tests "org.apache.solr.cloud.OverseerCollectionConfigSetProcessorTest" --tests "org.apache.solr.cloud.DeleteCoreRemnantsOnCreateTest" --tests "org.apache.solr.cloud.CollectionsAPISolrJTest" --tests "org.apache.solr.cloud.api.collections.TestCollectionAPI" --tests "org.apache.solr.cloud.api.collections.CollectionsAPIDistributedZkTest" --tests "org.apache.solr.cloud.api.collections.SimpleCollectionCreateDeleteTest" --tests "org.apache.solr.cloud.CreateRoutedAliasTest" --tests "org.apache.solr.cloud.AliasIntegrationTest" --tests "org.apache.solr.cloud.ReindexCollectionTest" --tests "org.apache.solr.cloud.api.collections.LocalFSCloudIncrementalBackupTest"
```

- `OverseerCollectionConfigSetProcessorTest` runs this command against Mockito mocks.
  Its `waitForState` stub returns null and its `hasCollection` stub returns true after
  the create message, so the new `copyWith` branch should not be entered. If it fails,
  look there first.
- `AliasCmd` line 93 matches on the text "collection already exists"; that message is
  unchanged.
- `DeleteCoreRemnantsOnCreateTest` line 190 matches "Underlying core creation failed";
  the text is kept as a prefix.

Then `./gradlew tidy` and `./gradlew check -x test`. Formatting was done by hand.

## 7. Client-visible changes to mention in a PR

- Unexpected create failures return `Could not create collection <name>: <cause>`
  where they returned an empty message.
- `Underlying core creation failed while creating collection: <name>` is followed by
  the failures in parentheses.
- A create that fails after the state write no longer leaves the collection. This
  includes the two timeouts and an alias failure.
- On a placement rejection the response no longer carries the cleanup delete's output.
