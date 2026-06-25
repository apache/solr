# Recovery Subsystem Code Review

**Date:** 2026-06-24
**Branch:** `ref-branch`
**Review range:** `1130d57a6f5...HEAD` — the recovery / state-plane / tlog / leader-election series
(47 files, ~3,955 insertions; the "StatePlane delta-plane", NRT-tlog-recovery, PeerSync-searcher,
and leader/terms-hardening commits).

## Scope

Recovery and everything that feeds it:

- **Recovery state machine** — `RecoveryStrategy`, `PeerSync` / `PeerSyncWithLeader`
- **tlog / updatelog** — `UpdateLog`, `TransactionLog`, `DirectMemBufferedInputStream`,
  `DefaultSolrCoreState`, `DirectUpdateHandler2`
- **State plane** — `StatePlaneWriter`/`Reader`/`Cursors`, `StateDelta`(`Codec`/`Config`),
  `ShardStateLog`, `StateSnapshot`, `ZkStateWriter`, `StatePublisher`
- **Reader path** — `ZkStateReader`, `ZkStateReaderQueue`, `ClusterState`, `DocCollection`, `Slice`, `Replica`
- **Overseer / election** — `Overseer`, `OverseerElectionContext`, `LeaderElector`,
  `ShardLeaderElectionContext(Base)`, `ZkShardTerms`, `WorkQueueWatcher`
- **Update forwarding** — `DistributedZkUpdateProcessor`, `DistributedUpdateProcessor`, `SolrCmdDistributor`
- **ZK plumbing** — `SolrZkClient`, `SolrZooKeeper`, `ConnectionManager`, `ZkMaintenanceUtils`

## Method

13 independent finder angles (one per subsystem cluster + a removed-behavior auditor + a
concurrency/fencing/incarnation specialist) swept the diff and the enclosing functions; each
candidate got a 1-vote, 3-state verification (CONFIRMED / PLAUSIBLE / REFUTED).

- **First pass (13 angles):** 69 candidates → **18 CONFIRMED, 11 PLAUSIBLE, 39 REFUTED**.
- **Gap-sweep pass (3 lenses, blind to nothing already evaluated):** **+3 CONFIRMED, +1 PLAUSIBLE**
  (see the Gap-sweep addendum below).
- **Totals:** **21 CONFIRMED, 12 PLAUSIBLE.**

The author independently re-verified every CONFIRMED critical/high against source (including the
candidate whose verifier hung — `TransactionLog:1224`, real, promoted to CONFIRMED/high — and the
gap-sweep regression `OverseerTaskProcessor:218`).

**Verdict legend:** `CONFIRMED` = trigger + wrong outcome named and checked against source.
`PLAUSIBLE` = mechanism is real, the trigger is timing/config/upstream-state dependent — each needs
a decision or a repro, not a blind fix.

---

## CONFIRMED — Critical

### C1. Retry after an errored leader-tlog replay drops the preserved buffer → permanent data loss
`solr/core/src/java/org/apache/solr/cloud/RecoveryStrategy.java:843` · durability

`replay()` deliberately keeps the buffer tlog when a leader-tlog catch-up replay errors
(`applyLeaderTlogThenBuffer` only drops it on `recoveryInfo.errors == 0`, `UpdateLog.java:2060-2061`).
But `LogReplayer.run()`'s `finally` flips `ulog` state to `ACTIVE` **on every replay, including the
errored one** (`UpdateLog.java:2158`). On the next `doRecovery` iteration the guard at line 843
(`if (ulog.getState() != BUFFERING) ulog.bufferUpdates();`) now sees `ACTIVE`, so it calls
`bufferUpdates()`, which unconditionally `dropBufferTlog() + deleteBufferLogs()`
(`UpdateLog.java:1851-1852`).

- **Trigger:** NRT replica in `MASTER_VERSION_ZERO` Option-B catch-up. Leader-forwarded updates
  accumulated **only** in the buffer tlog during the prep window. A replay record errors
  (`report.errors > 0`) → `replay()` throws → `successfulRecovery=false` → retry → line 843 drops
  the buffer.
- **Outcome:** those window-forwarded updates exist nowhere else (they were never in the leader's
  tlog/commit point), so they are **permanently lost**; the replica converges to a doc set missing
  them. This defeats the documented "buffer left fully intact (never partial-apply-then-drop)" invariant.
- **Fix direction:** key buffer-preservation off something the replayer's `finally` does not clobber.
  Either don't flip state to `ACTIVE` on an errored/aborted replay, or have the retry re-detect "buffer
  still present" rather than inferring it from `ulog.getState()`. (The `BUFFERING` check is the wrong
  signal because the replayer owns the state transition.)

---

## CONFIRMED — High

### H1. Scoped state-plane recursive watch is never removed (add wraps, remove doesn't)
`solr/solrj/src/java/org/apache/solr/common/cloud/ZkStateReader.java:823` · resource-leak

`armStatePlaneWatch` registers via `zkClient.addWatch(path, this, PERSISTENT_RECURSIVE, …)`, which
internally wraps the watcher: `SolrZkClient.addWatch` stores `wrapWatcher(this)` =
`ProcessWatchWithExecutor(this)` (`SolrZkClient.java:1485/1489`). `unregisterStatePlaneWatch` calls
`zkClient.removeWatches(path, this, …)`, and `SolrZkClient.removeWatches` passes the watcher **raw**
(`:1499/1503`, no `wrapWatcher`). `ProcessWatchWithExecutor.equals` (`:1558`) returns false for any
non-`ProcessWatchWithExecutor` argument, so the stored wrapper never matches `this` → ZK returns
`NoWatcher` (swallowed by the rc callback).

- **Trigger:** a core for collection C moves off the node (`unregisterCore` →
  `releaseLocalInterest` → `unregisterStatePlaneWatch`) while C still exists cluster-wide.
- **Outcome:** the `PERSISTENT_RECURSIVE` watch stays armed forever. The node keeps receiving and
  reprocessing every delta/snapshot/manifest leaf event for a collection it no longer hosts —
  defeating the fanout-reduction goal and leaking one server-side recursive watch per churned
  collection. (On collection DELETE the node deletion auto-clears it, so only local-churn teardown leaks.)
- **Fix direction:** make `SolrZkClient.removeWatches` wrap symmetrically (`wrapWatcher(watcher)`), or
  remove via the same wrapper instance. Given `equals/hashCode` delegate to the inner watcher, wrapping
  on removal is the minimal fix.

### H2. `mkDirs` shares single-slot error-capture arrays across concurrent callbacks → fail-not-closed
`solr/solrj/src/java/org/apache/solr/common/cloud/SolrZkClient.java:828` · concurrency

`mkDirs` fires N async `create`s in a loop, each handed the **same** single-element arrays
`code[0]/path[0]/nodata[0]` (`:823-826`) via `MkDirsCallback` (`:846-847`). The caller resets those
arrays at the **top of each loop iteration** (`:828-830`) while prior iterations' callbacks run
concurrently on the ZK EventThread, and the post-`await` check reads only `code[0]` (`:869`).

- **Trigger:** a non-`NODEEXISTS` failure (NoAuth / InvalidACL / NoNode-on-parent / ConnectionLoss)
  on create K (K < N); iteration K+1's reset, or a later success writing `code[0]=0`, clobbers the
  captured error.
- **Outcome:** `mkDirs` returns "successfully" despite a node that was never created → caller proceeds
  on an incomplete path tree. A torn read can also pair `path[0]` from one create with `code[0]` from another.
- **Fix direction:** capture per-create results (an array indexed by iteration, or a concurrent
  collection of failures), not one shared slot; aggregate after the latch.

### H3. `ZkStateWriter.stop()` final flush is always fenced out → last buffered state lost on clean handoff
`solr/core/src/java/org/apache/solr/cloud/overseer/ZkStateWriter.java:1701` · durability

`Overseer.close()` sets `closed = true` (`Overseer.java:388`) **before** calling
`zkStateWriter.stop()` (`:391`). `stop()`'s "final flush" (`ZkStateWriter.java:1698-1704`) calls
`writeStateUpdatesInternal(stateUpdates.keySet(),1).get(10s)`, which publishes through
`StatePlaneWriter.publish` → `fence.ownsElectionAuthoritative()` → (`OverseerElectionFence`)
`owns && stillElected()`, and `stillElected()` = `!overseer.isClosed()` (`:206`) = **false**.

- **Trigger:** any graceful `Overseer.close()/closeAndDone()` with ≥1 entry still buffered in
  `stateUpdates` (e.g. a freshly published LEADER short-state the Worker had not yet appended).
- **Outcome:** `publish()` throws `FencedException` on every shard, the aggregate future completes
  exceptionally, `.get()` throws, the code logs "did not complete cleanly", and the buffered updates
  are silently dropped. The flush's durability guarantee is never met; recovery depends entirely on
  the next overseer's reconcile.
- **Fix direction:** the final flush must run before ownership is renounced. Either set `closed` after
  `stop()` returns, or give `stop()` a fence bypass for the bounded final-flush window (it is still the
  elected owner until it relinquishes the election znode).

### H4. `ZkShardTerms.scheduleTermNotify` drops a coalesced ZK refresh → stale terms, missed recovery
`solr/core/src/java/org/apache/solr/cloud/ZkShardTerms.java:133` · concurrency

The worker lambda captures `refreshFirst` **once** (`:133`) and re-reads that captured value on every
loop iteration (`:136 if (refreshFirst) refresh();`). A coalesced request that arrives while a worker
runs only sets `checkAgain=true` (`:124-126`) — its `refreshFirst` argument is discarded.

- **Trigger:** a local write keeps a worker running with `refreshFirst=false`
  (`setNewTerms → scheduleTermNotify(false)`), then the leader pushes this replica's term **down** in
  ZK and the persistent watch fires `processEvent → scheduleTermNotify(true)` while the `false` worker
  is still running.
- **Outcome:** the re-loop reuses captured `false`, so `refresh()` is never called; listeners are
  notified against stale in-memory terms (still the old high term), so `RecoveringCoreTermWatcher` is
  not told to recover. The replica stays ACTIVE while genuinely behind — the exact lost-update /
  distrib-query undercount the surrounding comment was added to prevent — until some later
  `NodeDataChanged` happens to coincide with no running worker.
- **Fix direction:** track a pending `refreshFirst` flag (OR-accumulate it) under `ourLock` and consume
  it on each loop iteration, instead of capturing the first caller's value for the worker's lifetime.

### H5. `DirectMemBufferedInputStream` `DataInput` primitives are unbounded → torn-record SIGSEGV / silent garbage
`solr/core/src/java/org/apache/solr/update/TransactionLog.java:1224`
(`DirectMemBufferedInputStream.java:102-188`) · correctness

The torn-trailing-record tolerance added to `LogReader.next()` rests on two guards: `catch(Throwable)`
around `codec.readVal` (`:1225`) and the 4-byte size-trailer check (`:1235`). But the stream's
`DataInput` primitives — `readByte` (`:120`), `readShort` (`:133`), `readInt` (`:157`), `readLong`
(`:170`), `readFully(b,off,len)` (`:102-105`) — do **direct unchecked Agrona `buffer.getX(position)`**
with no bound check against `length` (only `read()`/`read(byte[])` throw `EOFException`). The
constructor bounds the reader at `raf.length()`, not `fos.size()` (the logical written size), and the
mapping has pre-grown slack beyond `raf.length()`.

- **Trigger:** a leader-copied tlog whose trailing record is torn mid-fill (the documented NRT
  catch-up case).
- **Outcome (two paths):**
  1. A garbage length field (e.g. a bogus string/array length) makes `readVal` `readFully` far past the
     mapping capacity → **native SIGSEGV that `catch(Throwable)` cannot catch** → node crash during
     recovery replay. The class's own comment (`TransactionLog.java:1053-1071`) already acknowledges
     "unmapping mid-read is a native SIGSEGV … not a catchable Java exception."
  2. Reads into zero/stale mapped slack (between `fos.size()` and capacity) return silently; zeros
     decode as NULL/empty, so `readVal` returns a structurally-plausible record. Detection then rests
     **solely** on the trailer check — and a slack-derived trailer that happens to match consumed bytes
     accepts a garbage record into replay.
- **Fix direction:** bound every primitive read against `length` and throw `EOFException` (so the
  existing `catch(Throwable)` turns it into a clean EOF), or validate the decoded length fields before
  the bulk read. The trailer check is a second line of defense, not a substitute for bounds checks.

### H6. `removeDocCollectionWatcher` decrements `coreRefCount` unconditionally → double-remove tears down a live watch
`solr/solrj/src/java/org/apache/solr/common/cloud/ZkStateReader.java:2492` · concurrency

The restored decrement `if (v.coreRefCount.get() > 0) v.coreRefCount.decrementAndGet();` (`:2492`)
runs **before and independent of** whether the watcher was actually present (`:2493
v.stateWatchers.remove(watcher)`). Registration incremented `coreRefCount` exactly once, so a
double-remove of the same watcher over-decrements.

- **Trigger:** `waitForState` registers a wrapper (`coreRefCount++`); the predicate already matches, so
  `registerCollectionStateWatcher` auto-removes it (decrement #1), then `waitForState`'s `finally`
  removes it again (decrement #2) while another live core keeps the `CollectionWatch` alive.
- **Outcome:** `coreRefCount` is driven to 0 while a core is still registered; `canBeRemoved` becomes
  true, the collection watch + state-plane interest are torn down and C is demoted to lazy, stalling
  live-state delivery to that core.
- **Fix direction:** only decrement when the watcher was actually present
  (`if (v.stateWatchers.remove(watcher) && v.coreRefCount.get() > 0) v.coreRefCount.decrementAndGet();`),
  making register/unregister strictly symmetric.

---

## CONFIRMED — Medium

### M1. PeerSync buffer-replay path ignores `report.errors` → publishes ACTIVE with dropped updates
`solr/core/src/java/org/apache/solr/cloud/RecoveryStrategy.java:1216` · durability

The leader-tlog path was tightened to `report.failed || (leader != null && report.errors > 0)`, but on
the PeerSync buffer-replay path `leader == null`, so a per-record error (`recoveryInfo.errors++` without
`failed`, `UpdateLog.java:2146`) does **not** throw. The replica opens a searcher and publishes ACTIVE
with that buffered update silently dropped (the buffer is also dropped — `applyBufferedUpdates` only
checks cancel/closed, not errors, `UpdateLog.java:1974`). **Fix:** apply the same `errors > 0` gate on
both replay paths.

### M2. `ZkStateWriter.init()` discards the reconcile-publish future → reconciled LEADER stranded on transient failure
`solr/core/src/java/org/apache/solr/cloud/overseer/ZkStateWriter.java:1466` · durability

On overseer takeover, `reconcileLeadersFromZk` arms the ephemeral-leader id and calls
`writeStateUpdatesInternal(singleton(id),1)` fire-and-forget. A transient publish failure (ZK
ConnectionLoss, manifest-seed failure, fence not yet owning during early takeover) re-arms
`pendingChangedIds` but schedules **no retry**, and reconcile is not queue-driven. In a quiescent
cluster (exactly the scenario the `init` comment describes) the leader never reaches the delta plane
and readers hang in `getLeader…`. **Fix:** await/inspect the future and retry, or enqueue a
queue-gated reconcile item.

### M3. Coupled per-item durability futures in the Worker batch → unrelated durable items reprocessed forever
`solr/core/src/java/org/apache/solr/cloud/overseer/WorkQueueWatcher.java:242` · durability

When the ZkStateWriter Worker coalesces WriteUnits for collections A and B into one bulkMessage, B's
publish failure makes `CompletableFuture.allOf` (`ZkStateWriter:1821`) fail, and the Worker
(`:1895-1902`) `completeExceptionally`s **both** done futures. WorkQueueWatcher then leaves A's queue
item undeleted even though A's state was durably appended; if B never lands, A's item is reprocessed
every watch cycle (hot unbounded reprocess loop). **Fix:** track durability per WriteUnit, not per
coalesced batch — complete each unit's future on its own collection's outcome.

### M4. `StatePublisher.close()` during a ZK outage abandons parked unpersisted batches
`solr/core/src/java/org/apache/solr/cloud/StatePublisher.java:549` · durability

If batches are parked in `pendingBatches` and ZK is unreachable, the worker loops in
`flushPendingBatches()` throwing `ConnectionLoss` → run-loop catch (`:203`) `waitForConnected()` →
repeat; it **never reaches `workQueue.poll()`** to consume the TERMINATE pill, so `terminated` stays
false. `close()` puts the pill, `awaitTermination(5s)` times out, returns, and the thread is later
killed with `pendingBatches` non-empty → the parked edge-triggered transitions are lost. Contradicts
the fail-closed intent. **Fix:** make the flush loop check `terminated`/`closed` and stop parking-retry
on shutdown, or drain-with-deadline before declaring closed.

### M5. `LogReader` constructor leaks refcount + active-reader gauge if the stream ctor throws
`solr/core/src/java/org/apache/solr/update/TransactionLog.java:1144` · resource-leak

`incref()` (`:1140`) and `incrementActiveReaders()` (`:1144`) run before the `fosLock` try block; if
`new DirectMemBufferedInputStream(buffer, raf.length())` throws IOException (closed channel during a
concurrent `forceClose`, or FS error) the ctor rethrows `RuntimeIOException` without `close()`. The
tlog refcount never reaches zero (mapped-file unmap blocked) and the `activeReaders` gauge is inflated
forever (eventually forcing `forceClose` to always hit its 5s timeout path). **Fix:** acquire the
incref/gauge inside the try, or roll them back in the catch before rethrowing.

### M6. Leftover `FLT-INVESTIGATION` debug block in the leadership-commit path
`solr/core/src/java/org/apache/solr/cloud/ShardLeaderElectionContext.java:380` · removed-behavior

A block self-labeled *"FLT-INVESTIGATION (temporary; revert before commit)"* shipped. On **every**
become-leader it emits a `log.error` and builds `RecentUpdates`
(`getUpdateHandler().getUpdateLog().getRecentUpdates()` — no null guard on `getUpdateLog()`, NPE for a
core with a null ulog, swallowed and logged), calling `ru.getVersions(1)` twice. Paired
`FLT-BUMP`/`FLT-BUMP-SKIP` error logs in `DistributedZkUpdateProcessor.finish` fire on every batch with
failed forwards. In a restart/chaos workload this floods error logs and adds tlog-reader churn on the
hot election path. **Fix:** delete the instrumentation as the comment instructs.

### M7. `armStatePlaneWatch` re-arm misses collections cached without an active `collectionWatch`
`solr/solrj/src/java/org/apache/solr/common/cloud/ZkStateReader.java:559` · removed-behavior

After the broad `/collections` watch was downgraded to non-recursive `PERSISTENT` and scoped watches
are re-armed on reconnect **only** from `collectionWatches.keySet()`, a collection left in
`watchedCollectionStates` without a `collectionWatch` entry (interest released by design) still passes
`process()`'s gate (which checks `watchedCollectionStates.containsKey`) but receives **no** leaf events
— the broad watch is non-recursive and no scoped watch is armed; reconnect never re-arms it. A consumer
reading the still-cached entry (e.g. `getCollectionRef`, which consults the watched map first) sees
indefinitely stale state. **Fix:** gate the cached entry's lifetime on having an active scoped watch, or
re-arm from `watchedCollectionStates.keySet()` too.

---

## CONFIRMED — Low

### L1. `StatePublisher` run-loop hot-spins on a terminal non-ConnectionLoss `KeeperException`
`solr/core/src/java/org/apache/solr/cloud/StatePublisher.java:205` · correctness

A `KeeperException` that is **not** `ConnectionLossException` (e.g. a `SessionExpired` not wrapped by
`ZkCmdExecutor`, or `NoAuth`) rethrown from `flushPendingBatches()` misses the `ConnectionLoss` catch
(`:203`) and lands in the generic `catch (Exception)` (`:206`), which only logs. The loop immediately
retries with no `waitForConnected()` and no backoff → tight busy-loop until the session recovers.
**Fix:** add a bounded backoff (or `waitForConnected`) on the generic path.

### L2. `DefaultSolrCoreState` write-lock-release catch is dead for the documented case
`solr/core/src/java/org/apache/solr/update/DefaultSolrCoreState.java:357` · altitude

The new try/catch that releases the writeLock when `changeWriter` throws never fires for the
close-old-writer IOException it claims to guard: `changeWriter` swallows commit/rollback exceptions via
`ParWork.propagateInterrupt` and only the `openNewWriter` branch (not taken here, `openNewWriter=false`)
can throw. The comment overstates the protection. **Fix:** correct the comment, or make `changeWriter`
actually propagate the close failure if the guard is meant to cover it.

### L3. `forceClose()` leaks the `activeReaders` gauge for abandoned wedged readers
`solr/core/src/java/org/apache/solr/update/TransactionLog.java:1083` · resource-leak

`forceClose()` (the deliberate final-shutdown SIGSEGV escape hatch) resets refcount to 0 and unmaps
without decrementing `activeReaders` for the still-active readers it abandons. After a forced close the
process-wide `activeReaders` gauge stays > 0 for the rest of the JVM, misrepresenting live readers.
**Fix:** zero/adjust the gauge for the abandoned readers in `forceClose` (metric-only correctness).

### L4. Redundant `id != null` guard on the dedup hot path
`solr/core/src/java/org/apache/solr/cloud/StatePublisher.java:438` · altitude

Line 424 already throws when `id` is null, so `id` is provably non-null at line 438; the
`id != null &&` conjunct is unreachable-false. Drop it to clarify the LEADER-exclusion intent.

---

## PLAUSIBLE — needs a decision or a repro

These have a real mechanism; the trigger is timing/config/upstream-state dependent. Treat as
"investigate", not "patch blindly".

| # | Location | Concern |
|---|----------|---------|
| P1 | `ZkStateWriter.java:562` (`drainPendingLeaderDemotions`) · high | When ≥2 LEADER promotions for one slice were deferred while the `DocCollection` was absent, the pending set is an **unordered** `ConcurrentHashMap` keySet with no recency record; drain keeps whichever id it visits first, so a **stale** leader can win the in-memory single-leader survivor (served by `Slice.getLeader`) and be republished. Needs: confirm A→B→C deferral with no intervening reconcile is reachable. |
| P2 | `DistributedZkUpdateProcessor.java:1391` · high | Failed forwards to a replica `ZkShardTerms` marks `isRecovering` are intentionally **not** term-bumped ("recovery will reconcile it"). But `RECOVERY_FAILED` leaves the `_recovering` marker set (never cleared by `ZkController.publish`), so a follower whose recovery already exited stays behind with no re-trigger. Needs: confirm the marker lingers after `recoveryFailed()`. |
| P3 | `DefaultSolrCoreState.java:914` · medium | `close()`'s commit-on-close proceeds even when the bounded `writeLock` `tryLock(10s)` **times out**, so `closeIndexWriter` can commit/close the IndexWriter concurrently with an in-flight update thread still using it (`AlreadyClosedException` / torn final commit). Documented as accepted residual; flagged because it can corrupt final commit state. |
| P4 | `RecoveryStrategy.java:985` (`replicaDivergesFromLeader`) · medium | The divergence guard **fails open** (returns false → ACTIVE) on **any** exception, including a transient leader-fingerprint-fetch error. Under sustained indexing a real divergence whose probe errors publishes ACTIVE divergent from the leader; an NRT replica has no poller to reconcile → permanent stale serving. Needs: confirm catch-all at the fingerprint fetch. |
| P5 | `RecoveryStrategy.java:985` · low | Converse of P4: when the data-bearing peer is up but the replica diverges, divergence-driven `stayRecovering` can burn the whole bounded retry budget on repeated full-copy fetches if indexing never quiesces, then publish `RECOVERY_FAILED` on a replica whose only fault is a moving leader tail. Consider a "near-consistent, keep RECOVERING" terminal instead. |
| P6 | `ZkShardTerms.java:427` (`saveTerms`) · medium | `saveTerms` now throws `BadVersionException` when in-memory version drifts **ahead** of ZK, but `setNewTerms` refuses to lower the version, so a cached `ZkShardTerms` not invalidated on same-name collection recreation (incarnation reuse) wedges **all** term writes for that shard forever. Needs: confirm the cached instance survives recreation without `collectionToTerms.remove`. |
| P7 | `TransactionLog.java:1389` (`FSReverseReader`) · medium | The reverse reader has **no** torn-trailing-record tolerance (unlike the hardened `LogReader.next()`): it seeds `nextLength` from the absolute file tail and reads `codec.readVal` at the derived `recordStart`, so a torn tail yields garbage `nextLength` → mis-seek past a valid record or decode garbage as the most-recent update (feeding a bogus version into peersync/RTG). Mirror the `LogReader` hardening (and H5's bounds checks) here. |
| P8 | `ZkStateWriter.java:1087` (`markReplicasDown`) · medium | Calls `writeStateUpdatesInternal(...)` from **inside** the `stateUpdates.compute()` lambda for the same collId; the scheduled task later does `stateUpdates.compute(collId)` and can park on the CHM bin lock still held by the outer compute (lock-ordering inversion under churn), and the discarded future means a failed DOWN append is never retried (watch-driven, no queue gate). |
| P9 | `PeerSyncWithLeader.java:324` · low | Skipping a null (trimmed) buffered update is **fail-open** when `-Dsolr.disableFingerprint=true`: `doSync` returns true at `:262` with no fingerprint verification, so the replica registers ACTIVE missing the buffered update. Gated on the disable flag (default `doFingerprint=true` catches it at `:260`). |
| P10 | `StatePlaneWriter.java:313` (`computePendingDemotions`) · low | The O(1) `currentLeaderId` index demotes only **one** leader; if the per-shard snapshot ever holds two raw LEADERs (upstream invariant violation), the persisted ring keeps the second LEADER until a reader rewrites it (the reader's `applyRing` path self-heals via a full scan). Defense-in-depth lost vs. the old full-map scan. |
| P11 | `RestoreCmd.java:393` · low | The per-slice `ZkShardTerms` register/bump is in a try-with-resources whose catch only **logs a warning and proceeds** to add empty replicas; if the term bump throws, the empty replicas register at term 0, short-circuit recovery, and publish ACTIVE serving 0 docs — exactly the divergence the block was added to prevent, re-enabled on the error path. |

---

## Gap-sweep addendum — defects the first pass missed

A second pass ran three gap-finder lenses (moved-code/footguns, setup-teardown/config/exception
handling, cross-subsystem handshakes) over the **same range**, each given the full list of the 69
already-evaluated candidates so it could only surface genuinely new defects. Four survived
verification.

### S1. `getLeaderId` sorts leader children with `getSeq`, which throws on bare-id nodes → OVERSEERSTATUS 500 (regression)
`solr/core/src/java/org/apache/solr/cloud/OverseerTaskProcessor.java:218` · correctness · **High (always triggered)**

`getLeaderId` now calls `LeaderElector.sortSeqs(children)` (`:218`) on the children of
`/overseer/overseer_elect/leader`. `sortSeqs` orders via `Comparator.comparingInt(getSeq)`, and
`getSeq` (`LeaderElector.java:307-315`) throws `IllegalStateException` when the name doesn't match
`LEADER_SEQ = ".*?/?.*?-n_(\d+)"` (`:77`). But the leader registration node is created with
`zkClient.mkdir(leaderPath + '/' + id, …)` (`ShardLeaderElectionContextBase.java:140`) — a **bare
id** (the overseer's internal id / port, e.g. `"54065"`), **not** a `-n_` sequence node.
`getLeaderId` catches only `NoNodeException` (`:220`), so the `IllegalStateException` propagates.

- **Trigger:** any `getLeaderId`/`getLeaderNode` call while an overseer leader exists — i.e. the
  normal steady state. `OVERSEERSTATUS` → `OverseerStatusCmd.call` → `getLeaderNode` → `getLeaderId`
  → `sortSeqs(["54065"])` → `getSeq("54065")` → no match → throws → **uncaught 500**.
- **This directly re-breaks the documented fix.** AGENTS.md records that `getLeaderId` was *"rewritten
  to read the ephemeral child node name … Fixes OVERSEERSTATUS 500."* The `sortSeqs` call was added
  later (commit `8d94201eaa7`, "Address review.md findings Parts A-H") to handle two transient
  children during handoff — but the mechanism is wrong for these non-sequence nodes.
- **Fix direction:** don't `sortSeqs` here. If two transient children must be disambiguated, sort the
  bare ids numerically (or pick by ephemeral `czxid` via `getChildren`+`Stat`), not by `getSeq` which
  only parses election-**queue** `-n_` nodes. (`getSortedElectionNodes` is the wrong template — it
  operates on the queue path.)

### S2. Narrowed forward-skip guard forwards live updates to RECOVERY_FAILED replicas → stale replica keeps `term==max`
`solr/core/src/java/org/apache/solr/update/processor/DistributedZkUpdateProcessor.java:864` · correctness · **High**

The forward-skip guard was narrowed to skip only not-live/DOWN replicas (`:864`); the prior guard also
skipped `RECOVERING` **and** `RECOVERY_FAILED`. Forwarding to `RECOVERING` is intentional (the buffer
fills and replays), and the new comment justifies only that case — but it **silently also un-skips
`RECOVERY_FAILED`**, for which nothing replays the buffer. `recoveryFailed()`
(`RecoveryStrategy.java:229-238`) publishes `RECOVERY_FAILED` and closes the strategy but **lowers no
term**, so `skipSendingUpdatesTo` = `!haveHighestTermValue()` is false (term still `maxTerm`).

- **Outcome:** the leader keeps forwarding to a `RECOVERY_FAILED` replica; the receiver buffers (and
  never replays) or applies onto a stale index and returns **success**, so the leader never adds it to
  `replicasShouldBeInLowerTerms` and never demotes its term. The replica sits at `term==maxTerm` with a
  stale/empty index; if the data-bearing leader then dies, `ShardLeaderElectionContext`'s
  `onlyLiveReplica` bypass lets it win leadership → **SOLR-9504-class data loss**.
- **Relationship to P2:** P2 (`DistributedZkUpdateProcessor:1391`) is the *other half* — even when a
  forward fails, a still-`isRecovering`-marked failed replica isn't term-bumped. Both stem from the
  `RECOVERY_FAILED` replica retaining `term==maxTerm` with no demotion path.
- **Fix direction:** keep forwarding to `RECOVERING` (buffer+replay) but re-exclude `RECOVERY_FAILED`
  from forwarding, or demote a replica's term when it enters `RECOVERY_FAILED` so it can no longer win
  the `onlyLiveReplica` election.

### S3. MODIFYCOLLECTION structure branch records durability unconditionally while the write exception is swallowed → fail-open
`solr/core/src/java/org/apache/solr/cloud/overseer/WorkQueueWatcher.java:192` · durability · **High**

The structure-change branch (MODIFYCOLLECTION from Reindex/Migrate/Restore/Split, ADDROUTINGRULE) runs
`applyCollectionStateUpdate(op, message)` then **unconditionally** records
`durableByKey.put(key, CompletableFuture.completedFuture(null))` (`:190-192`). But
`applyCollectionStateUpdate` (`:303-313`) does `overseer.writePendingUpdates(collection).get()` inside a
`catch (Exception){ log.error(...) }` — and `ZkStateWriter.writeStructureUpdates` completes that future
**exceptionally** on shutdown-during-write (`:1146-1148`), null reader/zkclient (`:1116/1121`), or any
synchronous throw. The `ExecutionException` is swallowed, the method returns normally, line 192 records
a *completed* future, and the second pass deletes the ZK queue item.

- **Outcome:** the structure change (e.g. `readOnly=true` for a reindex, a migrate routing rule) is
  lost permanently with no reprocess — the exact silent loss the per-item durability gate (M3) was
  added to prevent, but the **structure branch bypasses it**. Distinct from M3 (the `allOf`-coupling
  defect at `:242`).
- **Fix direction:** record the **real** future from `applyCollectionStateUpdate` (propagate it instead
  of swallowing), so a failed structure write leaves the queue item for reprocess.

### S4. `validateIdentity` mismatch is classified non-transient → reader stays stale until an unrelated watch
`solr/solrj/src/java/org/apache/solr/common/cloud/StatePlaneReader.java:448`
(`ZkStateReaderQueue.java:266,298-303,485-497`) · correctness · **PLAUSIBLE / medium**

During a same-name collection recreate window, the reader can read the fresh `state.json` (new
`dc.getId()`) while a per-shard ring/snapshot node still physically carries the prior incarnation's
`collectionId`. `ring.validateIdentity(dc, shard)` then throws `IllegalStateException`
(`ShardStateLog.java:63-66`, also `StateDelta`/`StateSnapshot`), wrapped as `SolrException` by
`getAndProcessDeltaUpdates`. The retry classifier `isTransientZkFailure` (`:485-497`) matches only ZK
connection/timeout/session exceptions, so `scheduleRetryOnTransientFailure` (`:298-303`) **drops the
fetch-retry budget** and schedules no re-fetch.

- **Outcome:** the collection's live state stays stale (`getLeaderRetry` can spin to timeout) until some
  later, unrelated watch event re-triggers a fetch — no proactive convergence on this fail-closed path.
- **Confirm by:** reproducing the recreate window where the ring node lags the new `state.json`.
- **Fix direction:** treat an incarnation/identity mismatch as a (bounded) retryable condition that
  re-fetches both nodes, rather than a terminal drop.

---

## Cross-cutting themes

1. **Fail-open on the error/exception branch.** The strongest recurring pattern: paths that are
   correct on the happy branch silently proceed-to-ACTIVE or return success when a sub-step errors —
   C1, M1, H3, **S2, S3**, P2, P4, P9, P11. The fork's stated invariant is *fail closed*; several
   error branches don't.
2. **Discarded / swallowed futures on reconcile/down/structure paths.** M2, M3, **S3**, P8 — async
   state-plane or structure work is launched fire-and-forget (or its `.get()` exception is swallowed)
   with no retry and no queue gate, so a transient failure during takeover / node-down / MODIFYCOLLECTION
   strands live state with nothing to flush it.
3. **add/remove and incref/decref asymmetry.** H1 (wrap on add, raw on remove), H6 (decrement not
   gated on presence), M5 + L3 (gauge/refcount incremented before the failure point or not balanced on
   the abandon path). Each is a one-line symmetry fix.
4. **Unbounded reads over the mapped tlog.** H5 + P7 — the torn-record hardening added a
   `catch`+trailer check but left the underlying `DataInput` primitives unbounded, so the guards don't
   cover the native-crash and stale-slack cases.
5. **State signal read from the wrong owner.** C1 (buffer presence inferred from `ulog.getState()`,
   which the replayer owns) and H4 (`refreshFirst` captured once for the worker's life) both key
   behavior off a value another actor mutates.

## Suggested order of attack

1. **C1** — silent data loss on a normal NRT recovery retry. Highest impact.
2. **S1** — `getLeaderId` regression: **always triggered**, re-breaks OVERSEERSTATUS (a documented
   fix), one-line revert of the bad `sortSeqs` call. Cheapest high-value fix.
3. **H5 / P7** — a node-killing SIGSEGV (and silent corruption) on tlog replay during recovery.
4. **S2 + P2** — `RECOVERY_FAILED` replica keeps `term==max` and can win leadership (SOLR-9504-class
   data loss); fix the forwarding guard and the term-demotion path together.
5. **H3, H1, H6, H4** — handoff/watch/term correctness; all small, well-scoped fixes.
6. **H2, S3** — fail-not-closed paths (`mkDirs`; MODIFYCOLLECTION structure write).
7. **M-series** then **P-series / S4** (these need a repro/decision first).

*39 candidates were verified and REFUTED (intentional fork design — state-plane-only live state,
`LEADER`→`ACTIVE` collapse, replica-name-is-core-name, fences/no-ops with explanatory comments — or
guarded elsewhere); they are not listed.*
