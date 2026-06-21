# Code Review — last 7 commits (`fe7db0a5da7..446b420813d`) + uncommitted working tree

**Reviewer:** Claude (high-effort, recall-biased). Findings ranked most-severe first.
Confidence tags: **CONFIRMED** = I read the code and the bug is constructible as stated;
**PLAUSIBLE** = surfaced by a focused finder agent, line-cited, consistent with the code but
not exhaustively re-verified line-by-line by me.

## Scope & method

The 7 commits under review:

| Commit | Size | What |
|---|---|---|
| `fe7db0a5da7` | **13,097 files, +168k / −1.58M** | The fork-base — the entire Solr fork |
| `84a26c455f6` | 7 files | slow-test outliers: ZkShardTerms spin, SSL HTTP/2 close deadlock, ZK fast-close |
| `78f5989efbc` | 1 file | AGENTS.md (docs only — not reviewed) |
| `712895f4204` | 3 files | outliers: TestSolrXml, TestDocBasedVersionConstraints, ZkCLITest |
| `bcf712a8857` | 3 files | flaky: newStringField Store.YES, async audit drain, audit asserts |
| `1130d57a6f5` | 1 file | rework `beast` gradle task |
| `446b420813d` | 4 files | **PeerSync recovery stale-searcher fix** (production) |

Plus **uncommitted** working-tree changes in `SolrCloudTestCase.java` and
`TestPullReplicaErrorHandling.java`.

The fork-base commit is the whole codebase, so it cannot be read line-by-line. I reviewed it
the way it matters: by fanning out focused finder agents over the **hot, correctness-critical
subsystems** that AGENTS.md flags as the fork's risky departures — the Replica state model,
`ZkStateWriter`/mutators, leader election, the memory-mapped `TransactionLog`/`JavaBinCodec`,
and the HTTP/2 SolrJ client buffer lifecycle — then verified the most severe claims against
the live source myself. The smaller commits I read in full.

---

## A. Production correctness bugs in the fork-base hot subsystems

### A1 — TransactionLog ADD path publishes the write position before writing the bytes (torn read) — **CONFIRMED, HIGH**
`solr/core/src/java/org/apache/solr/update/TransactionLog.java` ~534–561 (`write(AddUpdateCommand, long)`)

Under `fosLock` the ADD path reserves the record region by advancing `fos.position(pos + lastAddSize + 4)`
(publishing the new `fos.size()`), **then releases `fosLock`** and only afterwards copies the
bytes (`fos.putBytes` / `fos.putInt`) while holding just `mapLock.readLock()` (shared, does not
exclude other readers). `LogReader.next()` gates on `pos >= fos.size()` under `fosLock`, so it
will see the reserved-but-unwritten region as available and decode it under `mapLock.readLock()`.

`writeDelete` / `writeDeleteByQuery` / `writeCommit` do **all** of `putBytes` + `putInt` +
`position` advance *inside* `fosLock` — only the ADD path has this hole.

**Failure scenario:** realtime-get / PeerSync / recovery replay reading the tlog concurrently
with active indexing decodes a half-written (or zero-page / stale-reuse) ADD record → garbage
`SolrInputDocument`, or a wrong 4-byte size trailer that desyncs the sequential reader for every
subsequent record. **Fix:** copy the bytes before releasing `fosLock` (mirror the delete path).

### A2 — `ZkStateWriter.enqueueStateUpdates` slice-state branch NPEs on an unknown collection id — **CONFIRMED, HIGH**
`solr/core/src/java/org/apache/solr/cloud/overseer/ZkStateWriter.java` ~347–354

```java
String collection = idToCollection.get(collectionId);   // may be null
DocCollection docColl = cs.get(collection);              // → null
for (StateUpdate update : updates) {
    Slice slice = docColl.getSlice(update.sliceName);    // NPE — no null guard
```

The replica-state branch directly above guards `dc != null`; this slice-state branch does not.

**Failure scenario:** an UPDATESHARDSTATE slice update arrives for a collection a freshly-elected
overseer has not yet loaded into `cs`/`idToCollection` → NPE aborts the whole `enqueueStateUpdates`
call, dropping every batched replica-state update that came with it. **Fix:** `if (docColl == null) continue;`

### A3 — `ReplicaMutator.updateState` dereferences a possibly-null collection — **CONFIRMED, MEDIUM-HIGH**
`solr/core/src/java/org/apache/solr/cloud/overseer/ReplicaMutator.java` 161–162

```java
DocCollection collection = clusterState.getCollectionOrNull(collectionName);
if (collection.getReplica(coreName) == null) {   // NPE if collection == null
```

Line ~196 later does `collection != null ? collection.getSlice(...) : null` — the author knew
`collection` is nullable, but the earlier deref is unconditional.

**Failure scenario:** a state message for a concurrently-deleted collection → `getCollectionOrNull`
returns null → NPE instead of the intended graceful "replica does not exist" return.

### A4 — `ReplicaMutator.updateState` keys on the stripped `CORE_NAME_PROP` — **CONFIRMED bug / PLAUSIBLE reachability, MEDIUM**
`solr/core/src/java/org/apache/solr/cloud/overseer/ReplicaMutator.java` 159

`coreName = message.getStr(ZkStateReader.CORE_NAME_PROP)` then used as the replica key
(`collection.getReplica(coreName)`, `slice.getReplica(coreName)`). Per the fork's core invariant
`CORE_NAME_PROP` no longer exists, so `coreName` is null → `getReplica(null)` → null → the method
logs "replica does not exist" and **silently drops the state update**. This is exactly the
`replica.getName()` vs `CORE_NAME_PROP` anti-pattern AGENTS.md warns about, left unported in this
class. Severity depends on whether any live dispatch still routes a STATE message through
`ReplicaMutator.updateState` (the StateUpdates channel replaced most of this) — **worth confirming
against the live `WorkQueueWatcher` path**; if reachable it is a silent state-loss bug.

### A5 — `Replica.equals()` throws NPE on a null-id (synthetic) replica — **CONFIRMED, MEDIUM**
`solr/solrj/src/java/org/apache/solr/common/cloud/Replica.java` 272

```java
return name.equals(replica.name) && id.equals(replica.id) && collectionId.equals(replica.collectionId);
```

The ctor (236–238) explicitly allows `id == null` for "bare/synthetic election-only" replicas, and
`hashCode` uses null-safe `Objects.hash(name, id, collectionId)` — so such a replica can be stored
in a hash structure, and a later bucket-collision `equals` does `id.equals(...)` → NPE.
Also `getId()` (281) returns the literal string `"<collId>-null"` for these, which collides all
id-less replicas of a collection onto one key if used as a map key. **Fix:** `Objects.equals(id, replica.id)`.

### A6 — `Http2SolrClient` response-side pooled buffers are never returned to the pool — **PLAUSIBLE, MEDIUM (resource/efficiency, not a crash)**
`solr/solrj/src/java/org/apache/solr/client/solrj/impl/Http2SolrClient.java` 579–640, 845–890

The response buffer is acquired via `ExpandableBuffers.getInstance().acquire(...)` (579/845) and
handed to `SolrBufferingResponseListener` / `SolrFutureResponseListener`, but the `finally` blocks
only call `freeBuffer()`, which releases the **request-body** buffer, not the response buffer; the
listeners never release. The request-body paths (468/1060/1096) *do* free via `onComplete`, so the
asymmetry is real.

Severity note: `ExpandableBuffers.getInstance()` is a Jetty `ArrayByteBufferPool` — un-released
direct buffers are reclaimed by their Cleaner on GC, so this is **lost pooling / extra allocation
+ GC pressure under load**, not the unbounded native-memory leak it might first appear to be.
Still worth fixing for symmetry and throughput on hot request paths (recovery commits, forwarded
updates, admin calls).

### A7 — `ZkStateWriter.write()`: structure-change can be lost under concurrent writers — **PLAUSIBLE, MEDIUM**
`solr/core/src/java/org/apache/solr/cloud/overseer/ZkStateWriter.java` ~637–657, 710–726

`writeStructureUpdates` wraps `write()` in a `do/while` catching `BadVersionException`, but the
actual ZK write is async `setData(..., version = -1, callback)` — version `-1` never throws
BadVersion, so the retry catch is dead code and there is no optimistic-concurrency guard.
`dirtyStructure.remove(coll)` (710) is cleared *before* the async write completes. Two overseer
threads racing `write(coll)`: T1 clears dirty + schedules setData(S1); a new change re-marks dirty;
T2 schedules setData(S2); ZK applies them in arbitrary order with version `-1`, so S1 can land last
and the S2 change is lost with `dirtyStructure` already cleared. **Fix:** clear `dirtyStructure`
only after the write completes, or serialize per-collection writes.

### A8 — `LeaderElector.retryElection` doesn't await the in-flight election task before starting a new one — **PLAUSIBLE, MEDIUM**
`solr/core/src/java/org/apache/solr/cloud/LeaderElector.java` ~611–627

`retryElection` does `closeQuietly(this)` (cancels context) + `oldExecutor.shutdownNow()` (interrupts
but does **not** join the in-flight task), then immediately `joinElection(...)` on a fresh executor.
If the old task was mid-`runLeaderProcess` (ephemeral leader/lock node created, not yet published),
the abandoned old task and the new task can briefly race over the same context/ZK nodes — and the
old task unwinding can call `cancelElection`, deleting the lock the new task just relied on.
**Fix:** bounded-await the old task before submitting the new one.

### A9 — Shard-leader singleton lock can leak on an exception path → shard leaderless until session timeout — **PLAUSIBLE, MEDIUM**
`solr/core/src/java/org/apache/solr/cloud/ShardLeaderElectionContextBase.java` ~126–154

If the ephemeral `singletonPath` mkdir succeeds (`heldSingleton = true`) but the subsequent
`leaderPath + '/' + id` mkdir throws anything other than the two caught ZK exceptions, the generic
`catch` returns `false` **without releasing the singleton lock**. The lock is only released in
`cancelElection`. Until this replica's ZK session ends, no other replica can win (`mkdir` → NodeExists
→ `ours == false`). **Failure scenario:** transient ZK hiccup on the second mkdir → shard stuck
leaderless for the full session timeout while the replica is alive and re-joining.

### A10 — `Replica.write()` mutates a shared, non-thread-safe `propMap` during serialization — **PLAUSIBLE, LOW-MEDIUM (by-design but racy)**
`solr/solrj/src/java/org/apache/solr/common/cloud/Replica.java` 342

`write()` does `propMap.put(STATE_PROP, getState().toString())` on a `fastutil` map that the class
documents as **shared across DocCollection graphs**, with no synchronization, while another thread
may be reading/serializing the same map (CLUSTERSTATUS concurrent with overseer state writes).
The re-inserted comment explains *why* the put is needed (so serialized state isn't omitted), but
the unsynchronized mutation of a shared open-hash map can corrupt it (lost entry / resize race) or
serialize a torn state. Consider serializing onto a private copy of the props rather than mutating
the shared map.

---

## B. Findings in the recent (small) commits

### B1 — `AuditLoggerPlugin.close()` can now block shutdown up to 30s (was ~250ms) — **CONFIRMED, MEDIUM** (commit `bcf712a8857`)
`solr/core/src/java/org/apache/solr/security/AuditLoggerPlugin.java` (close / `waitForQueueToDrain`)

The drain-before-stop reordering is correct and fixes the dropped-events bug. But the timeout also
changed from an effective ~250ms to a true 30s, and the drain runs the runner thread live. If the
audit sink (`audit(event)`) is slow or hung, `auditsInFlight` stays > 0 and `close()` now blocks the
**full 30s** on every node/core shutdown. Previously misbehaving-sink shutdown was ~250ms. This is a
deliberate completeness/latency trade-off but is a real operational behavior change worth calling out;
consider a shorter bound (e.g. 5–10s) for the close path.

### B2 — `RecoveryStrategy.openSearcherAfterPeerSync` swallows a commit failure, then still goes ACTIVE — **CONFIRMED, MEDIUM** (commit `446b420813d`)
`solr/core/src/java/org/apache/solr/cloud/RecoveryStrategy.java` ~83–94

The fix is correct for the success case. But the soft-commit is wrapped in `catch (Exception) { log.warn(...) }`,
after which `successfulRecovery = true` is set unconditionally and the replica registers ACTIVE. If the
commit throws (heavy load, core closing mid-recovery), the replica goes ACTIVE with a **stale searcher** —
re-introducing the exact "expected N but was <N" bug this commit fixes, now silently behind a warn.
Consider failing the PeerSync branch (fall through to replication recovery) if the searcher reopen fails.

### B3 — (Uncommitted) `waitForAllReplicasDocCount` "commit nudge" can mask the very bug it is meant to detect — **CONFIRMED, MEDIUM (altitude)** 
`solr/test-framework/src/java/org/apache/solr/cloud/SolrCloudTestCase.java` ~567–580 (working tree, not committed)

This helper was added in `446b420813d` as defense-in-depth to *detect* a follower that has applied
docs but not reopened its searcher (precisely the A2/PeerSync class of bug). The uncommitted change
makes it, after 5s of non-convergence, issue `cluster.getSolrClient().commit(collection)` — which
forces a searcher reopen and makes those docs visible. That **papers over** the exact production
defect the helper exists to catch: a real "searcher never reopened" regression would now be hidden by
the test's own nudge and converge green. It also side-effects a shared cluster client (flushes
buffered docs that may matter to other assertions). Recommend dropping the nudge — if convergence
requires a client-side commit, that's a signal of the production bug, not a test-timing artifact.

### B4 — `SolrZooKeeper` fast-close interrupts the ZK SendThread *before* `super.close()` — **PLAUSIBLE, LOW (test-only, flagged)** (commit `84a26c455f6`)
`solr/solrj/src/java/org/apache/solr/common/cloud/SolrZooKeeper.java` ~106–121

`interruptSendThread()` runs *before* `super.close()`. The interrupt flag is therefore already set
when the ZK `SendThread` runs its NIO transport loop to transmit the graceful `closeSession` packet —
an interrupted thread doing `selector.select()` / `SocketChannel.write()` gets an immediate
`ClosedByInterruptException`, so the `closeSession` bytes may never reach the server and the session +
its ephemerals survive until server-side timeout. This means the in-code comment ("the graceful close
is otherwise untouched, so ephemerals still release promptly / behavior-preserving") is **factually
wrong about the mechanism**: the ordering races the `closeSession` *send*, not merely the cleanup nap.

It is safe **today** only because the flag is confined to ephemeral-free reader connections — verified
that `solr.zkClientFastCloseForTests` is read in exactly one place (`ZkStateReader(String,int,int)` ctor)
and `ZkController`'s own `SolrZkClient`s never enable it. But the comment explicitly invites broader
reuse, and extending it to any ephemeral-holding connection would leak sessions → stale live-nodes /
leader election. **Fix:** interrupt *during* cleanup (e.g. short-timeout join, then interrupt) so the
`closeSession` send completes first, and correct the misleading comment.

Also (minor): `SolrInternalHttpClient.doStop()`'s deadlock guard silently no-ops if
`getContainedBeans(ClientConnector.class)` is ever empty (a future Jetty wiring change) — add a
debug/warn log so the AB-BA deadlock can't silently reappear after an upgrade.

### B5 — `beast` gradle: per-suite log writer truncation race + final/abstract regex false-positive — **PLAUSIBLE, LOW (dev tool)** (commit `1130d57a6f5`)
`gradle/testing/beasting.gradle`

- **Log truncation:** `afterSuite` does `beastWriters.remove(className)` + close; a `TestOutputEvent`
  delivered after `afterSuite` re-enters `computeIfAbsent` → `new File(...).newWriter()` **truncates**
  the file, destroying the captured output the task's whole purpose is to retain (and leaks a second,
  never-closed writer). Guard with a "closed" sentinel instead of `remove`.
- **final/abstract detection:** the regex matches *any* line containing `class <Name>` including a
  comment/Javadoc line; if such a line also contains the word `final` or `abstract`, beast wrongly
  refuses to run a perfectly concrete class. Anchor on a real (non-comment) declaration.

These are convenience/diagnostic concerns in a dev-only task, not a correctness risk to the product.

---

## C. Checked and found correct (verified non-bugs)

To bound the recall claims, these high-risk areas were checked and are **sound**:

- **ZkStateWriter tombstone two-pass drop**, **MODIFYCOLLECTION seeding from incoming props**, and
  **`CollectionWatch.canBeRemoved` counting `propStateWatchers`** — all three previously-fixed bugs
  remain correctly fixed.
- **TransactionLog/JavaBin framing**: header VERSION-byte vs. record framing, `LogReader` header
  consumption at `pos==0`, `endsWithCommit` offset math, extern-string inline self-containment,
  `SolrDocumentList` single-ARR framing, and `ensureCapacity` resize under the exclusive write lock —
  all correct. (One latent: `int` casts of `long` offsets would corrupt a tlog > 2 GB, but default
  rollover keeps tlogs well under that.)
- **Replica short-state ↔ State table** (1=LEADER, 2=ACTIVE, …): internally consistent; `published`
  semantics correct (`getState`→ACTIVE, `getRawState`→LEADER, `Slice.getLeader` uses raw, `isActive`
  treats LEADER as active). No off-by-one.
- **`Http2SolrClient` request-body `freeBuffer()`** AtomicBoolean guard is genuinely idempotent across
  the `onComplete` backstop and the input-stream-close path — no double-free of the request buffer.
- **`SolrCloudTestCase` per-replica diagnostic clients** are built `withHttpClient(shared)`, so closing
  them does **not** stop the shared cluster httpClient (`closeClient` only true when self-created).
- Recent test refactors (`TestDocBasedVersionConstraints` deferred single commit, `TestSolrXml`
  dropping `initCore`, `ZkCLITest` 127.0.0.1:1, `ZkShardTermsTest` value-vs-reference waits, audit
  match-by-action) preserve coverage; no regressions spotted.

---

## Suggested priority order

1. **A1** TransactionLog ADD torn read (data corruption under concurrent read+index)
2. **A2 / A3** ZkStateWriter + ReplicaMutator NPEs (one-line null guards; overseer-aborting)
3. **A4** ReplicaMutator `CORE_NAME_PROP` silent drop — confirm reachability first
4. **A7 / A8 / A9** overseer/election concurrency races (reconnect-storm window)
5. **A5** Replica.equals NPE on null id
6. **B2 / B3** PeerSync fix swallows commit failure / test nudge masks the same bug class
7. **B1** audit close 30s shutdown bound
8. **A6** Http2 response-buffer pooling; **A10** Replica.write shared-map race
9. **B4 / B5** test-only / dev-tool low-severity items

---

# Part D — Round 2: next tier of hot fork-base classes

Second sweep over the classes that carry the most correctness weight after the round-1 set:
update forwarding, `ZkController`, the `StatePublisher`/overseer-dispatch loop, the
recovery/sync layer (`UpdateLog`/`PeerSync`), the `ZkStateReader` rewrite, and the HTTP
parse/serialize paths. Same method: focused finders, then I verified the most severe / most
concrete claims against the source (those are tagged **CONFIRMED**; line-cited but not
personally re-verified are **PLAUSIBLE**).

> **Cross-cutting theme — disabled safety/cleanup code.** A striking number of these are not
> logic errors but **commented-out** guards/cleanup that upstream relies on: `ZkController`
> graceful-shutdown DOWNNODE publish (D2-1), `StatePublisher.closed` flag (D3-1), the
> `coreRefCount` decrement in `ZkStateReader` (D5-1), upload-limit enforcement in
> `SolrRequestParsers` (D6-1), the `StatePublisher` ConnectionLoss republish (D3-4), and
> multipart temp-file cleanup (D6-2). Worth a dedicated pass to re-enable or consciously delete
> these, since each silently removes a protection that still appears configured.

## D1 — Update forwarding (`DistributedZkUpdateProcessor`, `SolrCmdDistributor`)

This is the silent leader→replica divergence path AGENTS.md explicitly warns about — and the
findings land right on it.

- **D1-1 — CONFIRMED, HIGH.** `SolrCmdDistributor.java` ~410–446 (`onFailure`). A per-replica
  forward failure with HTTP **404** sets `cancelExeption = t` and returns *without even adding the
  failure to `allErrors`*; `ClosedChannelException` / `cancel_stream_error` add to `allErrors` but
  also set the global `cancelExeption`. None of these branches feed the
  `replicasShouldBeInLowerTerms` / leader-initiated-recovery path in `doDistribFinish`. **Effect:** a
  follower that 404s (reloaded/replaced) or drops the connection mid-forward is **never pushed into
  recovery**, the leader applied locally + returned success to the client, and the follower stays
  ACTIVE silently diverged. The global `cancelExeption` is then thrown on the next `distrib*`/`finish`
  call, abandoning the rest of the batch's forwards too.
- **D1-2 — CONFIRMED, HIGH.** `DistributedZkUpdateProcessor.java:941` (`getCollectionUrls` commit
  fan-out). `... isNodeLive(..) && types.contains(..) && state==ACTIVE || state==BUFFERING` — `&&`
  binds tighter than `||`, so it parses as `(live && typeMatch && ACTIVE) || (BUFFERING)`. A
  BUFFERING slice-leader bypasses the type filter (and the liveness check, to the extent
  `getLeader(liveNodes)` doesn't already gate it). **Fix:** parenthesize
  `&& (state==ACTIVE || state==BUFFERING)`.
- **D1-3 — CONFIRMED, MEDIUM.** `SolrCmdDistributor.java` ~130–134 + `DistributedZkUpdateProcessor`
  `doDistribFinish` ~1268. On a closed CoreContainer, `finish()` inserts an `Error` whose `req` is
  `null` (`AlreadyClosedUpdateCmd(null)`); `doDistribFinish` does `if (error.req == null) return;`, so
  the error is logged but **never surfaced to the client** — the client sees success for an update
  aborted by shutdown (possible silent loss on shutdown).
- **D1-4 — PLAUSIBLE, HIGH.** DBQ forward failures may skip the term-bump / recovery escalation when
  the leader's own `isIndexChanged` is false for that op (`doDistribFinish` term-bump gated on
  `isLeader && changed==TRUE`, `zkShardTerms` left null) → a replica that failed a deleteByQuery stays
  ACTIVE with the docs un-deleted, never reconciled. Also DBQ→replica is async (`sync=false`), so a
  concurrent add can reorder after the DBQ.
- **D1-5 — PLAUSIBLE, HIGH.** `setupRequest` (~791–800, 874–889): when `slice.getLeader(liveNodes)`
  and a single `getLeaderRetry(...,2000,false)` both return null during a failover gap, it builds a
  `ForwardNode` with a null leader → `StdNode` ctor throws "nodeProps cannot be null" → the client
  update 500s instead of waiting for the new leader. Similar concern in `processCommit` swallowing a
  500ms leader-retry timeout and then mis-routing the commit (~176–246).
- **D1-6 — PLAUSIBLE, LOW.** `getNodesByRoutingRules` route-expiry removal is commented out
  (~1102–1113): expired MIGRATE routing rules are logged but never removed from slice state.
- **Verified non-bug:** `getSubShardLeaders` (~1014–1051) correctly builds and returns the local
  list now — the previously-broken field-vs-local bug is fixed. No `CORE_NAME_PROP`/`STATE_PROP`
  misuse in either file.

## D2 — `ZkController` lifecycle

- **D2-1 — CONFIRMED, HIGH.** `disconnect(boolean publishDown)` ~538–564: the entire
  `if (publishDown) { publishNodeAs(..., DOWNNODE) }` block is **commented out**, and `run()` calls
  `disconnect(true)` on shutdown. So graceful shutdown removes the ephemeral live-node but **never
  publishes DOWN** — since replica state lives in the StateUpdates channel (not tied to the live
  node), the replicas linger ACTIVE in cluster state after a clean stop. Rolling restarts leave a
  window routing to a replica advertised ACTIVE on a node that's gone. (Shard-term cleanup in the
  disconnect listener is likewise commented out.)
- **D2-2 — CONFIRMED, HIGH (companion to D3-1).** `publish()` / `publishNodeAs` (~1566, 1573, 2182)
  call `statePublisher.submitState(...)` with no closed/null guard; combined with D3-1 (the `closed`
  flag is commented out and `close()` shuts the worker), a registration finishing during shutdown
  enqueues state onto a dead worker → silently lost (or NPE if `statePublisher` was never created
  because `start()` wasn't called).
- **D2-3 — PLAUSIBLE, HIGH.** Reconnect handler (set in `start()` ~414) dereferences
  `statePublisher` / `overseerElector` / `zkStateReader` (~431, 434, 449) which are only assigned
  later in `init()`. A session event in the `start()`→`init()` window, or a partially-failed `init()`,
  NPEs in the reconnect executor → watches never re-established, node stops tracking cluster state.
- **D2-4 — PLAUSIBLE, HIGH.** `register()` leader-wait (~1191–1213): if `waitForState(60s)` times out
  (no replica ever ACTIVE — e.g. mutual-wait during a full-cluster reconnect, or a delayed
  self-election), `finishRegistration` is never scheduled and the replica never publishes / sets
  `hasRegistered`, surfacing as a 500.
- **D2-5 — PLAUSIBLE, MEDIUM.** Reconnect `createEphemeralLiveNode()` treats `NodeExistsException` as
  fatal (~1051–1058); if the old session's ephemeral hasn't been reaped when the new session creates
  it (common after expiry), the create throws, the reconnect task aborts (generic catch just logs),
  and the node is never "live" again for that session.
- **D2-6 — PLAUSIBLE, MEDIUM.** Reconnect `retryElection` reuses the original `Overseer`/
  `OverseerElectionContext` without the `isCloseAndDone()` guard that `rejoinOverseerElection` has
  (~449 vs ~1823) → can win overseer election around a closed Overseer that then processes nothing.

## D3 — `StatePublisher` + overseer dispatch (`WorkQueueWatcher`, `OverseerTaskProcessor`)

- **D3-1 — CONFIRMED, HIGH.** `StatePublisher.java`: `close()` has `// this.closed = true;`
  **commented out** (~440), so the `closed` flag (declared ~56) is never set and `submitState` has no
  guard. `close()` enqueues `TERMINATE_OP_MAP` then `workerExec.shutdown()`; any `submitState` after
  that puts onto a queue whose worker has consumed its terminate pill and exited → silently dropped.
  NPE if `start()` (hence `workerExec`) was never set up.
- **D3-2 — PLAUSIBLE, HIGH.** `WorkQueueWatcher.processQueueItems` (~145–219): the work-queue znode is
  deleted at the end of each iteration even when the apply was a swallowed-and-logged failure (the
  `UPDATESHARDSTATE` try/catch only logs, ~184–186). A shard-state transition that throws is dropped
  **and** its znode deleted → shard stuck in its prior state with no retry.
- **D3-3 — PLAUSIBLE, MEDIUM-HIGH.** `WorkQueueWatcher.processEvent` lost-wakeup (~92–129): the
  worker's `if (!checkAgain){running=false;break;}` stop-decision is made *without* holding `lock`,
  while `processEvent` sets `checkAgain=true`/tests `running` *under* `lock`. Interleaving can leave
  `checkAgain==true` with `running==false` and nothing scheduled — the last state update (e.g. a final
  DOWNNODE with no subsequent watch event) stalls. Move the check inside `lock`.
- **D3-4 — PLAUSIBLE, MEDIUM.** `StatePublisher.processMessage` async ZK `create` callback (~260–284)
  logs non-zero `rc` but the ConnectionLoss requeue is **commented out** → a failed publish is dropped;
  the 30s dedup window (~337–346) can then suppress the caller's identical retry (LEADER is exempt),
  permanently losing a non-LEADER transition.
- **D3-5 — PLAUSIBLE, MEDIUM.** `OverseerTaskProcessor.getLeaderId` (~211–219) returns
  `children.get(0)` **unsorted**; during an overseer handoff (two transient ephemeral children) it can
  return the stale id. Should sort like `getSortedElectionNodes`. Also `runningZKTasks`/`runningMap`
  can leak stale async ids across overseer failover (~333–366).
- **Verified non-bug:** all four files route through `overseer.getZkController()` (never the
  null-prone `cc.getZkController()`); the LEADER dedup exemption is deliberate and correct.

## D4 — Recovery / sync (`UpdateLog`, `PeerSync`, `PeerSyncWithLeader`)

- **D4-1 — CONFIRMED, HIGH.** `PeerSyncWithLeader.java` ~318–321: `updates.add(recentUpdates.lookup(bufferUpdate))`
  adds the result **unconditionally**, but `lookup` returns null if the buffered version was trimmed/
  rolled between the snapshot and here → a null enters the apply list → NPE/CCE in `Updater.applyUpdates`
  → sync reported failed (and updates before the null applied, those after not). **Fix:** null-check
  before add.
- **D4-2 — CONFIRMED, HIGH.** `UpdateLog.java` `copyOverBufferingUpdates` ~1329–1336: `state.set(State.ACTIVE)`
  is the **first** statement, before `copyOverOldUpdates(...)` does the actual buffered→live copy. It's
  under `blockUpdates()`+`tlogLock`, but readers that don't take those locks (`getVersions` from a
  peer's PeerSync, RTG/fingerprint, the state gauge) can observe ACTIVE mid-copy and conclude "in sync"
  with only a prefix of the buffered ops applied. `applyBufferedUpdates` correctly defers ACTIVE to the
  replayer's `finally` — this path should mirror that.
- **D4-3 — PLAUSIBLE, HIGH.** `UpdateLog.applyBufferedUpdates` (~1919, 1936–1940): on early break
  (`cancelApplyBufferUpdate || isClosed`) the replayer still runs its `finally` (state→ACTIVE) and
  `dropBufferTlog()` still fires → un-applied buffered updates dropped while the replica goes ACTIVE.
- **D4-4 — PLAUSIBLE, MEDIUM.** `PeerSync.handleUpdates` (~516–531) overwrites `sreq.fingerprint` with
  the leader's *post-update* fingerprint, then `compareFingerprint` against it → false "out of sync"
  under steady leader indexing → unnecessary full replication / possible recovery livelock.
- **D4-5 — PLAUSIBLE, MEDIUM.** `PeerSyncWithLeader` `skipDbq=true` (~301) + the gap-trim
  (~329–348): a deleteByQuery in the gap window is never returned and `existDBIOrDBQInTheGap` can't
  see it → a gap-window DBQ may be skipped → resurrected deleted doc on the replica. Worth confirming
  the leader's getUpdates honors `skipDbq` and DBQs are otherwise reconciled.
- **D4-6 — PLAUSIBLE, MEDIUM.** Plain `PeerSync` counts buffered-but-unapplied versions
  (`getRecentUpdates` prepends `bufferTlog`; `RecentUpdates.update` keeps them in `updates`) as
  "owned," so `ourUpdateSet.contains(otherVersion)` skips requesting versions not actually indexed.
- **D4-7 — PLAUSIBLE, LOW-MED.** `copyOverOldUpdates(long)` (~1388–1402) releases `tlogLock` then opens
  `oldTlog.getReader(0)` with no incref → a racing decref→`forceClose` can unmap the Agrona buffer
  mid-read (SIGSEGV class), the failure mode the `activeReplayers` drain elsewhere was added to prevent.

## D5 — `ZkStateReader` (the cluster-state rewrite)

The headline invariants the user was most concerned about **check out**: the `linkState`
owner-merge does *not* let a foreign DocCollection steal a replica's live-state link (ZkStateReader
builds fresh Replica instances per parse, so they link as first-owner), `CollectionWatch.canBeRemoved`
counts `propStateWatchers`, and the restored `AliasesManager` CAS/BadVersion retry loop is correct.

- **D5-1 — CONFIRMED, MEDIUM-HIGH (leak).** `ZkStateReader.java:2249`: in `removeDocCollectionWatcher`
  the decrement `//if (v.coreRefCount.get() > 0) v.coreRefCount.decrementAndGet();` is **commented
  out**, while `registerDocCollectionWatcher` (~1894) increments it. Every register/remove cycle leaks
  a `coreRefCount`, so `canBeRemoved` (needs `refCount<=0`) never holds and the collection can never
  go lazy again → the watched-collection set grows monotonically for a long-lived client that
  repeatedly `waitForState`s on transient collections (memory + per-event work leak).
- **D5-2 — PLAUSIBLE, MEDIUM.** `ZkStateReaderQueue` justStates delta branch (~205–207) lacks the
  null-guard / `.exceptionally` hardening the full-fetch branch has: if the collection was demoted to
  lazy between the watch event and the drain, `getAndProcessStateUpdates(null)` → wraps a null
  collection → the state-update is silently dropped and no watcher notified (lost notification).
- **D5-3 — PLAUSIBLE, MEDIUM.** `refreshLiveNodes` (~841–906): the `this.liveNodes = newLiveNodes.get()`
  assignment happens outside the version-CAS lambda, so it isn't atomic with the `liveNodesVersion`
  update; concurrent refreshers (PERSISTENT watch on the notifications executor + explicit refresh)
  can leave `liveNodes` older than `liveNodesVersion` claims → a node-down event missed until the next
  children-changed event. No lock around the method.
- **D5-4 — PLAUSIBLE, LOW-MED.** `waitForState` evaluates the predicate inline (~1980) *before*
  registering the watcher (~1987); a change landing in that window, for a still-lazy collection, may
  not be re-evaluated. Register before the inline check, or re-check inside the registration compute.
- **D5-5 — PLAUSIBLE, LOW.** Alias `update()` (~2497) dereferences `getConnectionManager().getKeeper()`
  with no null guard; during a reconnect window `getKeeper()` can be null → NPE escapes the
  KeeperException-only retry loop, aborting an alias write.

## D6 — HTTP parse / serialize (`SolrRequestParsers`, `BinaryResponseWriter`)

- **D6-1 — CONFIRMED, HIGH (availability/DoS).** `SolrRequestParsers.java`: `init(multipartUploadLimitKB,
  formUploadLimitKB)` is an **empty/commented-out stub** (~150–164) and the limits are never stored or
  applied. `MULTIPART_CONFIG` is hardcoded `new MultipartConfigElement(null, -1L, -1L, 2097152)` —
  unlimited file/request size — and `HttpSolrCall.java:143` calls `parseFormDataContent(..., Long.MAX_VALUE,
  ...)`. So `multipartUploadLimitKB` / `formUploadLimitKB` from `solrconfig.xml` are silently ignored;
  a multi-GB multipart or form-urlencoded body is buffered unbounded (heap for form, 2 MB-threshold
  spill-to-disk for multipart) → OOM / disk DoS, with the control *appearing* configured.
- **D6-2 — CONFIRMED, HIGH (disk leak).** Multipart parts spilled to temp files (threshold 2 MB) are
  never deleted — `Part.delete()` is called nowhere and the dispatch-filter cleanup is commented out.
  Repeated >2 MB uploads accumulate temp files until the disk fills.
- **D6-3 — PLAUSIBLE, MEDIUM.** `BinaryResponseWriter.java` ~84–94/122–132: the new `StoredField`
  fast-path only handles a `Utf8CharSequence` value; a `StoredField` holding a `String`/numeric value
  (`getCharSequenceValue()==null`) falls through to the `IndexableField` branch that does
  `solrQueryRequest.getSchema()` → the documented NPE still exists for a request-less `Resolver`
  (`getParsedResponse` / streaming export). The fix narrowed but didn't eliminate the hazard.
- **D6-4 — PLAUSIBLE, LOW.** wt=javabin guard residual: a body with **no** Content-Type but `wt=javabin`
  that is actually XML/JSON is still force-labeled `application/javabin` and fails to parse (mainly bites
  hand-rolled clients; SolrJ always sets Content-Type).
- **Verified non-bug:** form-urlencoded is correctly NOT re-added as a content stream; remote streams /
  stream.body default to disabled (no default SSRF); the `Utf8CharSequence` direct-write path is correct.

## Round-2 top priority

1. **D1-1 / D1-2** — the 404/cancel recovery-bypass and the `&&/||` commit-fan-out precedence bug:
   concrete realizations of the silent-divergence mode.
2. **D6-1 / D6-2** — unenforced upload limits + multipart temp-file leak (availability/DoS, look configured).
3. **D2-1 / D2-2 / D3-1** — commented-out DOWNNODE publish + the `StatePublisher.closed` flag: stale-ACTIVE
   on clean shutdown and lost state publishes during shutdown.
4. **D4-1 / D4-2 / D4-3** — buffered-update apply bugs (null in apply list; ACTIVE-before-copy; cancel drops buffer).
5. **D5-1** — `coreRefCount` watch leak.
6. **D3-2 / D3-3** — work-queue item deleted after a swallowed apply error; dispatch lost-wakeup.

> Confidence note: every **CONFIRMED** item above I checked against the source this session. The
> **PLAUSIBLE** items are line-cited and consistent with the code but warrant a focused confirm (and,
> for the concurrency ones, a reproduction under load) before fixing — several live in code that is
> exercised only on failover / reconnect / shutdown paths that normal tests rarely stress.

---

# Part E — Round 3: core/searcher, container, dispatch, cloud client, recovery+terms, collection admin

Third sweep, over the next central tier: `SolrCore`/`DirectUpdateHandler2` (searcher
lifecycle + commit), `CoreContainer` (load/shutdown), `HttpSolrCall`/`SolrDispatchFilter`
(server dispatch), the SolrJ cloud client routing/retry layer, `RecoveryStrategy`+`ZkShardTerms`
(recovery loop + terms), and the collection-admin command path. Same method; **CONFIRMED** =
verified against source this session, **PLAUSIBLE** = line-cited, consistent, not personally
re-verified.

> The Part-D theme repeats here: several of the worst items are a **wrong gate / dead path** rather
> than complex logic — `destroy()` closing the core only when `solrReq != null` (E3-1), `reloadWaiting`
> only ever decremented (E2-3), `processMessage` ignoring the cmd's own `writeFuture` (E6-3),
> `CreateCollectionCmd.cleanup` calling the removed queue path (E6-2), and two `&&`/`||` precedence
> bugs (E4-4, plus D1-2 from round 2).

## E1 — `SolrCore` / `DirectUpdateHandler2` (searcher lifecycle + commit)

- **E1-1 — CONFIRMED, HIGH.** `SolrCore.java` doClose "ondeck" block (~2106–2128): `_realtimeSearchers.clear()`
  is **inside** the `for (RefCounted ... : _searchers)` loop, so once `_searchers` is non-empty the
  realtime list is emptied on the first iteration and the following `for (... : _realtimeSearchers)`
  loop closes nothing → realtime-only searchers (the post-PeerSync/RTG reopen path) leak their Lucene
  readers/commit points. The teardown also calls raw `searcher.get().close()` instead of going through
  the `RefCounted` `decref()` protocol, so a searcher still referenced by an in-flight query can be
  closed underneath it (and, for any holder still in the list after `closeSearcher()`, closed twice —
  `SolrIndexSearcher.close()` is not idempotent: `rawReader.decRef()` double-decrements). **Fix:** release
  through `decref()`/`closeSearcher()`, and move the `_realtimeSearchers.clear()` out of the loop.
- **E1-2 — PLAUSIBLE, MEDIUM.** `DirectUpdateHandler2.commit` with `openSearcher=false` (~733–754)
  reopens only the *realtime* searcher, never advancing the registered `_searcher`; ordinary `*:*`
  search stays stale until the next `openSearcher=true` commit. This is the exact class the PeerSync fix
  patched — worth auditing every `openSearcher=false` commit call site (split uses default `true`,
  closeWriter sets `closing` so it's fine; confirm no other ACTIVE-replica path leaves a stale `_searcher`).
- **E1-3 — PLAUSIBLE, LOW-MED.** `getRealtimeSearcher`/`openNewSearcher` shutdown race (~2548–2610):
  `isClosed` is set only at the *end* of `doClose`, while `closeSearcher()` nulls `realtimeSearcher`
  earlier via a concurrent ParWork collector → a racing RTG can pass the `isClosed` guard, find a null
  realtime searcher, and hit an `AlreadyClosedException`/NPE from an already-decref'd IndexWriter rather
  than a clean `CoreIsClosedException`.
- **E1-4 — PLAUSIBLE, LOW.** `registerSearcher` (~3103–3145): if `register()` throws after `_searcher`
  was assigned, the catch decrefs the holder but leaves `_searcher` pointing at it → the next register's
  `_searcher.decref()` double-decrefs.

## E2 — `CoreContainer` (load / shutdown)

- **E2-1 — PLAUSIBLE, HIGH (ties to D2-1/D3-1).** `close()` (~1093–1168) shuts down and awaits
  `solrCoreExecutor`/`coreAdminExecutor` but **not** `solrCoreCloseExecutor`, which is where
  `SolrCores.close()` submits the actual `core.closeAndWait()` work; `zkSys` (→ `ZkController` →
  `StatePublisher`) is then closed in the trailing ParWork. So async core-close tasks routinely run
  *after* `StatePublisher` is closed → a core closing tries to publish state into a dead channel
  (NPE/AlreadyClosed; replica's final state never published). The awaited executor is the wrong one.
- **E2-2 — CONFIRMED behavior, MEDIUM-HIGH.** `getCore()` → `SolrCores.getCoreFromAnyList()` → `core.open()`:
  `open()` throws `AlreadyClosedException` when the core began closing between the map lookup and `open()`,
  but `getCore`'s contract is "return null if not present" and callers use `try (SolrCore c = getCore(...))`
  + null-check. A concurrent unload/close racing a `getCore` (reload, rename, query routing) surfaces a
  spurious `AlreadyClosedException` to the client instead of a clean miss. (Verified `open()` throws on
  refCount<=0; the get→open window is unsynchronized.)
- **E2-3 — CONFIRMED, LOW (latent).** `reloadWaiting.decrementAndGet()` (~1731) with **no** matching
  increment anywhere → the counter only ever goes negative. Harmless today (unused for control flow) but
  any future guard keyed on it is pre-broken. Dead/incorrect state.
- **E2-4 — PLAUSIBLE, MEDIUM.** `getCore()` lazy-load path (~2122–2128) returns a freshly
  `createFromDescriptor`'d core **without** the `open()` incref the loaded path takes, so the first
  caller's try-with-resources `close()` decrefs the creation reference → inconsistent refcount contract
  between the two return paths; first accessor can start closing a just-registered core.
- **E2-5 — PLAUSIBLE, MEDIUM.** `load()` failure cleanup (~945–951) only removes core descriptors and
  rethrows, leaving `updateShardHandler`/`shardHandlerFactory`/`metricManager`/`zkSys` (with a live
  ephemeral node already created) unclosed → direct `load()` callers (embedded/test) get a half-init
  container with a registered live node but no cores. `create()` duplicate-name check is also racy
  (check-then-act on a ConcurrentHashMap).

## E3 — `HttpSolrCall` / `SolrDispatchFilter` (server dispatch)

- **E3-1 — CONFIRMED, CRITICAL.** `HttpSolrCall.destroy()` (~644–654): `IOUtils.closeQuietly(solrCore)` is
  **nested inside `if (solrReq != null)`**. Any request that acquired a core ref (`cores.getCore(...)`,
  via `open()` → +1 refcount) but ends with `solrReq == null` — HEAD→/select, the standalone default-core
  passthrough, a core acquired then routed to REMOTEQUERY — **never closes the core**, leaking a refcount
  permanently. A core stuck above refcount 0 can never be unloaded/reloaded → RELOAD/DELETE/shutdown hang.
  **Fix:** hoist `IOUtils.closeQuietly(solrCore)` out of the `solrReq != null` guard. (One-line, highest ROI.)
- **E3-2 — PLAUSIBLE, HIGH (security).** `call()` (~527–528) skips authorization entirely when
  `action == REMOTEQUERY || FORWARD`. REMOTEQUERY is also chosen for **external** client requests that
  land on a node holding no local replica, and the forward stamps `QoSParams.INTERNAL`. If the cluster
  trusts the INTERNAL/PKI marker for inter-node calls, an external client steered to a non-hosting entry
  node can bypass authorization. The skip is correct only for genuine inter-node hops.
- **E3-3 — PLAUSIBLE, HIGH (use-after-free).** With `solr.asyncIO=true`, `SolrCall.writeResponse` streams
  the body from a Jetty `WriteListener.onWritePossible` callback *after* `call()` returns, but
  `SolrDispatchFilter.filter`'s `finally` runs `call.destroy()` immediately on return (closing `solrCore`
  and releasing the pooled buffer). The async writer then reads/releases a buffer on an already-torn-down
  core → truncated/corrupt response. (The audit half in `runWhenFinished` is currently latent because the
  `startAsync` block is commented out; the WriteListener half is live whenever `ASYNC_IO` is set.)
- **E3-4 — PLAUSIBLE, MEDIUM.** `sendException` (SolrDispatchFilter ~731–747) can write a **second**
  response body after a partial/committed response (the only guard is a racy `isCommitted()`), corrupting
  the body / mismatching Content-Length.
- **E3-5 — PLAUSIBLE, MEDIUM.** REMOTEQUERY (proxied) requests emit **no audit event** at all (no
  COMPLETED/ERROR) → audit trail silently omits every cross-node-routed request.
- **E3-6 — PLAUSIBLE, MEDIUM (availability).** Unbounded `while(true){ if(!isCoreLoading) break; sleep(250);}`
  (~205–209, and the 150ms loop in `checkProps`; both tagged `// nocommit`) blocks the request thread with
  no timeout → a wedged core load parks every request addressing it and saturates the pool.

## E4 — SolrJ cloud client routing / retry

- **E4-1 — CONFIRMED, CRITICAL.** `BaseCloudSolrClient.condenseResponse` (~688–697): a shard response with
  `responseHeader.status > 0` is only copied into the top-level `status` (`if (s>0) status=s;`) and
  **nothing throws** — a partially-failed distributed update returns a 200/success `NamedList` to the
  caller with the failure buried in the header. Concrete realization of the "silent distributed-update
  failure" trap.
- **E4-2 — CONFIRMED, HIGH.** Same method, ~694: `int s = (Integer) header.get("status");` unboxes — if a
  shard returns a `responseHeader` without `status`, this NPEs and aborts the entire rollup, converting a
  partial success into a total client-visible failure (and discarding the shards that succeeded).
- **E4-3 — CONFIRMED, HIGH.** `LBHttp2SolrClient.java:215`: `if (!isNonRetryable && (RETRY_CODES.contains(e.code())) || e.getMessage().contains("Connection refused"))`
  — `&&` binds tighter than `||`, so an **update** (`isNonRetryable==true`) failing with a "Connection
  refused" message is retried despite the non-retryable contract → possible **duplicate document
  application**; and `e.getMessage()` can be null → NPE. **Fix:** parenthesize and null-guard the message.
- **E4-4 — CONFIRMED, MEDIUM-HIGH.** `ZkShardTerms.saveTerms` (~398–410): on `BadVersionException`, if
  `newTerms.getVersion() > foundVersion` it `return true` ("saved successfully") **without writing**. Every
  CAS caller (`registerTerm`/`startRecovering`/`doneRecovering`/`setTermEqualsToLeader`/`ensureTermsIsHigher`)
  treats `true` as done, so if in-memory term version drifted ahead of ZK (node deleted+recreated, stale
  in-memory `ShardTerms`), a `setTermEqualsToLeader` at recovery completion silently no-ops → replica
  publishes ACTIVE while its ZK term stays below the leader (skipped by update fan-out) **and** the
  `_recovering` marker stays stuck (can never become leader). *(Filed here as it's the terms half of E5.)*
- **E4-5 — PLAUSIBLE, HIGH.** `directUpdate` serial path (~523–538) throws on the first shard failure
  without aggregating or attempting later routes → partial application + the retry layer may resend the
  whole batch (re-applying already-succeeded shards). The non-routable-request failure path (~563–570)
  likewise discards already-collected successful shard responses.
- **E4-6 — PLAUSIBLE, MEDIUM.** Stale-state retry (~974–1044) guards `retryCount < MAX_STALE_RETRIES`
  per-branch (comm-error vs stale-state) on a shared counter, so a request oscillating between the two
  classifications can exceed the intended cap; `_stateVer_` parse (~925) splits on `>` and `parseInt`s both
  halves with no guard (malformed value → NFE/AIOOBE aborts a successful response).
- **E4-7 — PLAUSIBLE, MEDIUM/LOW.** `LBSolrClient`: a non-standard zombie (added via the async
  `LBHttp2SolrClient.addZombie`, `standard=false`) is dropped after `NONSTANDARD_PING_LIMIT` and never
  revived (revive only re-adds `standard` servers) → a transiently-failed node on a *direct*
  `LBHttp2SolrClient` is permanently evicted (masked for `CloudHttp2SolrClient`, which rebuilds the list
  per request). `pickServer` can also divide-by-zero (`% aliveServerList.length`) when the alive list is
  empty and `numServersToTry` is set → uncaught `ArithmeticException`.

## E5 — `RecoveryStrategy` (full loop) + `ZkShardTerms`

- **E5-1 — PLAUSIBLE, HIGH.** `ZkShardTerms.scheduleTermNotify` (~110–144): the `running`/`checkAgain`
  coalescing gate is a pair of plain `volatile boolean`s with a non-atomic check-then-act. A term-change
  notification can be permanently lost: the worker reads `checkAgain==false`, then before it sets
  `running=false` another thread sees `running==true` and sets `checkAgain=true`; the worker then exits and
  the just-set flag is stranded with no runner → a `RecoveringCoreTermWatcher` notification is dropped, so a
  replica pushed below the leader's term is never told to recover and stays ACTIVE-but-behind. (Same
  lost-wakeup shape as D3-3 in `WorkQueueWatcher`.)
- **E5-2 — PLAUSIBLE, MEDIUM-HIGH.** `RecoveryStrategy` PeerSync branch treats "leader reports no versions"
  as success for an empty local shard (`syncSuccess=true`) **without** the `leaderHasUpdateVersions` guard
  that the replication branch has (~760–771) → PeerSyncing against a just-restarted leader that hasn't
  replayed its tlog yet "succeeds" and publishes ACTIVE with an empty index. The asymmetry between the two
  branches is the tell.
- **E5-3 — PLAUSIBLE, MEDIUM.** Close-during-recovery (~803–831): after `successfulRecovery==true`, the
  re-checks of `isClosed()`/`core.isClosing()` can `return false` **before** `publish(ACTIVE)`, so
  `setTermEqualsToLeader`/`doneRecovering` never runs and the `_recovering` marker stays set in ZK → on
  restart the fully-caught-up replica can never win an election (`canBecomeLeader` false forever).
- **E5-4 — PLAUSIBLE, MEDIUM/LOW.** `ensureTermsIsHigher`/the CAS loops (~205–212) have no iteration cap;
  combined with E4-4's refresh path and a brief double-leader (the `onlyLiveReplica` bypass), competing
  term writers can livelock on the update fast-path. A non-AlreadyClosed `publish(ACTIVE)` failure (~836–839)
  re-runs the **full** PeerSync+replication cycle (burning the bounded retry budget) instead of just
  retrying the publish.

## E6 — Collection-admin commands

- **E6-1 — CONFIRMED, MEDIUM (live bug, slated-for-removal feature).** `OverseerCollectionMessageHandler`
  `processRebalanceLeaders` (~474): `zkStateReader.getBaseUrlForNodeName(message.getStr(message.getStr(NODE_NAME_PROP)))`
  — the **inner** `getStr` returns the node name, which is then used as a *key* for the outer `getStr` →
  null → `getBaseUrlForNodeName(null)`; and `sreq.nodeName = message.getStr(CORE_NAME_PROP)` is the stripped
  null. REBALANCELEADERS is entirely broken. (AGENTS.md marks REBALANCELEADERS for deletion, so the
  disposition is "remove the handler" — but as written it's a live defect if reached.)
- **E6-2 — CONFIRMED, HIGH.** `CreateCollectionCmd.cleanup` (~135):
  `ocmh.overseer.getCoreContainer().getZkController().getOverseerCollectionQueue().offer(...)` uses both the
  **removed queue-based path** and the documented `cc.getZkController()` null-trap. On a failed async create,
  cleanup throws NPE (swallowed by the catch) and the half-created collection's orphan znodes/replicas are
  left behind. Should route through `overseer.getZkController()` + the DELETE/StateUpdates path (as
  `cleanupCollection` correctly does).
- **E6-3 — PLAUSIBLE, MEDIUM-HIGH.** `processMessage` (~314–371) never reads `response.writeFuture` that
  `CreateCollectionCmd` sets — it re-enqueues and creates its own `writeFut` instead, so the cmd's intended
  "state durable before cores created" ordering is not the one enforced (masked today by the finalizer's own
  `waitForState`, but `writeFuture` is dead for any cmd relying on it).
- **E6-4 — PLAUSIBLE, MEDIUM.** DELETE does its removal + `zkClient.clean` inside the cmd's `finally`
  (bypassing the dispatcher write path); needs confirmation that `ZkStateWriter.removeCollection` also clears
  `assignMap`/`idToCollection` — if not, recreating a same-named collection resumes from a stale
  replica-assign counter.
- **E6-5 — PLAUSIBLE, MEDIUM/LOW.** `waitToSeeReplicasInState` keys by `getCoreUrl()`; given the fork's
  name==coreName unification, duplicate coreUrls in the input collapse and the `r.size()==coreUrls.size()`
  check can never satisfy → spurious timeout. `Assign.assignShard` derives the next shard id from
  `size()+1` (~75–77) → collides with an existing shard after a gap. `modifyCollection` reloads cores even
  when the READ_ONLY structure write failed (~699–714) → cores reload a stale read-only value.
- **Verified non-bug / fragile-invariant:** SPLITSHARD mints then discards a sub-shard replica `id` in
  `fillRanges` (keeping only the core-name suffix), so the persisted name suffix and internal id diverge —
  safe **only** because `ZkStateWriter.init` reseeds with `Math.max(internalId, nameSuffix)` on overseer
  takeover. The split-lock is released in `finally` on all paths (no deadlock). `sliceCmd` correctly uses
  `replica.getState()`.

## Round-3 top priority

1. **E3-1** — `HttpSolrCall.destroy()` core refcount leak (cores never unload; RELOAD/DELETE/shutdown hang). One-line fix.
2. **E4-1 / E4-2 / E4-3** — cloud client: silent partial-update success, NPE-aborts-rollup, and the precedence bug causing duplicate updates.
3. **E1-1** — `doClose` searcher teardown (reader/commit-point leak + raw-close of in-use/registered searchers).
4. **E2-1 / E2-2** — shutdown awaits the wrong executor (state published into a closed channel); `getCore` throws instead of returning null.
5. **E4-4 / E5-1 / E5-3** — terms `saveTerms` false-success, the term-notify lost-wakeup, and recovery leaving `_recovering` stuck → silent ACTIVE-but-behind / un-electable replicas.
6. **E6-2** — `CreateCollectionCmd.cleanup` null-trap + removed queue path (orphaned collections on create failure).

> Same confidence caveat as Parts A–D: **CONFIRMED** items were checked against source this session;
> **PLAUSIBLE** items are line-cited and consistent but should get a focused confirm — and the
> concurrency/shutdown ones a load/failover reproduction — before fixing.

---

# Part F — Round 4: ParWork, IndexWriter state, versioning/RTG, replication, distributed search, caches

Fourth sweep, over the engine internals: the pervasive **`ParWork`** concurrency framework, the
`DefaultSolrCoreState` IndexWriter lifecycle, `VersionInfo`/`VersionBucket`/RTG, the
`IndexFetcher`/`ReplicationHandler` replicate-from-leader path, the `HttpShardHandler` distributed-search
fan-out, and the `CaffeineCache`/`CommitTracker` cache+autocommit layer. **CONFIRMED** = verified
against source this session; **PLAUSIBLE** = line-cited, consistent, not personally re-verified.

> **F0 — `ParWork` semantics are load-bearing (read this first).** Several earlier findings (the
> TransactionLog/A1 double-write concern, the SolrCore E1-1 searcher teardown, the CoreContainer E2-1
> shutdown ordering) hinge on what `ParWork.close()` actually guarantees. Confirmed this session:
> - **The only barrier is `addCollect()`, and only between WorkUnits, only at `close()` time.** Anything
>   added with `collect()` *after* the last `addCollect()` is flushed into one final WorkUnit and runs
>   **concurrently**. A caller that writes `collect(a); collect(b);` expecting `a` to finish before `b`
>   is wrong — they race. (F1-4)
> - **A 1-object group runs inline on the caller thread; a ≥2-object group runs on the pool** — so the
>   same close path silently changes concurrency semantics based on how many objects were collected. (F1-5)
> - **With `ignoreExceptions=true` (what `SolrCore.close()` uses), task failures are logged and
>   swallowed**, and an `InterruptedException` during the join is swallowed and treated as completion —
>   so `close()` reports success even when a resource was left half-closed or a task never ran. (F1-1/F1-2)
>
> Net: any earlier "ParWork closes these N things in order" assumption holds **only** if each was its own
> `addCollect()` group; otherwise they race, and any failure is invisible.

## F1 — `ParWork` / `ParWorkExecutor` (custom concurrency framework)

- **F1-1 — CONFIRMED, HIGH.** `ParWork.java` join loop (~437–462): a task's `future.get()` throwing
  `InterruptedException` is caught with a bare `break` — the interrupt flag is **not** restored, no
  exception is recorded, and the future is **not** cancelled, so `close()` returns *normally* while the
  in-flight close task may still be running or never ran. Contradicts the codebase's own
  `propagateInterrupt` discipline. → silent incomplete close on shutdown + lost interrupt.
- **F1-2 — CONFIRMED, HIGH.** With `ignoreExceptions=true` (the common close path, incl.
  `new ParWork(this, true)` in `SolrCore.close`), a failing close task is logged and swallowed
  (`handleObject` returns normally), the future "succeeds", and `close()` reports success — so e.g.
  `updateHandler.close()`/tlog/IndexWriter throwing on close leaves the resource half-released while the
  core is marked closed (next reopen/lock-acquire then fails).
- **F1-3 — PLAUSIBLE, HIGH.** `RejectedExecutionException` fallback (~421–431) runs the task inline in the
  submit loop; if that inline `call()` throws (a Closeable failing to close), it propagates out of the
  `for` loop and the **remaining** objects in that WorkUnit are never submitted/closed (leak). And
  `ParWorkExecutor`'s own rejection handler does a blocking `getQueue().put(r)` on an
  already-shutdown pool (~72–79) → a close task enqueued into a pool that never drains (silently dropped).
- **F1-4 — CONFIRMED (semantics), MEDIUM-HIGH.** `collect()` without a trailing `addCollect()` merges into
  the final WorkUnit → resources the caller assumes close sequentially run concurrently (the root enabler
  of the E1-1 / A1 race concerns). No diagnostic.
- **F1-5 — CONFIRMED (semantics), MEDIUM.** 1-object group runs inline on the caller thread; ≥2-object
  group runs on the pool (~386–408) → silent inline-vs-parallel behavior change (thread-locals/MDC/
  `SolrThread` identity differ).
- **F1-6 — PLAUSIBLE, MEDIUM.** Nested ParWork on the same bounded shared pool (`ParWorkRootExec`,
  core/max = PROC×3) blocks in `future.get()` waiting for nested tasks that can't get a thread →
  re-entrancy starvation/deadlock under load; the IO/unknown root pools instead use
  `Integer.MAX_VALUE` max → unbounded thread creation (`ParWorkRootIOExec`/`ParWorkUnknownRootIOExec`).
- **F1-7 — PLAUSIBLE, LOW.** `verifyValidType` throws at `collect()` time → a bad object aborts collection
  of the rest of the batch (already-collected items still close; later ones leak). `ParWorkExecutor.afterExecute`
  leaves `JavaBinCodec.THREAD_LOCAL_ARR`/`SolrQTP` thread-local cleanup **commented out** → pooled-thread
  thread-local retention/bleed.

## F2 — `DefaultSolrCoreState` (IndexWriter lifecycle)

- **F2-1 — CONFIRMED, HIGH.** `close()` (~826–852) **does not take `iwLock.writeLock()`** (both the lock and
  unlock are commented out, with a "can't lock here without a blocking race" note), so `closeIndexWriter`
  runs `IndexWriter.commit()/close()` concurrently with an `/update` thread still holding only the
  `readLock` and using the same writer → `AlreadyClosedException`/partial commit. The writeLock="changing
  writers" invariant is violated exactly on the close path.
- **F2-2 — CONFIRMED, HIGH.** `getIndexWriter` (~156–210) has **no `this.closed` check** (the SolrCoreState
  closing flag), and the container-shutdown guard is commented out at the top and present only inside the
  `indexWriter == null` branch. So once `decrefSolrCoreState` set `closed` and entered `close()`, a racing
  `getIndexWriter` with `indexWriter != null` skips to `incref()` and hands out the writer being closed;
  in the null branch for a single-core unload (container not globally down) it can **reopen a brand-new
  writer on a Directory `close()` is releasing**.
- **F2-3 — PLAUSIBLE, MEDIUM.** `lock()` retry loop (~239–248) swallows `InterruptedException`, doesn't
  restore the flag, and spins `tryLock` forever with no timeout → if the writeLock is leaked (see F2-4)
  every `getIndexWriter` caller hangs uninterruptibly.
- **F2-4 — PLAUSIBLE, MEDIUM.** `closeIndexWriter(core, rollback)` (~315–320) acquires the writeLock and
  intentionally does **not** release it (relying on a later `openIndexWriter` finally), but has no
  try/finally around the `changeWriter` call that can throw `IOException` → on throw the writeLock leaks
  permanently and all updates hang (F2-3).
- **F2-5 — PLAUSIBLE, MEDIUM.** Recovery `CompletableFuture` chains run on the shared `getRecoveryExecutor()`,
  which `close()`/`cancelRecovery` do not shut down or interrupt; an in-flight `RecoveryTask` reassigns
  `recoveryStrat` in `run()` even after `cancelRecovery` nulled it, and its `finally` can **resubmit
  itself** → recovery work running on / after a closing core, and "cancel" not actually cancelling.

## F3 — `VersionInfo` / `VersionBucket` / `RealTimeGetComponent`

- **F3-1 — CONFIRMED, MEDIUM (latent livelock).** `VersionBucket.runWithLock` (~62–102) holds an exclusive
  `lock` for the whole body, making the per-id `blockedIds`/`lockCondition` machinery effectively dead — but
  if ever reached, `lockCondition.awaitNanos(250)` waits **250 nanoseconds**, not 250 ms (the literal is raw
  nanos; compare `DistributedUpdateProcessor` which does `TimeUnit.NANOSECONDS.convert(250, MILLISECONDS)`),
  i.e. a ~100%-CPU spin. Reduce to a plain `lock/finally-unlock` or scale the wait.
- **F3-2 — CONFIRMED, MEDIUM.** `RealTimeGetComponent.mergePartialDocWithFullDocFromIndex` (~531):
  `long docVersion = (long) doc.getFirstValue(VERSION_FIELD);` — unchecked cast, no null guard → NPE/CCE on
  in-place RTG reconstruction when `_version_` isn't retrievable (e.g. DV-only and excluded by
  `onlyTheseFields`). The partial-doc side immediately below *is* defensively typed; the full-doc side isn't.
- **F3-3 — PLAUSIBLE, MEDIUM.** `getInputDocumentFromTlog` sets `versionReturned` from the in-place partial
  entry *before* the switch, then can return `DELETED` (~573–599) with `versionReturned` still holding a live
  version → a caller reading `versionReturned` directly (not via `getInputDocument`) sees a version for a
  deleted doc.
- **F3-4 — PLAUSIBLE, LOW-MED.** `processGetUpdates` (~1234–1257) serves `recentUpdates.lookup(version)`
  with no check the version is actually in the index/realtime view (`// TODO: do any kind of validation
  here?`) → the inverse of the stale-searcher bug: a follower can be handed a version not yet visible in the
  leader's searcher. `resolveVersionRanges` (~1283–1289) reads `rangeBounds[1]` with no length check → a
  malformed range string AIOOBEs and aborts the whole peersync getUpdates.
- **Verified non-bug:** `blockUpdates()` is the fair write lock and correctly drains in-flight
  `lockForUpdate` readers before returning; the `bucket.highest` read-modify-write is fully under the bucket
  lock (no lost update); bucket lock is released in `finally` on all paths.

## F4 — `IndexFetcher` / `ReplicationHandler` (replicate-from-leader recovery)

- **F4-1 — CONFIRMED inconsistency, HIGH (corruption risk for >8MB files).** `IndexFetcher.fetchPackets`
  reads the per-packet `packetSize` off the wire (~1784) and uses it for the checksum compare
  (`checksum.update(buf, 0, packetSize)`, ~1817), but the actual `fis.readFully(buf, 0, ...)` (~1806),
  `file.write(buf, ...)` (~1827), and `bytesDownloaded += ...` (~1829) all use
  `(int)Math.min(this.size, PACKET_SZ)` **instead of `packetSize`**. For a file ≤ 8 MB (single packet)
  these coincide; for a file **> 8 MB** the final short packet diverges (the server's last chunk is
  `remainder` bytes, not `PACKET_SZ`) → reads/writes the wrong length, plus an odd `while(fis.read()!=-1)`
  drain loop right after the read. The inconsistency is confirmed; the resulting corruption/EOF for
  multi-packet files is highly likely — **verify with a >8 MB full-copy replication test**.
- **F4-2 — PLAUSIBLE, HIGH.** Downloaded segment **data files are never checksum/footer-verified**
  (`includeChecksum=false`, hardcoded ~1665; nothing calls `CodecUtil.checkFooter`/`checksumEntireFile` on
  the fetched tmp files). The only completeness check is `SegmentInfos.readLatestCommit(tmpIndexDir)`
  (~637), which parses `segments_N` only, never the referenced bodies → a torn/corrupt `.fdt`/`.dvd`
  (incl. from F4-1) is installed and recovery reports SUCCESS, surfacing as `CorruptIndexException` at
  search time.
- **F4-3 — PLAUSIBLE, MEDIUM.** Commit-point reservation is per-`filecontent`-request with only a ~10s
  re-reserve window (`extendReserveAndReleaseCommitPoint`); a slow/large fetch stalling >10s between files
  lets the leader's deletion policy reclaim a still-needed generation → fetch fails / copies a torn file.
- **F4-4 — PLAUSIBLE, MEDIUM.** `downloadConfFiles` swallows per-file download exceptions
  (`catch(Exception){ log.error }`) yet still proceeds to `reloadCore` → a replica reloads with a STALE
  conf/schema while reporting success. The fsync service path also double-closes (`cleanup` closes the
  file, then the fsync task `sync()`+`close()`s it again → `AlreadyClosedException` captured into
  `fsyncException` and re-thrown, turning a good download into a spurious failure).
- **F4-5 — PLAUSIBLE, LOW.** `markReplicationStop()` body is commented out (no-op) → replication
  timing/`isReplicating` stats stay stale; several `DirectoryFactory.get()` sites in `ReplicationHandler`
  (`getFileList`, `loadReplicationProperties`) have their `release()` **commented out** → Directory
  refcount leak that can block clean shutdown.

## F5 — `HttpShardHandler` / `HttpShardHandlerFactory` / `SearchHandler` (distributed search)

- **F5-1 — CONFIRMED, HIGH.** `take()` (~313–354) loops `while (pending.get() > 0)` on a **blocking
  `responses.take()`** — the `poll(5s)` + `httpShardHandlerFactory.isClosed()` + `cancelAll()` escape hatch
  is **commented out**. Combined with `cancelAll()` (~357–365) decrementing `pending` once per
  *still-mapped* entry but **not** draining the `responses` queue or zeroing `pending`, the three structures
  (`pending`, `responseCancellableMap`, `responses`) desync: a response whose callback already fired and was
  removed from the map still counts in `pending`, so a later stage's `take()` can block forever on a
  response that never comes. → distributed query hang.
- **F5-2 — PLAUSIBLE, HIGH.** A cancelled request's async callback still `responses.add(...)` (the
  `Cancellable`/`asyncReq` listener never checks `cancelled`), and `cancelAll` doesn't drain `responses`;
  since the handler instance is reused across phases, a stale response from a cancelled prior phase can be
  attributed to the wrong `ShardRequest` → corrupted merge. The termination check
  `responses.size() == actualShards.length` (~345) has no per-shard dedupe, so a duplicate/late response
  can satisfy it while a real shard is never represented → silently incomplete results.
- **F5-3 — PLAUSIBLE, HIGH.** Silent incomplete results without `shards.tolerant`: `SearchHandler` sets
  `partialResults` only when `srsp.getException() != null`, but a shard returning HTTP 200 with a body-level
  error (no exception — the `LBHttp2SolrClient` `code==200` CancelledException path logs and treats as
  success) is counted as complete → partial results reported as complete.
- **F5-4 — PLAUSIBLE, MEDIUM.** `commExecutor` is never instantiated (init commented out) and
  `maximumPoolSize=Integer.MAX_VALUE`/`queueSize=-1` are dead — the factory's pool knobs no longer govern
  anything, so there's no Solr-side bound/backpressure on fan-out and no `CallerRuns` throttle for nested
  distributed requests (deadlock risk). `ServerIterator` always tries `servers.get(0)` first and only
  advances on failure → within a shard, "load balancing" is failover-only (no rotation); and a configured
  `loadBalancerRequestsMaximumFraction < 1` can floor `numServersToTry` to 0 → "No live SolrServers" for a
  shard that has replicas.

## F6 — `CaffeineCache` / `CommitTracker` (cache + autocommit)

- **F6-1 — PLAUSIBLE, HIGH (stale-searcher class).** `CommitTracker` has a single fixed
  `openSearcher`/`softCommit` per tracker and `run()` always commits with those fixed flags; a `commitWithin`
  request coalesced/routed onto the **hard** tracker (configured `openSearcher=false`, common when
  autoSoftCommit handles visibility) runs with `openSearcher=false` → docs flushed but the main searcher
  **not reopened** (exactly the fork's documented stale-searcher failure). The tracker offers no per-commit
  `openSearcher` override outside the test-only setter. Confirm how `DirectUpdateHandler2` routes
  `commitWithin`/soft requests. Related: `_scheduleCommitWithin` can drop a commit on a cancel-then-`closed`
  race → docs never made visible.
- **F6-2 — CONFIRMED, MEDIUM.** `CaffeineCache.buildCache` does `newCache.putAll(prev.asMap())` on a live
  resize (`setMaxRamMB`/`setMaxSize`) **without** adding to `ramBytes`, while `onRemoval` later decrements
  `ramBytes` for those same entries → `ramBytes.sum()` goes **negative**, corrupting `ramBytesUsed()` and
  therefore RAM-based eviction decisions and the reported metric. (`put`/`computeIfAbsent` both add to
  `ramBytes`; `putAll` is the only insert path that doesn't.)
- **F6-3 — CONFIRMED, LOW.** `CommitTracker.didCommit()` is an **empty no-op** (the `docsSinceCommit.set(0)`
  is in `didRollback()`, not here) → an external/explicit or time-triggered commit doesn't reset
  `docsSinceCommit`, so the maxDocs autocommit fires a redundant commit shortly after an unrelated commit.
- **F6-4 — PLAUSIBLE, MEDIUM.** `CaffeineCache.close()` dispatches `invalidateAll()`+`cleanUp()` to the
  shared `ParWork` executor and immediately `ramBytes.reset()`s — the async `onRemoval` decrements race the
  reset (negative/garbage `ramBytes`), and deferring invalidation keeps reader-bound `DocSet`/`DocList`
  values (and the closed searcher's reader) reachable under executor backpressure during reload storms.

## Round-4 top priority

1. **F1-1 / F1-2** — `ParWork` swallows interrupts and (with `ignoreExceptions=true`) close-task failures → partial close reported as success; this underpins the whole shutdown story.
2. **F4-1 / F4-2** — replication `fetchPackets` uses `min(size,PACKET_SZ)` instead of `packetSize` (>8MB corruption) with no body-level checksum verification → corrupt index installed, recovery "succeeds".
3. **F5-1** — distributed-search `take()`/`cancelAll` `pending` desync (escape hatch commented out) → query hang.
4. **F2-1 / F2-2** — IndexWriter closed without `iwLock` / handed out after `closed` → use-after-close on the update path.
5. **F6-1** — `CommitTracker` fixed `openSearcher` flag → a routed `commitWithin` may not reopen the searcher (stale-searcher class).
6. **F3-1 / F3-2** — `awaitNanos(250)` ns spin; unchecked `_version_` cast NPE on in-place RTG.

> **Commented-out-safety theme (now overwhelming).** Round 4 alone adds: `ParWork` interrupt handling
> bypassed; `DefaultSolrCoreState.close()` `iwLock` commented out; `getIndexWriter` shutdown guard commented
> out; `HttpShardHandler.take()` timeout/closed escape hatch commented out; `HttpShardHandlerFactory.commExecutor`
> init commented out; `ReplicationHandler.markReplicationStop` and several `Directory.release()` calls commented
> out; `ParWorkExecutor` thread-local cleanup commented out. Together with Parts D–E (DOWNNODE publish,
> `StatePublisher.closed`, `coreRefCount` decrement, upload-limit enforcement, ConnectionLoss republish,
> multipart cleanup), this is a systemic pattern: **a dedicated pass to triage every commented-out guard/cleanup
> block — re-enable or consciously delete — would likely retire a large fraction of these findings at once.**

> Confidence caveat unchanged: **CONFIRMED** items checked against source this session; **PLAUSIBLE** items are
> line-cited and consistent but warrant a focused confirm (and the concurrency/replication ones a reproduction)
> before fixing.

---

# Part G — Round 5: ZK client, cluster-state/routing, query engine, update loaders, Directory cache, auth + shared executors

Fifth sweep, over: the full ZooKeeper client (`SolrZkClient`/`ConnectionManager`/`ZkCmdExecutor`), the
cluster-state + routing model (`DocCollection`/`ClusterState`/`CompositeIdRouter`), `SolrIndexSearcher`,
the update loaders (`UpdateRequestHandler` + Javabin/Json/XML), `CachingDirectoryFactory`, and
`PKIAuthenticationPlugin` + `UpdateShardHandler`. **CONFIRMED** = verified against source this session;
**PLAUSIBLE** = line-cited, consistent, not personally re-verified.

> Two reassuring **clears** this round: the `CompositeIdRouter`/`HashBasedRouter` hash-range/routing math is
> sound (no misroute / boundary bug found), and `PKIAuthenticationPlugin` **does** cryptographically verify
> every `SolrAuth` header against the sender's published key (it does not fabricate the INTERNAL/`$` trust) —
> the REMOTEQUERY authorization-skip exposure is in `HttpSolrCall` (already filed as **E3-2**), not in PKI.

## G1 — ZooKeeper client (`SolrZkClient` / `ConnectionManager` / `ZkCmdExecutor`)

- **G1-1 — CONFIRMED, HIGH.** `SolrZkClient.SetData` (~1528–1545) snapshots the `ZooKeeper` handle in its
  **constructor** and `execute()` uses that field; `setData(...,retryOnConnLoss=true)` builds one `SetData`
  and `ZkCmdExecutor.retryOperation` re-invokes the **same** instance. Every other op (create/getData/
  exists/delete) reads `connManager.getKeeper()` fresh per `execute()` — `setData` is the outlier. After a
  session expiry swaps the `SolrZooKeeper`, every retry hits the **old/closed** handle → the state/data write
  silently never lands on the live session while the retry loop spins to exhaustion. **Fix:** read
  `connManager.getKeeper()` inside `SetData.execute()`.
- **G1-2 — CONFIRMED, HIGH (data loss).** `ZkMaintenanceUtils.clean(zkClient, path, filter)` (~276–293):
  after collecting filter-matched nodes, the loop body calls `clean(zkClient, path)` — the **root**, not
  `subpath` — so `SolrZkClient.clean(path, nodeFilter)` deletes the **entire** subtree regardless of the
  predicate (and once per matched node). **Fix:** `clean(zkClient, subpath)`.
- **G1-3 — PLAUSIBLE, CRITICAL.** `ZkCmdExecutor.retryOperation` retries `create` on `ConnectionLoss` — but
  ConnectionLoss doesn't mean the create failed; the retry then throws `NodeExistsException` (not caught by the
  ConnLoss/SessionExpired clause) for a create that actually succeeded → spurious hard failure on
  ephemeral/`makePath`/`mkdir` during a connection blip (and orphan nodes for sequential creates). No
  already-created reconciliation.
- **G1-4 — PLAUSIBLE, HIGH.** Several async writes swallow non-zero result codes: `setData`/`deleteAsync`/
  `create(Create2Callback)`/`sync` return void with no centralized rc handling; `getData(List)` drops a
  non-NONODE error to a `log.warn` and returns "successfully" with the path missing from the map → a failed
  write/read is indistinguishable from success/absent. `mkDirs` ignores the `latch.await(5s)` return and records
  only the first non-NODEEXISTS error.
- **G1-5 — PLAUSIBLE, MEDIUM.** `ConnectionManager.waitForConnected` checks `keeper.getState().isConnected()`
  on the volatile keeper while `reconnect()` sets `connected=false` and swaps `keeper` at different points with
  no shared lock → a waiter can observe the old (expiring) handle as connected and issue an op on a
  being-torn-down session. `zkCallbackExecutor.shutdownNow()` in `close()` races in-flight watch dispatch and the
  `onReconnect` task (submitted to the same executor) → `RejectedExecutionException` silently drops a watch
  notification or the whole reconnect (watches never re-registered).

## G2 — Cluster state + routing (`DocCollection` / `ClusterState` / `CompositeIdRouter`)

- **G2-1 — CONFIRMED, HIGH (stale routing).** `StateUpdates extends ConcurrentHashMap` with **no `hashCode`
  override** and `AtomicInteger` values (also no override → identity hash); `DocCollection.updateState` does
  in-place `sateForReplica.set(state)` (line 603); `BaseCloudSolrClient` uses
  `col.getStateUpdates().hashCode()` as a freshness token (1216, 1242). A DOWN→RECOVERING→ACTIVE→LEADER
  transition that mutates an **existing** entry in place leaves the map hashCode **unchanged**, so the client's
  `updateHash == col.getStateUpdates().hashCode()` check treats stale state as fresh → keeps routing on a stale
  replica view. Masked only when the separate `stateUpdatesVersion`/`znodeVersion` is independently bumped on
  the same fetch; the hash check is dead weight that gives false confidence. (The hash *does* change when entries
  are added/removed, so structure changes are caught — only in-place state flips are invisible.)
- **G2-2 — PLAUSIBLE, MEDIUM.** `DocCollection.copy()`/`getSlicesCopy()` are shallow (share `Replica` objects),
  but `copy()` builds a **fresh empty** `stateUpdates` seeded in `setStates()` from `replica.getPublishedState()`
  (registration-time DOWN baseline) with `stateUpdatesVersion == -1`, while the shared `Replica.getState()` still
  reads the canonical live atomic → one object yields two answers. Any consumer of `getClusterstate()`'s copy that
  reads `getStateUpdates()`/`getStateUpdatesZkVersion()` as authoritative sees DOWN baselines and version -1.
- **G2-3 — PLAUSIBLE, MEDIUM.** `StatePlaneReader.carryForwardStateUpdates` does
  `newState.setStateUpdates(prev.getStateUpdates())`, aliasing one live map (and its AtomicIntegers) across two
  DocCollections and side-effecting `prev`'s map via `setStates`' `computeIfAbsent`; if `prev` is still referenced
  by another in-flight `ClusterState` snapshot, `updateState` on `newState` bleeds into `prev`.
- **G2-4 — PLAUSIBLE, LOW.** `Slice.getLeader(liveNodes)` writes the shared `this.leader` cache field as a
  read side-effect (Slices are shared across shallow copies) — currently only `toString()` reads it, so cosmetic,
  but it's an unguarded write to shared state on a "read" path. And `getLeader` returns **null** during the
  deliberate demote-then-promote leader-handoff window → a routing caller doing `getLeader().getCoreUrl()` NPEs
  under normal leader churn unless it null-checks.
- **Verified non-bug:** all of `Range.includes/overlaps/isSubsetOf`, `sliceHash`/`KeyParser`/`getRange`/the
  triLevel bit-mask math, `partitionRange`, and the `getActiveSlices` shuffle (filters to ACTIVE, so split
  sub-shards in CONSTRUCTION/RECOVERY are excluded) are correct — no misroute.

## G3 — `SolrIndexSearcher` (query/docset engine)

**Clean — no correctness bug found (verified).** `SolrIndexSearcher.java` is byte-identical to the fork
baseline (`git diff` empty) and is standard Apache Solr 9.x. Verified against source + the `lucene-core-9.0.0`
API: the `QueryResultKey` captures everything result-affecting (query, order-insensitive filters, sort,
flags incl. `GET_SCORES`, `minExactCount`; offset/len correctly excluded because the cache stores a keyed
*superset* sliced by `subset(offset,len)`); the filterCache `getDocSet(DocsEnumState)` adds `sub.slice.start`
to reader-relative docids and applies `liveDocs` (no deleted-doc leak, no wrong-reader base);
`getProcessedFilter` union/intersection ordering is correct; and the Lucene-9.0.0 collector ports
(`TopScoreDocCollector.create`, `TopFieldCollector.create`/`populateScores`, the `scoreMode`/`hitsRelation`
logic) match the 9.0.0 signatures. **Next audit target if extending:** the collaborator set-algebra classes
(`DocSetUtil`, `SortedIntDocSet`, `BitDocSet`, `DocSlice`) — out of scope here, and where a reader-relative
vs global docid or an intersection off-by-one would actually manifest if one exists.

## G4 — Update loaders (`UpdateRequestHandler` / Javabin / Json / XML)

- **G4-1 — CONFIRMED, MEDIUM (commit-timing).** `JavabinLoader.delete` (~140–175): `delcmd.commitWithin` is set
  once (143), but `delcmd.clear()` after each deleteById (164) resets it to -1, and the deleteByQuery loop runs
  *after* with no re-seed → only the **first** deleteById honors `commitWithin`, and **all** deleteByQuery entries
  lose it whenever mixed with any deleteById. Same pattern in `XMLLoader.processDelete` (`clear()` wipes the
  `<delete commitWithin=N>` attribute for the 2nd+ `<id>`). → DBQ/extra deletes commit on the default schedule,
  not within the requested window.
- **G4-2 — CONFIRMED, MEDIUM (leak).** `JsonLoader` `finally` has `// IOUtils.closeQuietly(reader);` commented out
  (no drain either, ~123–125); `XMLLoader` XSLT-path `finally` has both `closeQuietly(is)` and `readFully(is)`
  commented out (~134–137) and `parser.close()` commented on both paths → content-stream/reader leak per update,
  which under the HTTP/2 pooled-buffer regime can wedge connection reuse.
- **G4-3 — PLAUSIBLE, MEDIUM.** `JavabinLoader` multistream (~106–119) assumes strict NamedList-then-bytes
  alternation; a bytes segment arriving before any NamedList silently indexes with the prior/`old` params rather
  than erroring.
- **Verified non-bug:** no loader casts a deserialized list to `ArrayList` (no fastutil CCE); add-command
  `commitWithin`/`overwrite`/`version` propagate correctly; a mid-stream parse error still propagates to the client
  (the already-applied docs remaining is normal Solr semantics, not silent success); child docs preserved in the
  javabin codec.

## G5 — `CachingDirectoryFactory` (Directory cache)

- **G5-1 — CONFIRMED, HIGH (architectural).** The fork **removed the entire refcount/`CacheValue`/close-deferral
  machinery**. `get()` returns a **shared** Directory via `computeIfAbsent` with no per-holder accounting and no
  `closed` check; `release(fullPath)` (~163–166) evicts from both maps but **never closes** the Directory (so the
  Lucene `LockFactory` write-lock is never released → next open on that path risks `LockObtainFailedException`) and
  doesn't `normalize()` its key while `get()` does (key-space mismatch → leak on the intended release path);
  `remove(Directory)` (~153–161) unconditionally `closeQuietly(dir)` + deletes on-disk with no ref check, and a
  null `fullPath` (dir not in `byDirCache`) leaves a **closed** Directory in `byPathCache` to be served to the next
  `get()`. The live risk today is `remove()` closing a Directory another holder still uses (use-after-close /
  index corruption); `release()` is a latent landmine (its callers are commented out — same theme). `close()` vs
  `get()` share no lock → close/get race leaks a freshly-inserted Directory; registered `closeListeners` are
  **never invoked**.

## G6 — `PKIAuthenticationPlugin` + `UpdateShardHandler`

- **G6-1 — CONFIRMED, HIGH.** `UpdateShardHandler.close()` (~236–256) tears down the four `Http2SolrClient`s but
  **never shuts down `recoveryExecutor`** (created ~152, instrumented ~192; no `shutdownAndAwaitTermination`
  anywhere — the upstream barrier is commented out at ~144–149). The executor that drives recovery
  `CompletableFuture` chains outlives the `recoveryOnlyClient` they use → recovery tasks run against a **closed**
  client, and there's no drain barrier guaranteeing recovery stopped before teardown (inverts the required
  shutdown order; ties to **F2-5**).
- **G6-2 — CONFIRMED, MEDIUM.** `UpdateShardHandler.setSecurityBuilder` (~268–274) calls
  `builder.setup(recoveryOnlyClient)` **twice** (lines 270 and 272 — copy-paste) → the PKI listener factory is
  registered twice on `recoveryOnlyClient`, so every recovery request stamps the `SolrAuth` header **twice**
  (`headers(...add(...))`, not `set`) → the receiver may pick either value, plus double token work per recovery
  control request. **Fix:** drop the duplicate / set up the intended client.
- **G6-3 — PLAUSIBLE, HIGH.** `PKIAuthenticationPlugin.decipherHeader` (~142–163) passes a `null` public key
  (from `getRemotePublicKey` when the node isn't in live-nodes or the fetch throws) straight into `parseCipher` →
  `CryptoKeys.decryptRSA(...,null)` throws, caught, returns null → a **transient** key-fetch miss becomes a hard
  auth rejection of an otherwise-valid token, with no distinction from a bad signature (both retry branches can
  pass null).
- **G6-4 — PLAUSIBLE, MEDIUM.** The replay window is timestamp-only (`usr + " " + currentTimeMillis`, 15s, no
  nonce/seen-cipher cache), and the check `(receivedTime - timestamp) > MAX_VALIDITY` is one-directional — a
  **future** timestamp (negative delta) passes, so a captured `SolrAuth` header (incl. the `$`/SU principal that
  bypasses authorization) is replayable for 15s and a clock-skewed/attacker-chosen future timestamp extends that
  window. `keyCache.put` has no concurrency guard against two threads installing different-generation keys during
  rotation. (Upstream-design weakness, but it's the mechanism by which a captured INTERNAL/SU token is trusted.)

## Round-5 top priority

1. **G1-1 / G1-2** — `setData` retries against a stale ZK handle (silent lost write after reconnect); `clean(path,filter)` deletes the whole tree (data loss). Both ~one-line fixes.
2. **G5-1** — `CachingDirectoryFactory` refcounting removed → `remove()` closes a shared Directory in use; `release()` never releases the lock.
3. **G2-1** — `StateUpdates.hashCode()` value-blind freshness token → clients route on stale replica state.
4. **G6-1 / G6-2** — `recoveryExecutor` never shut down (recovery against closed clients); `recoveryOnlyClient` double PKI setup (duplicate auth header).
5. **G1-3 / G1-4** — non-idempotent `create` retried (spurious NodeExists); async write rc's swallowed.
6. **G4-1 / G4-2** — `commitWithin` wiped between sibling deletes; JSON/XML loader stream closes commented out.

> Commented-out-safety theme (still accumulating): round 5 adds `JsonLoader`/`XMLLoader` stream closes,
> `CachingDirectoryFactory.release()` callers, and `UpdateShardHandler`'s `recoveryExecutor` shutdown — all
> commented out. The standing recommendation holds: a dedicated triage pass over every commented-out guard/cleanup
> block (re-enable or consciously delete) would retire a large share of findings across Parts D–G at once.

> Confidence caveat unchanged: **CONFIRMED** = checked against source this session; **PLAUSIBLE** = line-cited and
> consistent but warrant a focused confirm (concurrency/ZK/auth ones a reproduction) before fixing.

---

# Part H — Round 6: second pass over the most-important already-reviewed classes (miss-hunt)

A deliberate re-read of the highest-value hot classes already covered (cluster-state write path, update
forwarding, the Replica/Slice state model), hunting only for bugs **missed** in earlier rounds. Six agents
were launched; two (TransactionLog/JavaBinCodec, UpdateLog/PeerSync/RecoveryStrategy) and the SolrCore agent
**did not complete** — they hit a hard daily API budget cap (HTTP 429), not a clean "nothing found". Those three
subsystems are therefore **NOT re-covered this round** and remain at their Round-1/3/4 coverage; see the gap note
at the end. Everything below was verified against source this session unless tagged PLAUSIBLE.

## H1 — `ZkStateWriter` single-leader demotion is skipped when the collection structure isn't loaded → stale LEADER wedges leader migration — **CONFIRMED, HIGH**

`ZkStateWriter.java:318` (`enqueueStateUpdates`). The single-leader-per-slice demotion (whose own comment
documents that a lingering non-ephemeral LEADER "permanently wedges leader migration" —
`HttpPartitionTest.testLeaderZkSessionLoss`, ForceLeader variants) is gated on:

```java
DocCollection dc = collName == null ? null : cs.get(collName);
Map<Integer,Integer> curMap = this.stateUpdates.get(collectionId);
if (dc != null && curMap != null) { /* demote other LEADERs in slice */ }
```

When `dc == null` — overseer takeover applying state updates before/while `cs` is repopulated, or a new
LEADER published before its structure-change lands — the demotion is silently skipped. Because StateUpdates
entries are **not ephemeral**, the stale LEADER from the prior leader survives, and `Slice.getLeader()`
(`getRawState()==LEADER`, first-in-iteration) can then return two-leaders-collapsed-to-one nondeterministically.
This is exactly the failure mode the block was written to prevent, reachable on the structure-not-yet-known path.
**Fix direction:** when `dc==null`, still demote against `curMap` using the StateUpdates membership (or defer the
LEADER apply until structure is present) rather than no-op'ing the invariant.

## H2 — `SolrCmdDistributor` swallows a 404 from a replica forward: no client error, no recovery, replica diverges — **CONFIRMED, MEDIUM-HIGH**

`SolrCmdDistributor.java:443` (`onFailure`):

```java
if (code == 404) {
  cancelExeption = t;
  return;          // <-- NOT added to allErrors
}
```

Every other failure branch does `allErrors.put(req, error)` (or retries). A 404 (follower core
unloaded/closed/renamed mid-forward) sets `cancelExeption` and returns **without recording the error**.
`doDistribFinish`/`doFinish` never see it → no shard-term bump → no leader-initiated recovery scheduled for that
follower, and the client still gets success (the add applied on the leader). `cancelExeption` only surfaces on the
*next* distrib call on this distributor; for the last/only doc in a batch there is no next call, so the 404 is lost
entirely and the follower silently diverges. Ties into the documented "leader→replica forward failures are SILENT"
fork behavior, but this is a distinct, fixable swallow.

## H3 — `ZkStateWriter` slice-state branch NPEs on an unknown collection id (sibling of A2) — **CONFIRMED, MEDIUM-HIGH**

`ZkStateWriter.java:354` (`enqueueStateUpdates`, the `sliceStates.forEach` pass):

```java
DocCollection docColl = cs.get(collection);   // collection = idToCollection.get(collectionId), may be null
boolean didUpdate = false;
for (StateUpdate update : updates) {
  Slice slice = docColl.getSlice(update.sliceName);   // <-- docColl unguarded
```

A2 fixed/flagged the **replica-state** branch; this is the **slice-state** (`UPDATESHARDSTATE`) branch and has its
own unguarded `docColl` deref. If the structure-change for `collectionId` hasn't been applied yet (or
`idToCollection` miss → `cs.get(null)` → null), `docColl.getSlice(...)` throws NPE, aborting the whole
`enqueueStateUpdates` call — so any replica-state batch processed in the same invocation is dropped too. Same
null-guard fix as A2.

## H4 — `ZkStateWriter.write()` clears `dirtyStructure` before the async `setData` succeeds → a non-ConnectionLoss async error abandons the structure write — **PLAUSIBLE, MEDIUM**

`ZkStateWriter.java:710-726`. `dirtyStructure.remove(collection.getName())` runs synchronously **before** the
async `setData` callback fires. In the callback, only `ConnectionLossException` re-adds to `dirtyStructure` and
reschedules; every other non-zero `rc` (e.g. `SessionExpired`, `NoNode`) is merely logged. Since `dirtyStructure`
was already cleared and the outer `writeStructureUpdates` retry loop only catches a **thrown** `BadVersion` (not an
async-callback error code), such an error leaves in-memory `cs` ahead of the persisted `state.json` with no retry.
(Note: the agent framed this as a BadVersion bug, but `setData` is called with version `-1`, so BadVersion can't
actually occur — the real exposure is SessionExpired/NoNode, which downgrades severity but the swallow is real.)

## H5 — `getCollectionUrls` operator-precedence bug admits/excludes replicas by `(A && B && C) || BUFFERING` — **CONFIRMED, MEDIUM**

`DistributedZkUpdateProcessor.java:941`:

```java
if (isNodeLive(replica.getNodeName()) && types.contains(replica.getType())
        && replica.getState() == Replica.State.ACTIVE || replica.getState() == Replica.State.BUFFERING) {
```

`&&` binds tighter than `||`, so this is `(live && typeOk && ACTIVE) || BUFFERING`. A BUFFERING replica is added
**regardless** of node-liveness and type. The sibling `getReplicaNodesForForCommit` (line 1006) correctly writes
`isNodeLive(...) && (ACTIVE || BUFFERING)`, proving the intended grouping and that line 941 is an unintended
precedence bug. Impact is narrowed by `getCollectionUrls` operating on `docCollection.getLeader(slice, liveNodes)`
(already live), so today the practical effect is "a BUFFERING leader bypasses the type gate" — but the condition is
wrong and one refactor away from a real misroute. One-paren fix.

## H6 — `couldIbeSubShardLeader` / `amISubShardLeader` deref `coll.getSlice(myShardId)` with no null guard → uncaught NPE/500 during split/state races — **CONFIRMED, MEDIUM**

`DistributedZkUpdateProcessor.java:957` and `:969`:

```java
Slice mySlice = coll.getSlice(myShardId);
Slice.State state = mySlice.getState();    // <-- mySlice can be null
```

`getSubShardLeaders` wraps the same `getSlice` exposure in `catch (Throwable)`; these two do not. On the FROMLEADER
path, if this core's shard was just removed/renamed in the freshly-read clusterState (split that deleted the parent
slice, or a stale-vs-new state race), `getSlice` returns null and the deref throws a raw NPE out of `setupRequest`
as an opaque 500, instead of cleanly returning "not a sub-shard leader". Add a null guard returning `false`.

## H7 — `ReplicaMutator.updateState` re-fetches `slice` then calls `slice.getReplicasCopy()` unguarded — **CONFIRMED, MEDIUM**

`ReplicaMutator.java:252-253`. Earlier in the method `slice` is computed null-tolerantly and its LEADER/type
carry-over is guarded by `if (slice != null)`, but after building the new `Replica` the code reassigns
`slice = collection.getSlice(sliceName)` and immediately calls `slice.getReplicasCopy()` with no null check. A
stale/mismatched message naming a slice not present in this collection (or a slice removed between the two
`getSlice` calls) → NPE, dropping the state update. Mirror the earlier null guard.

## H8 — `CollectionMutator.deleteShard` mutates the shared live `Slice` props map in place — **CONFIRMED, MEDIUM**

`CollectionMutator.java:116-119`:

```java
Map<String, Slice> newSlices = new LinkedHashMap<>(coll.getSlicesMap());  // shallow: same Slice instances
newSlices.get(sliceId).getProperties().put("remove", "true");            // <-- mutates the canonical Slice
DocCollection newCollection = coll.copyWithSlicesShallow(newSlices);
```

The shallow copy shares the live `Slice` instances (and their props maps) held by the canonical `DocCollection` in
`cs`. Stamping `remove=true` onto `getProperties()` poisons the **live** slice instance: a concurrent
`getClusterState` snapshot taken before the tombstone-drop sees a slice flagged for removal, and if the structure
write later fails the live slice is permanently marked `remove=true` while still present. Build a fresh props map
(the pattern used in the merge path) instead of mutating the shared one.

## H9 — `finish()` on a closed distributor stores an `error.req == null` synthetic `AlreadyClosedException` → NPE while building the client exception — **PLAUSIBLE, MEDIUM**

`SolrCmdDistributor.java:130-135`. On shutdown, `finish()` synthesizes `error.t = new AlreadyClosedException()`
with `error.req` left null and puts it in `allErrors`. The consumer `DistributedUpdatesAsyncException.buildMsg`
(`DistributedUpdateProcessor.java:1412`) does, for a sole error, `error.req.uReq.getHeaders()` → **NPE** when
`req==null`, so a clean "already closed" signal turns into an NPE-derived 500 (and `buildCode` treats the
default `statusCode==0` as 500). A shutdown mid-distribution thus produces a confusing failure rather than a clean
`AlreadyClosedException`. Set a non-null `req` (or special-case the synthetic error in `buildMsg`/`buildCode`).

## H10 — `ZkStateWriter` live-node-down listener fires `writeStateUpdatesInternal` with a shared, growing `collIds` set from inside each collection's `compute()` — **PLAUSIBLE, LOW-MEDIUM**

`ZkStateWriter.java:955/982`. `collIds` is declared once outside the per-collection loop and accumulated across
every collection that lost a replica, while `writeStateUpdatesInternal(collIds, 1)` is invoked inside the
bin-lock `compute()` of **each** such collection, each time passing the growing union. Already-written collections
are re-submitted on the writer executor (redundant), and a collection whose DOWN `compute()` hasn't yet run can be
written from a stale (pre-DOWN) map, losing that node's DOWN mark until a later trigger. Snapshot/scope the id set
per collection.

## Minor / contract-level (verified, low severity)

- **`Replica.equals` (Replica.java:270) and `Slice.equals` (Slice.java:55)** cast to the target type *before* any
  `instanceof`/`getClass` (or, for `Slice`, any null) check → `ClassCastException` for a foreign argument, and
  `Slice.equals(null)` → NPE (Slice has no null guard at all; Replica's `if (replica==null)` sits uselessly *after*
  the cast). Violates the `equals` contract (must return false for incomparable types). A5 already covers the
  separate `id==null` NPE in `Replica.equals`.
- **`Replica.getId()` (Replica.java:281)** returns the literal `"<collId>-null"` for synthetic (`id==null`)
  replicas, so two distinct synthetic replicas of one collection collide to the same `getId()`. Low impact (id-keyed
  dedupe/logging only).
- **`Replica.State.shortStateToState(0,…)` throws `IllegalStateException`** while the parallel
  `shortStateToLetterState(0)` returns `'U'` — an inconsistency suggesting 0 is an intended "unknown/uninitialized"
  sentinel. `getState()`/`getRawState()` would throw if a replica's live atomic ever holds 0. Reachability
  unproven (`getShortState` only emits 1–6), so PLAUSIBLE/LOW — but the two methods should agree.
- **`Slice.getLeader(liveNodes)` (Slice.java:296)** returns the first LEADER in (unordered fastutil) iteration and
  side-effect-writes the shared `volatile leader` on an otherwise-immutable snapshot. The cache is never read back,
  so the write is dead; the nondeterminism only bites during a two-LEADER handoff window — which H1 shows is
  reachable when demotion is skipped.
- **`DocCollection.updateState` (DocCollection.java:594)** writes the AtomicInteger in *this* collection's
  `stateUpdates` map, but a `Replica` shared from another collection whose `linkState` re-seat was refused
  (foreign owner) reads a *different* atomic — so the update can be invisible through this collection while
  `replicaCountMatchesStateUpdates()` still reports consistent. D5 cleared the read-side as correct; this is the
  write-side angle and is lower-confidence — flag for a focused confirm rather than a fix.

## Round-6 coverage gap (must-note)

Two of the six planned re-reviews **did not run to completion** (daily API budget cap, HTTP 429), so they add no
new coverage this round and any miss in them is still open:

- **`TransactionLog` + `JavaBinCodec`** — re-review aborted. Still at Round-1 coverage (A1 torn-read + the 17 prior
  fixes). This is the single most bug-dense subsystem in the fork and most deserves the re-pass; re-run when budget
  resets.
- **`UpdateLog` / `PeerSync` / `PeerSyncWithLeader` / `RecoveryStrategy`** — re-review aborted. Still at
  Round-2/Round-3 coverage (B2, D4, E5).
- **`SolrCore` / `DefaultSolrCoreState` / `DirectUpdateHandler2`** — agent did not report a result; treat as
  not-re-covered (still E1-1, F2-1, F2-2, F6-3).

Re-running just these three (one agent each) is the highest-value next step when the budget window resets.

## Round-6 top priority

1. **H1** (ZkStateWriter demotion skipped on `dc==null` → wedged leader migration) — highest; it directly defeats a
   guard the fork added specifically to stop a known hang.
2. **H2** (404 forward swallow → silent replica divergence) and **H3** (slice-state NPE aborting co-batched writes).
3. **H4/H7/H6/H8/H9** — null-guard / ordering / in-place-mutation fixes, mostly one- or two-line.
4. Re-run the three budget-aborted re-reviews (tlog/javabin, recovery, SolrCore).
