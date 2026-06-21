# solr-ref Fork — Working Context for Claude

## What this repo is

A heavily-modified Apache Solr fork. Branch `rip`. WIP "raw" commits — never revert,
stash, checkout, or reset other people's in-flight work. Do not commit or push without
being asked.

Parallel write work can be done using worktrees.

Core architectural departures from upstream Apache Solr:

- **StateUpdates-based Overseer** — replaced the classic queue-based `ClusterStateUpdater`.
  There is no `ClusterStateUpdater`. Replica state flows through the `StateUpdates` channel,
  not the overseer work queue.
- **Replica name IS core name** — `Replica.getName()` returns the unified name used as both
  the SolrCloud replica identifier and the core name. There is no separate `CORE_NAME_PROP`
  / `"core"` value. `replica.getStr(ZkStateReader.CORE_NAME_PROP)` returns null. Always use
  `replica.getName()`.
- **Replica strips STATE_PROP** — the `Replica` constructor removes `STATE_PROP` from the
  prop map. Live state lives in the StateUpdates channel. Read it via `replica.getState()`
  (uses `shortStateToState(n, published=true)`) or `replica.getRawState()` (published=false).
  Never read `replica.getStr(ZkStateReader.STATE_PROP)` — it is always null.
- **`Replica.State.LEADER`** — this fork defines a distinct `State.LEADER` (shortState=1).
  `shortStateToState(1, published=true)` returns `State.ACTIVE`, so `replica.getState()`
  and `isActive()` both treat LEADER as active. `getRawState()` uses `published=false` and
  returns the true `State.LEADER`. `Slice.getLeader()` checks `getRawState()==LEADER`.
- **HTTP/2 only** — the server runs HTTP/2. The deprecated Apache-`HttpClient`-based SolrJ
  clients have been **deleted**: `HttpSolrClient`, `CloudSolrClient`,
  `ConcurrentUpdateSolrClient`, `LBHttpSolrClient`, plus `DelegationTokenHttpSolrClient` and
  `HttpClusterStateProvider`. Use the HTTP/2 replacements: `Http2SolrClient`,
  `CloudHttp2SolrClient`, `ConcurrentUpdateHttp2SolrClient`, `LBHttp2SolrClient`,
  `Http2ClusterStateProvider`. Shared abstract bases (`BaseCloudSolrClient`, `LBSolrClient`,
  `BaseHttpSolrClient` — note `RemoteSolrException` lives on `BaseHttpSolrClient`) remain.
  Builder API differences: `withBaseUrl` (not `withBaseSolrUrl`), `connectionTimeout`/
  `idleTimeout` on `Http2SolrClient.Builder` (not `withConnectionTimeout`/`withSocketTimeout`);
  cloud/CUSC timeouts are set on the injected `Http2SolrClient`. `SolrTestCaseJ4.getHttpSolrClient`
  / `getCloudSolrClient` / `getConcurrentUpdateSolrClient` / `getLBHttpSolrClient` all return the
  HTTP/2 types. The shared `DebugServlet` test util now lives in its own class under
  `solrj/.../impl/DebugServlet.java`.
- **Memory-mapped TransactionLog** — custom Agrona `MappedResizeableBuffer`-backed tlog
  with a custom `JavaBinCodec`. Many correctness bugs have been fixed (see tlog section).
- **Lucene 9.0.0** — pinned to the released 9.0.0 (not SNAPSHOT). Several API ports were
  required. Do not try to upgrade; 9.12.x requires a multi-wave API port.

---

## Build & environment

- **Java:** jenv, per-project. `.java-version` pins to `11.0`. Do NOT set JAVA_HOME
  globally — it fights jenv. Run `./gradlew` from the repo root; the `.java-version` file
  makes jenv pick Java 11.
- **Repos:** `~/.gradle/init.gradle` was replaced (from the Apple artifactory mirror) to
  use `gradlePluginPortal()` + `mavenCentral()`. Original backed up at
  `~/.gradle/init.gradle.apple-bak`. If you ever need Apple internal artifacts, restore it.
- **Build:** `./gradlew :solr:core:compileJava` to check for compile errors.
  `./gradlew :solr:core:test --tests "FQN.ClassName"` to run a test.
  Always use absolute gradlew path; cwd persists across Bash calls.
- **Lock file:** after changing `versions.props`, run `./gradlew --write-locks`.
- **Test results:** `solr/core/build/test-results/test/TEST-*.xml` — gradle wipes and
  rewrites these on every run. They are authoritative. Never `tail` the console for
  pass/fail; it truncates and creates false passes. Check:
  ```
  grep -o 'tests="..." skipped="..." failures="..." errors="..."' TEST-*.xml
  ```
  GREEN = `failures="0" errors="0"`, `tests` > 0, `skipped` only if methods are
  `@Nightly`/`assume`-gated.
- **Test logging:** INFO is suppressed for `o.a.s.*` in the test log4j2 config. Use
  `log.error(...)` for ad-hoc tracing. Per-node logs in MiniSolrCloudCluster land in
  `solr/core/build/test-results/test/outputs/OUTPUT-*.txt` (not the gradle console).
- **Agrona / security policy:** the custom `TransactionLog` reflects into `sun.nio.ch`.
  Requires `permission java.lang.RuntimePermission "accessClassInPackage.sun.nio.ch"` in
  `gradle/testing/randomization/policies/solr-tests.policy` AND
  `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` in
  `gradle/testing/defaults-tests.gradle`. Without these, any indexing test fails with
  `ExceptionInInitializerError`.
- **One test at a time:** never run concurrent `:solr:core:test` — they clobber
  test-results.

---

## Test policy (updated directive)

**`@Ignore` is not an acceptable end-state.** Every ignored test must end as:

1. **Enabled + passing** — stale ignore removed, or production/test bug fixed.
2. **Adapted + passing** — test logic updated to match the fork's new implementation.
3. **Deleted** — the test exercises behavior the fork deliberately removed (e.g.,
   queue-based Overseer internals, replica-property operations).

Re-ignoring with a comment is only a temporary holding state, not a resolution.

### Known open test gaps (require production work before tests can pass)

- **`TestTlogReplica`** — multiple StateUpdates-model gaps (HTTP 400 on tlogReplicas=4
  create, two-leader state, replicas stuck RECOVERING). Also: `SolrCore.initIndex` only
  creates an empty index when `!coreContainer.isZooKeeperAware() || newCore`, so a cloud
  core restarting with an empty/lost index dir skips creation → `IndexNotFoundException`.
  High blast radius; fix cautiously.
- **`TestLeaderElectionWithEmptyReplica` (SOLR-9504)** — the prescribed fix (delete
  `setTermEqualsToLeader` in `ZkController.register()`) is correct as a data-safety guard
  but is not a standalone fix. See the detailed diagnosis section below.
- **Replica-property / preferred-leader** — `WorkQueueWatcher.processQueueItems` switch
  only handles `STATE` and `UPDATESHARDSTATE`. All ADDREPLICAPROP/DELETEREPLICAPROP/
  BALANCESHARDUNIQUE calls poison the overseer queue and cascade timeouts into later tests.
  Delete tests in this family until the Overseer dispatch is extended.
- **`PeerSyncTest`** — pre-existing failure on baseline HEAD, not a regression. Do not
  treat as a gate.
- **`DeleteReplicaTest.raceConditionOnDeleteAndRegisterReplica`** — pre-existing broken on
  baseline HEAD, not a regression.

---

## Overseer / StateUpdates architecture — what to know

The classic Overseer queue (`ClusterStateUpdater`) is replaced. Key classes:

- `StatePublisher` — submits replica state updates into the StateUpdates channel.
- `ZkStateWriter` — consumes structure changes and state updates, writes to ZK.
- `WorkQueueWatcher` — handles the overseer state-update work queue (STATE, UPDATESHARDSTATE
  only).
- `CollectionWorkQueueWatcher` — handles collection/configset admin operations.
- `OverseerElectionContext` — wins the overseer election and starts the Overseer.

**Overseer leader node** — written as an ephemeral CHILD node under
`/overseer/overseer_elect/leader/<id>` (not as JSON data on the parent). Read via
`getChildren(OVERSEER_ELECT + "/leader")`, not `getData`. The `OverseerTaskProcessor.getLeaderId`
fix (reading children) is in place on branch `rip`.

**OVERSEERSTATUS stats** — `CollectionWorkQueueWatcher` wraps dispatch with
`stats.time()/success()/error()` accounting. `OverseerStatusCmd` reads this via
`OverseerTaskProcessor.getLeaderNode`.

**`Overseer.getZkController()`** — this is `public` (was package-private; made public to
allow `ZkStateWriter`, `QueueWatcher`, `OverseerCollectionMessageHandler`,
`OverseerConfigSetMessageHandler`, and `OverseerCollectionConfigSetProcessor` to route
through `overseer.getZkController()` instead of `cc.getZkController()`, which is null in
minimal test setups).

**`OverseerConfigSetMessageHandler` constructor** — takes `(CoreContainer cc, Overseer overseer)`,
not just `(CoreContainer cc)`. Call-sites: `CollectionWorkQueueWatcher`,
`OverseerCollectionConfigSetProcessor`.

**`QueueWatcher` constructor** — uses `overseer.getZkController()`, not `cc.getZkController()`.

---

## Leader election — what to know

**Shard leader election:**
- `ShardLeaderElectionContextBase.runLeaderProcess` writes the shard leader ephemeral node at
  `leaderPath + '/' + id` using `CreateMode.EPHEMERAL`.
- `canBecomeLeader(coreName)` in `ZkShardTerms` requires BOTH `haveHighestTermValue` (term ==
  maxTerm) AND no `recoveringTerm` flag. If either is false, the replica cannot become leader
  by the normal path.
- **`onlyLiveReplica` bypass** — if `!canBecomeLeader` but there is no other live replica,
  `ShardLeaderElectionContext.runLeaderProcess` sets `setTermToMax=true` and lets the candidate
  proceed regardless of terms. An empty replica with term 0 WILL win leadership via this path
  when the data-bearing replica is down. This is the SOLR-9504 root path. The
  `setTermEqualsToLeader` deletion (FIX 2) changes when the term is set but does NOT prevent
  the leadership win through the `onlyLiveReplica` bypass.
- **Term lifecycle:** `registerTerm` → 0. `startRecovering` → sets to maxTerm + adds
  `recoveringTerm` flag. `setTermEqualsToLeader` → sets to maxTerm + removes `recoveringTerm`
  flag. `doneRecovering` → removes `recoveringTerm` flag.

**Overseer leader election:**
- `LeaderElector.retryElection` — shuts down the old executor and creates a fresh one (to
  avoid `RejectedExecutionException` from the SynchronousQueue-based pool when reconnect
  fires while the previous election task is still running).
- `ZkController.start()` must be called after construction to initialize the overseer elector,
  reconnect callback, and `StatePublisher`. Tests that create a bare `ZkController` and expect
  leader election must call `start()`.
- After ZK session expiry and reconnect, `ConnectionManager.reconnect()` fires
  `disconnectListener` (which closes the elector) then `onReconnect.command()` (which calls
  `retryElection`). The fresh executor created in `retryElection` ensures the new election
  task is not rejected.
- The test `TestLeaderElectionZkExpiry` creates its polling `SolrZkClient` with
  `server.getZkHost()` (no chroot) to match the path space used by `chRootClient` in
  `ZkTestServer`. Using `server.getZkAddress()` (which adds `/solr` chroot) reads a
  completely different ZK path and never finds the leader node.

**`Replica.State.LEADER` vs `ACTIVE`:**
- `getState()` calls `shortStateToState(n, published=true)`. With `published=true`, shortState=1
  maps to `State.ACTIVE`. So `isActive()`, `getState()`, and `waitForState` checks for ACTIVE
  all correctly treat a LEADER-state replica as active.
- `getRawState()` calls `shortStateToState(n, published=false)`. shortState=1 maps to
  `State.LEADER`. `Slice.getLeader()` uses `getRawState()==LEADER` to identify the leader.
- Both `isActive(liveNodes)` and `isActive()` work correctly for LEADER without any change.

---

## SOLR-9504 diagnosis (TestLeaderElectionWithEmptyReplica) — open

Test: index 10 docs on a 1×1 collection, kill leader n1, async-add empty replica n2, restart
n1, expect both active + all docs. **Bug:** with n1 down, n2 (term 0, `canBecomeLeader=false`)
wins via the `onlyLiveReplica` bypass in `ShardLeaderElectionContext.runLeaderProcess` and
becomes an empty leader (`getOtherHasVersions()=false`, "become leader anyway" path ~line 213).
Deleting `setTermEqualsToLeader` at `ZkController.register()` is a correct data-safety guard but
does NOT stop the bypass win. A complete fix must make the `onlyLiveReplica` bypass refuse an
empty candidate (no update-log versions, not the original leader) when the shard previously had
data — or have `register()` check `canBecomeLeader` before `checkRecovery`. Stays `@Ignore`'d.

---

## Key production fixes applied (branch `rip`, all verified)

### ZkController / leader election

- **`OverseerTaskProcessor.getLeaderId`** — rewrote to read the ephemeral child node name
  under `/overseer/overseer_elect/leader/` instead of parsing JSON from the parent node
  (which always had null data). Removes unused `Stat` import and `ID` static import.
  Fixes OVERSEERSTATUS 500 and unblocks `TestLeaderElectionZkExpiry`.
- **`LeaderElector.retryElection`** — shuts down the old executor and creates a fresh
  `SynchronousQueue`-based `ThreadPoolExecutor` before re-joining, preventing
  `RejectedExecutionException` during session-expiry reconnect storms.
- **`Overseer.getZkController()`** — made public.
- **`ZkStateWriter`, `QueueWatcher`, `CollectionWorkQueueWatcher`,
  `OverseerCollectionMessageHandler`, `OverseerConfigSetMessageHandler`,
  `OverseerCollectionConfigSetProcessor`** — all route through `overseer.getZkController()`
  instead of `cc.getZkController()` (which is null in minimal test setups like
  `TestLeaderElectionZkExpiry`).
- **`OverseerConfigSetMessageHandler`** — constructor changed to `(CoreContainer, Overseer)`.

### ZkStateWriter

- **Tombstone drop bug** — `enqueueStructureChange` now correctly drops replicas with
  `"remove": true` in two order-independent passes (was order-dependent, so deleted replicas
  could linger in cluster state).
- **MODIFYCOLLECTION no-op bug** — the merge path now seeds `newDocProps` from the incoming
  `docCollection.getProps()` (was accidentally seeding from current collection and discarding
  incoming changes).
- **`CollectionWatch.canBeRemoved`** — now counts `propStateWatchers` size, not just
  `stateWatchers` and `coreRefCount`.

### Alias writes

- **`ZkStateReader.AliasesManager.applyModificationAndExportToZk`** — body was entirely
  commented out. Restored the standard retry-loop implementation. Alias create/modify/delete
  now actually write to ZK.

### Collection handlers

- **`CollectionsHandler.waitForActiveCollection`** — uses `replica.getState()` (not the
  stripped `replica.getStr(STATE_PROP)`) for the active-replica check.
- **`MigrateCmd.call`** — `split.key` null/blank check moved before collection lookups.
- **`DistributedZkUpdateProcessor.getSubShardLeaders`** — returned the never-assigned local
  variable `subLeaderNodes` instead of the populated instance field `nodes`. Fixed to build
  into and return the local. Repairs split-time doc forwarding.
- **`OverseerStatusCmd` / `CollectionWorkQueueWatcher`** — operation stats now recorded via
  `stats.time()/success()/error()` wrapping the dispatch. OVERSEERSTATUS returns real counts.

### TransactionLog / JavaBin (heavy, many bugs fixed)

17 distinct tlog/javabin correctness bugs were fixed (spurious VERSION bytes, write-position
not advancing, extern-string list desync, dropped `SolrDocumentList` docs, double-FREE of the
pooled HTTP buffer, `LogReader` skipping the header, buffered live deletes lost, etc.). Full
enumeration and detail live in memory file `solr-ref-tlog-javabin-bugs.md` — read it before
touching `JavaBinCodec`, `TransactionLog`, or the javabin request/response writers.

### HTTP/2 / Jetty

- **`JettySolrRunner` cleartext connector** — lists `httpFactory` (HTTP/1.1) first, then h2c,
  so one port serves HTTP/1.1 + h2c-upgrade + prior-knowledge-h2c. The old config only listed
  h2c, so legacy HTTP/1.1 clients got "Invalid Http response".
- **`SolrRequestParsers.parse()`** — restored form-urlencoded and multipart handling that had
  been stripped. Form-urlencoded: no content stream added (params already parsed by container).
  Multipart: `MultipartConfigElement` attribute + `PartContentStream` revival.
- **`SolrRequestParsers.parse()` wt=javabin guard** — only forces `application/javabin`
  content-type when `Content-Type` header is absent (`contentType == null`). Previously
  forced it unconditionally, routing XML/JSON bodies to javabin parser.
- **`BinaryResponseWriter.Resolver.resolve()`** — removed dead `LegacyFieldType` guard;
  `Utf8CharSequence` is now written directly for `StoredField`.
- **`JavaBinCodec.VERSION`** — made public; `ZkNodeProps.load` now detects javabin via
  `bytes[0] == JavaBinCodec.VERSION` instead of the hardcoded `2`.
- **`Http2SolrClient` double-FREE** — `SolrHttpRequest.freeBuffer()` guarded with
  `AtomicBoolean` so the pooled Agrona buffer is released exactly once.
- **`SolrCore.deleteNamedSnapshot`** — checks remaining snapshots before deleting an index
  commit; was unconditionally deleting even when another snapshot still referenced it.

### solrj update clients (ConcurrentUpdate streaming)

- **`ConcurrentUpdateSolrClient.sendUpdateStream`** — the streaming body's `ContentProducer`
  blind-cast Apache HttpClient's plain `OutputStream` to `FastOutputStream`
  (`(FastOutputStream) out`), throwing `ClassCastException` and silently aborting every
  streamed update (servlet `doPost` never fired). Now wraps:
  `out instanceof FastOutputStream ? (FastOutputStream) out : new FastOutputStream(out)`.
- **`BinaryRequestWriter.write(SolrRequest, FastOutputStream)`** — this javabin override was
  missing, so streamed bodies (both HTTP/1 `ConcurrentUpdateSolrClient` and HTTP/2
  `Http2SolrClient.send` at ~line 458) fell through to the base `RequestWriter` which emits
  XML while `getUpdateContentType()` advertises javabin → content/type mismatch → server 500.
  Added the override to marshal javabin + flush.
- **`RequestHandlerBase.handleRequest` metrics** — the per-request `requestTimes` /
  `distribRequestTimes` / `localRequestTimes` timers and `totalTime` counters were commented
  out, so `UPDATE./update.requestTimes:count` (and all request-handler timers) stayed 0.
  Restored `requestTimes.time()` + `timer.stop()` + `totalTime.inc(elapsed)`. Unblocked
  `CloudHttp2SolrClientRetryTest` / `CloudSolrClientRetryTest`.
- **FIXED — `ConcurrentUpdateHttp2SolrClient` streaming truncation** — the old HTTP/2 CUSC posted
  bodies via Jetty `OutputStreamContentProvider` (chunked, no Content-Length); the connection tore
  down mid-stream ("IllegalStateException: STOPPING"), truncating the body → server
  `SolrException("parsing error", EOFException)` → 500 → swallowed. **Resolved:** `OutStream` now
  marshals the whole batch into a pooled Agrona buffer and sends it atomically as a
  `ByteBufferRequestContent` with an explicit `CONTENT_LENGTH` on `OutStream.close()`
  (`Http2SolrClient.java` ~432-470); the `Runner` blocks on the response listener after close
  (`ConcurrentUpdateHttp2SolrClient.sendUpdateStream` ~256-262). No async producer thread races the
  connection any more, so there is no truncation. Verified GREEN: `ConcurrentUpdateHttp2SolrClientMultiCollectionTest`,
  `ConcurrentUpdateHttp2SolrClientBadInputTest`, `SolrExampleStreamingHttp2Test` (41),
  `SolrExampleStreamingBinaryHttp2Test` (42), all `failures=0 errors=0`. Added an
  `.onComplete(freeBuffer)` backstop on the `OutStream` request (matches the canonical
  `createRequest()` path; `freeBuffer()` is idempotent) so the pooled request buffer is released even
  if `responseListener.get()` times out before the response `InputStream` is obtained — previously a
  buffer-pool leak on that error path.

### Test infrastructure

- **`ClusterStateMockUtil`** — after `new Replica(...)`, calls
  `replica.setState(new AtomicInteger(Replica.State.getShortState(state)))` because the fork's
  Replica ctor strips STATE_PROP; also uses `CompositeIdRouter.DEFAULT` singleton.
- **`MockZkStateReader`** — overrides `getLeaderRetry` to return from mock cluster state.
- **`SolrTestCase.random()`** — do NOT restore `RandomizedContext.current().getRandom()`.
  Doing so fixes `testUsingConsistentRandomization` but regresses active tests
  `TestDocTermOrds.testRandom` + `testRandomWithPrefix` (they depend on fresh-per-call
  `Random`). Leave as is; `testUsingConsistentRandomization` stays `@Ignore`'d.
- **`SimpleFacets` points-field mincount<=0 warning** — make thread-safe via
  `synchronized(responseHeader)` + `Collections.synchronizedList` for the addition.

---

## Recurring patterns and anti-patterns

### Things that always need to change together when touching replica code

1. **State reads** — always `replica.getState()` (never `replica.getStr(STATE_PROP)`).
2. **Core name** — always `replica.getName()` (never `replica.getStr(CORE_NAME_PROP)`).
3. **Leader detection** — `slice.getLeader(liveNodes)` uses `getRawState()==LEADER`;
   active/recovery checks use `isActive()` which uses `getState()` (published=true) and
   correctly treats LEADER as ACTIVE.

### The `cc.getZkController()` null trap

In minimal test setups (e.g. `TestLeaderElectionZkExpiry`), the `CoreContainer` has no
`ZkController` registered on it (`cc.getZkController()` returns null). Production code
inside the Overseer that needs the ZkController must use `overseer.getZkController()`, which
is always set. Files already fixed: `ZkStateWriter`, `QueueWatcher`,
`CollectionWorkQueueWatcher`, `OverseerCollectionMessageHandler`,
`OverseerConfigSetMessageHandler`. Watch for this pattern in new Overseer-related code.

### `fastutil` list casts

The fork's JavaBin codec deserializes lists as `it.unimi.dsi.fastutil.objects.ObjectArrayList`.
Any test or code that casts a deserialized response list to `java.util.ArrayList` will get a
`ClassCastException`. Always cast to the `List` interface. This is the same fix needed in:
`CustomHighlightComponentTest`, `TestReplicationHandler.assertVersions`, and anywhere that
calls `entries.get("someListField")` and casts the result.

### HTTP/1.1 vs HTTP/2

Any test or production code using Apache `HttpClient`, `HttpSolrClient`, or `URL.openStream()`
against the fork's Jetty server will fail. Migrate to `Http2SolrClient.Builder(baseUrl).build()`.
Tests that did this and still failed: see `solr-ref-test-enablement.md` for the full list.

### DisjunctionMaxQuery ordering (edismax)

Lucene 9.0.0 `DisjunctionMaxQuery` uses an unordered `Multiset` for disjuncts. Any test
that asserts an exact `toString()` containing `|` disjunct order will fail seed-dependently
(~14/33 tests fail per run on varying seeds). There is no Solr-side fix. Tests in
`TestExtendedDismaxParser` that assert exact query strings remain `@Ignore`'d. Do not
re-investigate.

### Debugging inside MiniSolrCloudCluster

- `System.out`/`System.err` is swallowed by embedded nodes. Use `log.error(...)`.
- Per-node logs land in `solr/core/build/test-results/test/outputs/OUTPUT-*.txt`.
- For distributed test failures, query each replica directly with `distrib=false` to
  identify which replica has wrong data before debugging the forwarding path.
- `DistributedUpdateProcessor` failure modes are SILENT — if a leader→replica forward
  fails, the client gets success (the add was applied locally). Only the replica diverges.

### ZkStateWriter merge path

Two common bugs in `enqueueStructureChange` that have already been fixed but are easy to
re-introduce:
1. Seeding `newDocProps` from `currentCollection` instead of incoming `docCollection` →
   MODIFYCOLLECTION becomes a no-op.
2. Dropping tombstones (`"remove": true` replicas) only inside the non-remove branch →
   deletion is order-dependent and deleted replicas can linger.

The correct pattern: two passes — carry over non-remove replicas, drop remove-flagged ones.

---

## Unresolved / open production leads

These are diagnosed root causes that have NOT been fixed yet:

- **`ShortClassNames.properties` stale short-names** — `solr.ClassicTokenizerFactory` /
  `solr.ClassicFilterFactory` still map to Lucene 8 FQCNs; they moved to
  `org.apache.lucene.analysis.classic.*` in Lucene 9.0.0.
- **`SolrCore.initIndex` empty-dir cloud startup** — only creates an empty index when
  `!coreContainer.isZooKeeperAware() || newCore`. A restarting cloud core with an empty/lost
  index dir skips creation → `IndexNotFoundException`. High blast radius; fix carefully.
- **`MigrateCmd` stale clusterState** — the StateUpdates overseer hands MigrateCmd a
  momentarily-stale cluster state snapshot; freshly-created collections may not be visible.
- **PKI inter-node principal propagation** — `PKIAuthenticationPlugin` does not set the
  signed principal on inter-node shard sub-requests.
- **JMX / MBeans not wired** — fork's JMX reporter is not wired to the metrics registry;
  per-core SolrIndexSearcher MBeans are not registered. (Note: request-handler timer/counter
  metrics ARE now recorded — see the `RequestHandlerBase.handleRequest` fix above.)
- **`TestTlogReplica` multi-failure cluster** — see above; `SolrCore.initIndex` cloud
  path + StateUpdates two-leader state + tlog-replica recovery.
- **SOLR-9504 empty-replica leadership** — see detailed section above.
- **`ShardTerms.setTermEqualsToLeader` at `ShardTerms.java:~199`** — calls `registerTerm`
  but discards its return value (a latent bug; one-line fix but not yet applied).

---

## What "adapted" means for old-Overseer tests

Several tests existed to verify queue-based Overseer behavior that no longer exists.
The correct disposition:

| What the test tested | Disposition |
|---|---|
| Queue skip-counts (overseer_operations leader/state requests) | DELETE |
| ADDREPLICAPROP / DELETEREPLICAPROP / REBALANCELEADERS | DELETE (feature unimplemented) |
| overseer_operations / update_state stats | ADAPT: drop those assertions, keep collection_operations |
| Overseer election / leader registration | ADAPT to the ephemeral-child-node model |
| Collection create/modify/delete/split | Usually still valid — adapt STATE_PROP reads |


<!-- headroom:rtk-instructions -->
# RTK (Rust Token Killer) - Token-Optimized Commands

When running shell commands, **always prefix with `rtk`**. This reduces context
usage by 60-90% with zero behavior change. If rtk has no filter for a command,
it passes through unchanged — so it is always safe to use.

## Key Commands
```bash
# Git (59-80% savings)
rtk git status          rtk git diff            rtk git log

# Files & Search (60-75% savings)
rtk ls <path>           rtk read <file>         rtk grep <pattern>
rtk find <pattern>      rtk diff <file>

# Test (90-99% savings) — shows failures only
rtk pytest tests/       rtk cargo test          rtk test <cmd>

# Build & Lint (80-90% savings) — shows errors only
rtk tsc                 rtk lint                rtk cargo build
rtk prettier --check    rtk mypy                rtk ruff check

# Analysis (70-90% savings)
rtk err <cmd>           rtk log <file>          rtk json <file>
rtk summary <cmd>       rtk deps                rtk env

# GitHub (26-87% savings)
rtk gh pr view <n>      rtk gh run list         rtk gh issue list

# Infrastructure (85% savings)
rtk docker ps           rtk kubectl get         rtk docker logs <c>

# Package managers (70-90% savings)
rtk pip list            rtk pnpm install        rtk npm run <script>
```

## Rules
- In command chains, prefix each segment: `rtk git add . && rtk git commit -m "msg"`
- For debugging, use raw command without rtk prefix
- `rtk proxy <cmd>` runs command without filtering but tracks usage
<!-- /headroom:rtk-instructions -->
