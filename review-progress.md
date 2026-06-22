# review.md remediation progress

Goal: address every finding in review.md, or deem it incorrect/not-applicable. Test-first where sensible.

Legend: ✅ done (in working tree) · 🔧 in progress · ⬜ open · ❎ deemed incorrect/NA

## Part A
- ✅ A1 TransactionLog ADD torn read
- ✅ A2 ZkStateWriter slice-state NPE guard (== H3)
- ✅ A3 ReplicaMutator null collection
- ❎ A4 ReplicaMutator CORE_NAME_PROP — NOT a live bug: setState/updateState are dead code (no live callers; only commented SliceMutator lines + a TestInjection log string). State flows StatePublisher→ZkStateWriter directly. Also state messages DO carry CORE_NAME_PROP (StatePublisher sets it); the stripped invariant is Replica-only. Defensive guards already added by A3/H7.
- ✅ A5 Replica.equals null-id NPE
- ✅ A6 Http2 response buffer pooling
- ✅ A7 ZkStateWriter dirtyStructure/collLock
- ✅ A8 LeaderElector await
- ✅ A9 ShardLeader singleton leak
- ✅ A10 Replica.write shared-map snapshot

## Part B
- ✅ B1 AuditLoggerPlugin close 30s bound (Wave1 audit)
- ✅ B2 RecoveryStrategy commit-swallow → fall back
- ✅ B3 SolrCloudTestCase commit-nudge removed
- ✅ B4 SolrZooKeeper fast-close ordering + SolrInternalHttpClient warn
- ✅ B5 beasting.gradle truncation + regex

## Part D
- ✅ D1-3/4/5/6 SolrCmdDistributor + DistributedZkUpdateProcessor (Wave2 cmddist) — compile-verified
- ✅ D2-1 DOWNNODE publish + D2-2..D2-6 ZkController (Wave1 zkcontroller)
- ✅ D3-1 StatePublisher.closed flag + D3-2..D3-5 (Wave1 statepublisher)
- ✅ D4-1 PeerSyncWithLeader null add + D4-2 UpdateLog ACTIVE-before-copy + D4-3..D4-7, F3-3/4 (Wave1 updatelog)
- ✅ D5-1 coreRefCount + D5-2..D5-5 ZkStateReader(+Queue) (Wave2 zkstatereader) — compile-verified (fixed type: getKeeper()→ZooKeeper)
- ✅ D6-1 upload limits (HttpSolrCall) + D6-3 BinaryResponseWriter (Wave2 binresp); D6-2 multipart temp leak (Wave1 reqparsers)

## Part E
- ✅ E1-1 SolrCore doClose + E1-2 DirectUpdateHandler2 (Wave2 duh2) + E1-3/4 (Wave1 solrcore)
- ✅ E2-1 CoreContainer executor + E2-2..E2-5 (Wave1 corecontainer)
- ✅ E3-2 REMOTEQUERY auth + E3-3 async use-after-free + E3-4..E3-6 (Wave1 httpsolrcall); E3-1 baseline
- ✅ E4-1 condenseResponse  E4-2 unbox NPE  E4-3 LB precedence (baseline)
- ✅ E4-4 ZkShardTerms saveTerms (Wave1 shardterms); E4-5/E4-6 BaseCloudSolrClient + E4-7 LBSolrClient (Wave2 cloudclient)
- ✅ E5-1 term-notify (Wave1 shardterms); E5-2/E5-3/E5-4 RecoveryStrategy (Wave2 recovery)
- ✅ E6-1 REBALANCELEADERS (NA/slated-remove) + E6-3/E6-4/E6-5 OverseerCollectionMessageHandler/Assign (Wave2 overseercmd); E6-2 CreateCollectionCmd (Wave1 createcoll)

## Part F
- ✅ F1-1..F1-7 ParWork/ParWorkExecutor (Wave1 parwork)
- ✅ F2-1..F2-5 DefaultSolrCoreState (Wave1 corestate)
- ✅ F3-1/F3-2 baseline (VersionBucket/RealTimeGet); F3-3/F3-4 (Wave1 updatelog)
- ✅ F4-1..F4-5 IndexFetcher/ReplicationHandler (Wave1 indexfetcher)
- ✅ F5-1..F5-4 HttpShardHandler/Factory/SearchHandler (Wave1 shardhandler)
- ✅ F6-1 CommitTracker + F6-4 CaffeineCache (Wave1 f6misc); F6-2/F6-3 baseline

## Part G
- ✅ G1-1/G1-2 baseline; G1-3 ZkCmdExecutor / G1-4 SolrZkClient / G1-5 ConnectionManager (Wave2 zkconn)
- ✅ G2-1 StateUpdates hashCode + G2-2/G2-3/G2-4 (Wave1 valeq)
- ❎ G3 SolrIndexSearcher — verified clean (no action)
- ✅ G4-1/G4-2/G4-3 Javabin/XML/JsonLoader (Wave2 loaders)
- ✅ G5-1 CachingDirectoryFactory refcount (Wave1 dirfactory)
- ✅ G6-1 recoveryExecutor + G6-3 PKI null key + G6-4 replay window (Wave1 g6recovery); G6-2 baseline

## Part H
- ✅ H1 ZkStateWriter demotion deferred  H4 dirtyStructure reschedule  H6 subshard null  H7 ReplicaMutator slice null  H8 CollectionMutator shared slice  H9 finish null-req buildMsg  H10 collIds scope
- ✅ Minor: Replica.equals/Slice.equals/getId/shortStateToState/Slice.getLeader (Wave1 valeq — Replica.java/Slice.java)

## Never-re-reviewed (budget cap) — re-review when reached
- ✅ tlog/JavaBinCodec re-pass (TransactionLog Wave1; cross-checked against memory file); UpdateLog/PeerSync (Wave1 updatelog); SolrCore (Wave1 solrcore); DUH2 (Wave2 duh2)

## STATUS: all review.md findings dispositioned (Wave1 18 + Wave2 9 + Wave3 4 agents). Compile GREEN (core+solrj+test-framework, GRADLE_EXIT=0). New regression test: ReplicaTest.java. One compile fix applied centrally (ZkStateReader getKeeper()→ZooKeeper type).

## WAVE 3 (cross-file residuals flagged by Wave1 agents as out-of-their-file) — file-disjoint, compile GREEN (GRADLE_EXIT=0)
- ✅ D3-5(a) FIXED OverseerTaskProcessor.java:218 — `LeaderElector.sortSeqs(children)` before `children.get(0)` in getLeaderId (handoff returns lowest-seq, not stale)
- ✅ D3-5(b) FIXED OverseerTaskProcessor.java:346,369 — `runningZKTasks.remove(id)` added to markTaskComplete + resetTaskWithException (real grow-forever leak)
- ✅ G2-2 ALREADY-FIXED DocCollection.java — copy already produces fresh StateUpdates
- ✅ G2-3 FIXED StatePlaneReader.java — carryForwardStateUpdates no longer aliases prior StateUpdates
- ✅ F3-3 FIXED RealTimeGetComponent.java:597-604 — clear versionReturned (0L) on UPDATE_INPLACE→resolve-null→DELETED path (no live version for deleted doc)
- ✅ F3-4 FIXED RealTimeGetComponent.java:1298-1305 — `rangeBounds.length < 2` guard prevents AIOOBE on malformed range token; ❎ visibility-check sub-part DEEMED-INCORRECT (peersync-from-tlog/recentUpdates is intentional, matches upstream)
- ❎ E3-3 NA-LATENT SolrCall.java:464 — solr.asyncIO/asyncDispatchFilter default false, WriteListener path unreachable; WHY-comment added forbidding re-enable until lifecycle fixed
- ✅ E3-4 FIXED SolrDispatchFilter.java:657 — isCommitted() early-return guard at top of sendException (no double body write)
- ✅ E3-6(checkProps) FIXED SolrCall.java:699 — unbounded 150ms poll replaced with 30s deadline-bounded loop

## WAVE 1 (in flight, 18 agents) — file-disjoint
parwork(F1) solrcore(E1) corestate(F2) indexfetcher(F4) shardhandler(F5) dirfactory(G5-1)
zkcontroller(D2) statepublisher(D3) corecontainer(E2) shardterms(E4-4,E5-1) createcoll(E6-2)
updatelog(D4,F3-3/4) reqparsers(D6-1/2) httpsolrcall(E3-2..6) f6misc(F6-1,F6-4) audit(B1)
g6recovery(G6-1/3/4) valeq(H-minor,G2-1..4 incl Slice + DocCollection/StatePlaneReader)

## WAVE 2 (deferred — launch after Wave 1 quiesces + Wave 1 compiles clean) — files confirmed
- D1-3/4/5/6 → SolrCmdDistributor.java + DistributedZkUpdateProcessor.java
- D5-1(ZkStateReader:2249 coreRefCount)/D5-2(ZkStateReaderQueue)/D5-3/D5-4/D5-5 → ZkStateReader.java(+Queue)
- D6-3 → BinaryResponseWriter.java ;  D6-1 HttpSolrCall:143 Long.MAX_VALUE → AFTER httpsolrcall agent
- E1-2 → DirectUpdateHandler2.java
- E4-5/E4-6 → BaseCloudSolrClient.java ; E4-7 → LBSolrClient.java
- E5-2/E5-3/E5-4 → RecoveryStrategy.java
- E6-1(REBALANCELEADERS slated-remove)/E6-3/E6-4/E6-5 → OverseerCollectionMessageHandler.java + Assign.java
- G1-3/G1-4/G1-5 → ZkCmdExecutor.java + ConnectionManager.java + SolrZkClient.java(async rc)
- G4-1/G4-2/G4-3 → JavabinLoader.java + XMLLoader.java + JsonLoader.java
- (G2-2/3/4 already covered by valeq)
- D5-1 cross-file note: SolrCore/CoreContainer also have refCount but D5-1 is ZkStateReader-only.
</content>
</invoke>
