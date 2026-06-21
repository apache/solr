# Ignored-Test Triage — solr-ref (branch `rip`)

Scan of every `@Ignore` annotation under `solr/*/src/test` and `solr/contrib/*/src/test`. Each test is placed in exactly one root-cause family with a recommendation; the full per-test list follows each family.

**Total ignored: 349** — 113 class-level (`C`), 236 method-level (`m`). Modules: core 260, solrj 71, contrib/ltr 17, contrib/extraction 1.

> **Standing directive:** enable ignored tests one by one and fix failures at the **root cause**. Flaky == deterministic. Run `@Nightly` under `-Dtests.nightly=true`. Verify by reading per-class `TEST-*.xml`, never via `tail` (it hides the failure summary → false pass).

> **Already fixed & enabled this effort (not in the list below):** `SolrRequestParsers` wt-vs-body content-type, `JettySolrRunner` HTTP/1.1+h2c cleartext connector, `TestICUCollationField` Lucene-9 loader ctor, `CustomHighlightComponentTest` fastutil `List` casts — plus 9 tests un-ignored & green.

## Summary — families by priority

| # | Family | Count | Priority | One-line recommendation |
|---|--------|------:|:--------:|-------------------------|
| 1 | Debug / `nocommit` leftovers — REMOVE BEFORE RELEASE | 10 | P0 | Remove markers, root-cause — must not ship |
| 2 | Undocumented bare `@Ignore` (NO comment) — TRIAGE NEEDED | 52 | P1 | Run & triage; document or fix each |
| 3 | Flaky / race / non-convergence | 25 | P1 | Root-cause the race; awaitility not sleeps |
| 4 | `@BeforeClass` / setup NPEs (suite cannot start) | 9 | P1 | Fix the @BeforeClass NPE — re-enables whole class |
| 5 | Replica / core-name model divergence | 2 | P1 | Make Replica expose core name (fix model) |
| 6 | Assertion / behavior divergence (fork behaves differently) | 25 | P2 | Confirm intended contract, then fix product or assertion |
| 7 | Schema / configset / field-type loading | 11 | P2 | Fix configset/field-type loading (shared causes) |
| 8 | StateUpdates Overseer feature gaps | 7 | P2 | Implement or re-model StateUpdates Overseer ops |
| 9 | @Nightly-only failures | 6 | P2 | Fix under nightly; one LTR cause unblocks many |
| 10 | WireMock harness needs updating | 2 | P2 | Update mock host wiring |
| 11 | Legacy base-class migration (`AbstractFullDistribZkTestBase` → bridge) | 19 | P2★ | Finish bridge base class, un-ignore batch together |
| 12 | Other documented MRM TODO (per-test root cause) | 171 | P3 | Per-test root cause from recorded comment |
| 13 | Inherited from upstream Solr (pre-existing) | 5 | P4 | Inherited upstream; lowest priority |
| 14 | Environment-specific / by-design not-applicable | 3 | P4 | Convert to documented permanent skip |
| 15 | Abstract base classes (correctly ignored) | 2 | — | Leave as-is (correct usage) |

*Priority key: P0 must-fix-before-release · P1 high info/value · P2 scoped fixes · P2★ one big shared effort · P3 case-by-case · P4 low/inherited · — no action.*

## Debug / `nocommit` leftovers — REMOVE BEFORE RELEASE — 10

**Recommendation:** Accidentally committed debugging state (`nocommit`, `TMP TESTING`, `debug`, `TJP`, `MRM DEBUG`). Not real deferrals and no documented reason. **Action (highest priority):** re-enable each, reproduce, root-cause, delete the marker. A `nocommit` must never reach a release branch.

> **STATUS — RESOLVED (this session).** All 10 undocumented markers removed. **4 fixed & passing:** `testCreateAndDeleteCollection` (restored from a mangled 36×36-core debug state to a real 2×2 create/delete test), `testCreateCollWithDefaultClusterPropertiesNewFormat` (nightly), `searchingShouldFailWithoutTolerantSearchSetToTrue`, `TestManagedSynonymGraphFilterFactory`. **1 cleaned + repaired:** `testCreateDelete` — marker removed and the missing `@Test` added; now correctly gated by a *separate, documented* class-level `@Ignore` (StateUpdates tlog-replica, out of P0 scope). **5 reproduced genuine deeper bugs → re-`@Ignore`'d with precise root-cause comments** (keeps the build green; a bare `nocommit` must not ship): `testSharedSchema` (managed-schema defeats the shareSchema cache key), `testCp` (ZkCpTool trailing-slash dest stores no node data → NPE), `testBuffering` + `testLogReplayWithInPlaceUpdatesAndDeletes` (custom mmap TransactionLog buffering/replay), `testRouting` (class `@Before` HttpClusterStateProvider init fails — fork HTTP/2 cluster-state). Verified `BUILD SUCCESSFUL` (core+solrj). Nothing committed.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `testCreateCollWithDefaultClusterPropertiesNewFormat()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:166` | nocommit |
| `testCreateAndDeleteCollection()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:238` | TMP TESTING |
| `testSharedSchema()` | m | `core/src/test/org/apache/solr/cloud/ConfigSetsAPITest.java:71` | MRM |
| `testCp()` | m | `core/src/test/org/apache/solr/cloud/SolrCLIZkUtilsTest.java:160` | debug |
| `searchingShouldFailWithoutTolerantSearchSetToTrue()` | m | `core/src/test/org/apache/solr/cloud/TestDownShardTolerantSearch.java:63` | need to debug |
| `testCreateDelete()` | m | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:149` | nocomit debug |
| `TestManagedSynonymGraphFilterFactory` | C | `core/src/test/org/apache/solr/rest/schema/analysis/TestManagedSynonymGraphFilterFactory.java:41` | MRM DEBUG, recently exposed issues with new parallelism |
| `testBuffering()` | m | `core/src/test/org/apache/solr/search/TestRecovery.java:549` | nocommit |
| `testLogReplayWithInPlaceUpdatesAndDeletes()` | m | `core/src/test/org/apache/solr/search/TestRecovery.java:1534` | nocommit |
| `testRouting()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:248` | TJP |

## Undocumented bare `@Ignore` (NO comment) — TRIAGE NEEDED — 52

**Recommendation:** No rationale recorded. **Action: triage batch, highest information-gain.** Run each, capture the failure, then route into a family below — fix at root cause, or re-ignore *with a precise comment*. An undocumented `@Ignore` is debt whether or not it currently fails.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `test()` | m | `core/src/test/org/apache/solr/cloud/ForceLeaderTest.java:68` | — |
| `doTestNumRequests()` | m | `core/src/test/org/apache/solr/cloud/ShardRoutingTest.java:223` | — |
| `testKillLeader()` | m | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:318` | — |
| `testKillTlogReplica()` | m | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:442` | — |
| `testBasicLeaderElection()` | m | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:661` | — |
| `testOutOfOrderDBQWithInPlaceUpdates()` | m | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:706` | — |
| `deleteCollectionOnlyInZk()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIDistClusterPerZkTest.java:115` | — |
| `testDeleteNonExistentCollection()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIDistributedZkTest.java:290` | — |
| `testCoreInitFailuresOnReload()` | m | `core/src/test/org/apache/solr/core/TestCoreContainer.java:485` | — |
| `testExistsEquivilence()` | m | `core/src/test/org/apache/solr/core/TestDirectoryFactory.java:53` | — |
| `testAllInfoPresent()` | m | `core/src/test/org/apache/solr/core/TestSolrXml.java:64` | — |
| `proxySystemInfoHandlerAllNodes()` | m | `core/src/test/org/apache/solr/handler/admin/AdminHandlersProxyTest.java:76` | — |
| `proxyMetricsHandlerAllNodes()` | m | `core/src/test/org/apache/solr/handler/admin/AdminHandlersProxyTest.java:90` | — |
| `proxySystemInfoHandlerNonExistingNode()` | m | `core/src/test/org/apache/solr/handler/admin/AdminHandlersProxyTest.java:107` | — |
| `proxySystemInfoHandlerOneNode()` | m | `core/src/test/org/apache/solr/handler/admin/AdminHandlersProxyTest.java:115` | — |
| `testVeryLongWord()` | m | `core/src/test/org/apache/solr/handler/tagger/Tagger2Test.java:77` | — |
| `testPerformance()` | m | `core/src/test/org/apache/solr/schema/CurrencyFieldTypeTest.java:288` | — |
| `testIntPointFieldSortAndFunction()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:141` | — |
| `testIntPointFieldFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:212` | — |
| `testIntPointStats()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:286` | — |
| `testIntPointFieldMultiValuedFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:341` | — |
| `testIntPointSetQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:521` | — |
| `testDoublePointFieldSortAndFunction()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:568` | — |
| `testDoublePointFieldFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:633` | — |
| `testDoublePointStats()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:710` | — |
| `testDoublePointFieldMultiValuedFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:757` | — |
| `testDoublePointSetQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:934` | — |
| `testFloatPointFieldSortAndFunction()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:988` | — |
| `testFloatPointFieldFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1054` | — |
| `testFloatPointStats()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1133` | — |
| `testFloatPointFieldMultiValuedFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1270` | — |
| `testFloatPointSetQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1305` | — |
| `testLongPointFieldNonSearchableExactQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1334` | — |
| `testLongPointFieldSortAndFunction()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1366` | — |
| `testLongPointFieldFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1436` | — |
| `testLongPointStats()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1515` | — |
| `testLongPointFieldMultiValuedFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1560` | — |
| `testDatePointFieldSortAndFunction()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1677` | — |
| `testDatePointFieldFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1742` | — |
| `testDatePointStats()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1891` | — |
| `testDatePointFieldMultiValuedExactQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1899` | — |
| `testDatePointFieldMultiValuedNonSearchableExactQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1907` | — |
| `testDatePointFieldMultiValuedFacetField()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:1923` | — |
| `testDatePointMultiValuedFunctionQuery()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:2023` | — |
| `testInternals()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:2036` | — |
| `testWhiteboxCreateFields()` | m | `core/src/test/org/apache/solr/schema/TestPointFields2.java:3657` | — |
| `singleShardedPreferenceRules()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:511` | — |
| `GraphExpressionTest` | C | `solrj/src/test/org/apache/solr/client/solrj/io/graph/GraphExpressionTest.java:77` | — |
| `testExceptionStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamingTest.java:607` | — |
| `testParallelExceptionStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamingTest.java:653` | — |
| `testReadMapEntryTextStreamSource()` | m | `solrj/src/test/org/apache/solr/common/util/TestJavaBinCodec.java:444` | — |
| `testLargeString()` | m | `solrj/src/test/org/apache/solr/common/util/Utf8CharSequenceTest.java:31` | — |

## Flaky / race / non-convergence — 25

**Recommendation:** Intermittent failures, leaks, or loops that never converge in a timeout. **Action: root-cause the race — do NOT `@BadApple` or just retry.** Replace fixed sleeps/timeouts with awaitility polling on the real condition; fix the leak/non-convergence. Treat as deterministic (standing directive).

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `featureExtraction_valueFeatureExplicitlyNotRequired_shouldNotScoreFeature()` | m | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestExternalFeatures.java:163` | too flakey |
| `TestManagedFeatureStore` | C | `contrib/ltr/src/test/org/apache/solr/ltr/store/rest/TestManagedFeatureStore.java:34` | MRM TODO: flakey |
| `testOverseerStatus()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:849` | MRM TODO: - have to fix that race |
| `deleteReplicaOnIndexing()` | m | `core/src/test/org/apache/solr/cloud/DeleteReplicaTest.java:409` | MRM TODO: deleting the non-leader replica during concurrent indexing never converges to getReplicas().size()==1 within 20s — the deleted replica lingers in cluster state (often … |
| `MissingSegmentRecoveryTest` | C | `core/src/test/org/apache/solr/cloud/MissingSegmentRecoveryTest.java:48` | MRM TODO: load-flaky — testLeaderRecovery passes in isolation but its 10s "wait for active collection" times out under heavy parallel test contention (leader recovery doesn't co… |
| `testMalformedDBQViaShard1NonLeaderClient()` | m | `core/src/test/org/apache/solr/cloud/TestCloudDeleteByQuery.java:243` | TEST-MRM TODO: flakey - are we returning the error right for a forward to leader? |
| `testMalformedDBQViaShard2NonLeaderClient()` | m | `core/src/test/org/apache/solr/cloud/TestCloudDeleteByQuery.java:248` | TEST-MRM TODO: flakey |
| `testMalformedDBQViaNoCollectionClient()` | m | `core/src/test/org/apache/solr/cloud/TestCloudDeleteByQuery.java:253` | TEST-MRM TODO: flakey |
| `testUploadWithLibDirective()` | m | `core/src/test/org/apache/solr/cloud/TestConfigSetsAPI.java:365` | MRM-TEST TODO: flakey |
| `testKillLeader()` | m | `core/src/test/org/apache/solr/cloud/TestPullReplica.java:308` | MRM TODO test still flakey |
| `testPullReplicaStates()` | m | `core/src/test/org/apache/solr/cloud/TestPullReplica.java:313` | ("Ignore until I figure out a way to reliably record state transitions") |
| `TestSolrCloudWithDelegationTokens` | C | `core/src/test/org/apache/solr/cloud/TestSolrCloudWithDelegationTokens.java:63` | MRM TODO: delegation-token get/cancel flows fail under the fork's reworked auth/HTTP path (getDelegationToken/cancelDelegationToken). Original note also flagged this as leak-pro… |
| `testReadOnlyCollection()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIDistributedZkTest.java:158` | MRM TODO: - look at, prob some race, parallel commit |
| `testWTParam()` | m | `core/src/test/org/apache/solr/handler/V2ApiIntegrationTest.java:118` | MRM-TEST TODO: flakey, can fail with 404 |
| `testEstimator()` | m | `core/src/test/org/apache/solr/handler/admin/IndexSizeEstimatorTest.java:98` | .;llthere is some race here - the fieldsBySize can come back empty rarely |
| `StatsReloadRaceTest` | C | `core/src/test/org/apache/solr/handler/admin/StatsReloadRaceTest.java:35` | MRM TODO: NPE in requestMetrics() — the fork's metrics registry/handler returns null for the looked-up metric during concurrent core reload, so the race test's metric assertions… |
| `testVariableFragsize()` | m | `core/src/test/org/apache/solr/highlight/HighlighterTest.java:636` | MRM flakey test |
| `testManagedSynonyms()` | m | `core/src/test/org/apache/solr/rest/schema/analysis/TestManagedSynonymGraphFilterFactory.java:74` | MRM TODO: race ? |
| `SolrExampleStreamingHttp2Test` | C | `solrj/src/test/org/apache/solr/client/solrj/embedded/SolrExampleStreamingHttp2Test.java:35` | MRM TODO: ConcurrentUpdateHttp2SolrClient streaming path — batched adds aren't reliably flushed/committed under the fork, so doc counts come back 0 (testCommitWithinOnAdd, testF… |
| `testGetRawStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/BasicHttpSolrClientTest.java:264` | MRM TODO: flakey |
| `testNonRetryableRequests()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:569` | MRM TODO: flakey it seems |
| `testCaching()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientCacheTest.java:56` | MRM TODO: flakey or counts on more than 1 retry? |
| `testAliasHandling()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:199` | flakey test, leaks, seems to fail collection create |
| `testPing()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:1092` | flakey test? MRM TODO: |
| `testBasicTextLogitStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamExpressionTest.java:2955` | MRM TODO: catching some flakey issue ... |

## `@BeforeClass` / setup NPEs (suite cannot start) — 9

**Recommendation:** The whole suite aborts in setup with an NPE (schema bulk-update, static test-base state, etc.), so none of its tests run. **Action: fix the NPE'ing init path first** — high leverage since one fix re-enables every test in the class. Start from the recorded line; many are shared base-class init regressions.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `NestedShardedAtomicUpdateTest` | C | `core/src/test/org/apache/solr/cloud/NestedShardedAtomicUpdateTest.java:39` | MRM TODO: doNestedInplaceUpdateTest NPEs at line 163 — nested/in-place atomic updates across shards under the fork (in-place update path has known divergences). See [[solr-ref-t… |
| `TestPullReplicaWithAuth` | C | `core/src/test/org/apache/solr/cloud/TestPullReplicaWithAuth.java:57` | MRM TODO: NPE combining pull replicas with the auth framework — same family as TestAuthenticationFramework (auth plugin wiring) + TestPullReplicaErrorHandling (pull-replica reco… |
| `TestQueryingOnDownCollection` | C | `core/src/test/org/apache/solr/cloud/TestQueryingOnDownCollection.java:44` | MRM TODO: querying a collection whose replicas are all DOWN — the distributed query gets an empty/non-javabin shard response that fails JavaBin parse (NPE in JavaBinCodec._init)… |
| `ZkFailoverTest` | C | `core/src/test/org/apache/solr/cloud/ZkFailoverTest.java:36` | MRM TODO: testRestartZkWhenClusterDown NPEs at line 63 — the fork's ZK-failover / cluster-down recovery path doesn't behave as the test expects after restarting ZK while the clu… |
| `BlobRepositoryCloudTest` | C | `core/src/test/org/apache/solr/core/BlobRepositoryCloudTest.java:42` | MRM TODO: NPE exercising the blob repository (.system blob store) under the fork — the blob/.system collection plumbing diverged. Deep .system/blob-store divergence. |
| `TestLazyCores` | C | `core/src/test/org/apache/solr/core/TestLazyCores.java:56` | MRM TODO: init() NPEs (line ~101) for the lazy/transient core scenarios — the fork's reworked transient-core loading doesn't set up the lazy core the way these tests expect. Sam… |
| `SolrJmxReporterTest` | C | `core/src/test/org/apache/solr/metrics/reporters/SolrJmxReporterTest.java:51` | MRM TODO: @Before init() NPEs in SolrTestUtil.getTestName via getSimpleClassName — the fork's test-name plumbing returns null at the point this reporter test queries it during s… |
| `TestUseDocValuesAsStored` | C | `core/src/test/org/apache/solr/schema/TestUseDocValuesAsStored.java:52` | MRM TODO: RuntimeException/NPE exercising docValues-as-stored retrieval under the fork — the useDocValuesAsStored field handling diverges (cf. TestUseDocValuesAsStored2, fixed s… |
| `HttpSolrCallGetCoreTest` | C | `core/src/test/org/apache/solr/servlet/HttpSolrCallGetCoreTest.java:39` | MRM TODO: assertCoreChosen NPEs in ServletUtils.getPathAfterContext — the fork's servlet path parsing doesn't handle this test's mock request path, so core selection can't be ex… |

## Replica / core-name model divergence — 2

**Recommendation:** The fork separates replica name from core name; deriving a core name from `Replica` (`getName()`/`CORE_NAME_PROP`) yields null → `Missing required parameter: core`. **Action:** make the fork's `Replica` expose the backing core name (populate `CORE_NAME_PROP`), or update tests to resolve it the new way. Fixing the model is preferable and likely unblocks several tests.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `testBuildClusterState_Simple()` | m | `core/src/test/org/apache/solr/cloud/ClusterStateMockUtilTest.java:37` | MRM TODO: asserts replica getName()=="slice1_replica1" and coreUrl ".../slice1_replica1/", but the fork separates replica name (coreNodeName, "replica1") from core name ("slice1… |
| `TestSolrCoreSnapshots` | C | `core/src/test/org/apache/solr/core/snapshots/TestSolrCoreSnapshots.java:63` | MRM TODO: the "Invalid Http response" half is now fixed (cleartext connector serves HTTP/1.1), but CREATESNAPSHOT still returns 400 "Missing required parameter: core" — the core… |

## Assertion / behavior divergence (fork behaves differently) — 25

**Recommendation:** Test reaches its assertions but the fork returns a different value/shape/status (counts, `expected:<x> but was:<y>`, `notfound` vs `failed`, etc.). **Action: decide intent per-test** — is the fork's new behavior correct (update the assertion) or a regression (fix the product)? Don't blindly relax assertions; confirm the intended contract first.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `testAsyncCreateCollectionCleanup()` | m | `core/src/test/org/apache/solr/cloud/CreateCollectionCleanupTest.java:112` | MRM TODO: comes back as notfound instead of failed |
| `OverriddenZkACLAndCredentialsProvidersTest` | C | `core/src/test/org/apache/solr/cloud/OverriddenZkACLAndCredentialsProvidersTest.java:45` | MRM TODO: fork's ZK ACL/credentials provider wiring + SolrZkClient lifecycle changed — the VM-params-based ACL/credentials provider tests fail (readonly/all/no-credentials asser… |
| `testLiveSplit()` | m | `core/src/test/org/apache/solr/cloud/SplitShardTest.java:282` | MRM TODO: live split while indexing loses documents (expected:<1209> but was:<974>) — the fork's shard-split-during-concurrent-indexing path drops buffered updates. The other Sp… |
| `TestExactStatsCacheCloud` | C | `core/src/test/org/apache/solr/cloud/TestExactStatsCacheCloud.java:27` | MRM TODO: distributed cloud score differs from single-node control (e.g. expected:<1.66> but was:<1.88>) — the fork's distributed term-stats (ExactStatsCache) doesn't reproduce … |
| `TestLeaderElectionWithEmptyReplica` | C | `core/src/test/org/apache/solr/cloud/TestLeaderElectionWithEmptyReplica.java:43` | MRM TODO: after recovery the indexed docs aren't visible ("expected:<10> but was:<0>" at line 96) — under the fork's reworked leader election a leader isn't established for the … |
| `TestMiniSolrCloudClusterSSL` | C | `core/src/test/org/apache/solr/cloud/TestMiniSolrCloudClusterSSL.java:66` | MRM TODO: SSL/clientAuth cluster collection creation fails (checkCreateCollection) — the fork's SSL wiring for MiniSolrCloudCluster (incl. invalid-peer-name handling) diverges. … |
| `TestPullReplicaErrorHandling` | C | `core/src/test/org/apache/solr/cloud/TestPullReplicaErrorHandling.java:59` | MRM TODO: pull-replica error handling — when a pull replica disconnects from ZK its count goes to 0 (expected:<1> but was:<0>, state {2=D}); the fork's StateUpdates recovery doe… |
| `TestSolrCloudWithKerberosAlt` | C | `core/src/test/org/apache/solr/cloud/TestSolrCloudWithKerberosAlt.java:40` | MRM TODO: KerberosPlugin.setup fails — the generated MiniKdc jaas-client.conf has no 'Client' section (LoginException), so the fork's Kerberos auth plugin can't initialize. Deep… |
| `TestStressCloudBlindAtomicUpdates` | C | `core/src/test/org/apache/solr/cloud/TestStressCloudBlindAtomicUpdates.java:68` | MRM TODO: teardown fails with AccessDeniedException "Can't open a file still open for writing" (e.g. _0.fdm) — the fork's blind-atomic-update stress path leaves index files/writ… |
| `TestTlogReplica` | C | `core/src/test/org/apache/solr/cloud/TestTlogReplica.java:78` | MRM TODO: tlog-replica recovery/visibility under the fork's StateUpdates model — replicas stay behind (e.g. "not up to date ... expected:<4> but was:<2>") or aren't counted (tlo… |
| `TestCollectionAPI` | C | `core/src/test/org/apache/solr/cloud/api/collections/TestCollectionAPI.java:60` | MRM TODO: collection-API behavior assertions fail at test() line 91 under the fork (replica properties / cluster-status shape diverged from ReplicaPropertiesBase expectations). |
| `TestLocalFSCloudBackupRestore` | C | `core/src/test/org/apache/solr/cloud/api/collections/TestLocalFSCloudBackupRestore.java:45` | MRM TODO: backup request status never reaches COMPLETED (expected:<COMPLETED> but was:<NOT_FOUND>) — the fork's async backup/restore status tracking doesn't record the request i… |
| `TestBadConfig` | C | `core/src/test/org/apache/solr/core/TestBadConfig.java:25` | MRM TODO: several bad-config cases no longer throw at core init ("Did not encounter any exception" from bad-solrconfig-missing-scriptfile / bogus-scriptengine-name / tolerant-up… |
| `TestDistribPackageStore` | C | `core/src/test/org/apache/solr/filestore/TestDistribPackageStore.java:64` | MRM TODO: package-store signature verification assertion fails (expected "Signature does not match") — the fork's distributed file/package store signing/verification messaging d… |
| `DaemonStreamApiTest` | C | `core/src/test/org/apache/solr/handler/admin/DaemonStreamApiTest.java:44` | MRM TODO: daemon streaming-expression API — getTuples fails (checkCmdsNoDaemon); the fork's /stream daemon command handling diverges. Deep streaming-expression divergence. |
| `IndexSizeEstimatorTest` | C | `core/src/test/org/apache/solr/handler/admin/IndexSizeEstimatorTest.java:61` | MRM TODO: idle timeout (15s) waiting on the cluster during index-size estimation — the fork's estimator/HTTP path stalls. Deep (and likely load-sensitive) divergence. |
| `SegmentsInfoRequestHandlerTest` | C | `core/src/test/org/apache/solr/handler/admin/SegmentsInfoRequestHandlerTest.java:37` | MRM TODO: @AfterClass asserts the IndexWriter refcount is unchanged across the suite, but the fork's reworked SolrCoreState/IndexWriter lifecycle nets extra refs (e.g. 27 vs 19)… |
| `CustomTermsComponentTest` | C | `core/src/test/org/apache/solr/handler/component/CustomTermsComponentTest.java:36` | MRM TODO: extends ShardsWhitelistTest and fails on the shards-whitelist 403 for an IPv6 shard (http://[::1]:4 not on whitelist) — same shards-whitelist divergence as its base wh… |
| `SolrSlf4jReporterTest` | C | `core/src/test/org/apache/solr/metrics/reporters/SolrSlf4jReporterTest.java:46` | MRM TODO: asserts "log-level at-least INFO" but the fork's test log4j2 config runs the root logger at WARN (INFO disabled), so the Slf4j reporter's logged-metrics assertion can'… |
| `TestPackages` | C | `core/src/test/org/apache/solr/pkg/TestPackages.java:87` | MRM-Test TODO: debug, can be slow but was working in isloation |
| `XCJFQueryTest` | C | `core/src/test/org/apache/solr/search/join/XCJFQueryTest.java:42` | MRM TODO: cross-collection join (XCJF) fails with 500 from the /export handler during the join's streaming sub-request — the fork's export/streaming path errors under the join. … |
| `PKIAuthenticationIntegrationTest` | C | `core/src/test/org/apache/solr/security/PKIAuthenticationIntegrationTest.java:43` | MRM TODO: the HTTP-protocol blocker is now fixed (cleartext connector serves HTTP/1.1), so the test runs, but it fails on a distinct deeper issue: PKI inter-node principal propa… |
| `SolrExampleStreamingBinaryHttp2Test` | C | `solrj/src/test/org/apache/solr/client/solrj/embedded/SolrExampleStreamingBinaryHttp2Test.java:41` | MRM TODO: same ConcurrentUpdateHttp2SolrClient streaming-flush/commit divergence as the parent SolrExampleStreamingHttp2Test (doc counts come back 0). @Ignore is not inherited, … |
| `testWrongZkChrootTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:805` | MRM TODO: ~ getting a NoNodeException instead of the expected SolrException |
| `testStringCaching()` | m | `solrj/src/test/org/apache/solr/common/util/TestJavaBinCodec.java:546` | MRM TODO: asserts cross-codec-instance String interning (l1.get(0) == l2.get(0)) after two separate `new JavaBinCodec()` instances unmarshal, but a bare codec has no shared Stri… |

## Schema / configset / field-type loading — 11

**Recommendation:** Fail at schema/configset resolution (`_default` configset missing, `solr.TrieIntField` alias, analyzer count, bulk multi-update schema). **Action: per-test root cause** in the fork's schema/configset loader; `_default` and Trie* alias issues are likely shared causes worth fixing centrally.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `CreateRoutedAliasTest` | C | `core/src/test/org/apache/solr/cloud/CreateRoutedAliasTest.java:59` | MRM TODO: routed-alias collection creation fails with "Can not find the specified config set: _default" — the fork doesn't provide/auto-bootstrap the _default configset these te… |
| `DocValuesNotIndexedTest` | C | `core/src/test/org/apache/solr/cloud/DocValuesNotIndexedTest.java:63` | MRM TODO: @BeforeClass NPEs at the bulk-schema MultiUpdate request (DocValuesNotIndexedTest.java:163) so the suite can't initialize; the original note also flags a thread-safety… |
| `public long testIndexQueryDeleteHierarch` | m | `core/src/test/org/apache/solr/cloud/FullSolrCloudDistribCmdsTest.java:324` | MRM-TEST TODO: schema correct? org.apache.solr.client.solrj.impl.BaseCloudSolrClient$RouteException: Error from server at null: Unable to index docs with children: the schema mu… |
| `TestConfigSetImmutable` | C | `core/src/test/org/apache/solr/core/TestConfigSetImmutable.java:41` | MRM TODO: configset-immutable enforcement path returns SolrException "parsing error" in the fork (schema/config write rejection differs); needs investigation against the reworke… |
| `TestXIncludeConfig` | C | `core/src/test/org/apache/solr/core/TestXIncludeConfig.java:30` | MRM TODO: XInclude in schema/config fails with "unknown protocol: solrres" — the fork's Saxon-based schema parser doesn't wire the SystemIdResolver (solrres: URIs) into XInclude… |
| `DocumentAnalysisRequestHandlerTest` | C | `core/src/test/org/apache/solr/handler/DocumentAnalysisRequestHandlerTest.java:49` | MRM TODO: testHandleAnalysisRequest asserts MockTokenizer is applied on the query analysis of the 'whitetok' field, but the fork's document-analysis response doesn't surface it … |
| `testSchemaLoadingComplexAnalyzer()` | m | `core/src/test/org/apache/solr/schema/ResolveAnalyzerByNameTest.java:59` | MRM when run with the other tess we can be 6 vs 7 analyzers |
| `TestExtendedDismaxParser` | C | `core/src/test/org/apache/solr/search/TestExtendedDismaxParser.java:47` | MRM TODO: Many tests here assert the exact parsed-query toString() with a specific DisjunctionMaxQuery disjunct order. Lucene 9.0.0's DisjunctionMaxQuery stores its disjuncts in… |
| `StreamingTest` | C | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamingTest.java:75` | MRM TODO: two fork divergences — (1) the /streams request handler is not registered in the streaming-conf configset (RemoteSolrException 404 "/solr/streams: not found"), and (2)… |
| `testReplaceFieldTypeAccuracy()` | m | `solrj/src/test/org/apache/solr/client/solrj/request/SchemaTest.java:556` | MRM doesn't like solr.TrieIntField to org.apache.solr.schema.TrieIntField |
| `testConfigSet()` | m | `solrj/src/test/org/apache/solr/client/solrj/request/TestCoreAdmin.java:80` | MRM TODO: CREATE with a named configSet diverges in the fork's core/configset admin handling. |

## StateUpdates Overseer feature gaps — 7

**Recommendation:** Assert against the classic queue-based Overseer (operation stats, preferred-leader, `BALANCESHARDUNIQUE`/`ADDREPLICAPROP`). The fork's StateUpdates Overseer lacks these. **Action:** implement the missing ops, or rewrite assertions against the StateUpdates model. Decide per-feature whether the capability is still in scope.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `testAddAndDeleteReplicaProp()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:864` | MRM TODO: whats status of preferred leader? |
| `testBalanceShardUnique()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:890` | MRM TODO: whats status of preferred leader?: Error from server at null: CMD did not return a response:balanceshardunique |
| `OverseerStatusTest` | C | `core/src/test/org/apache/solr/cloud/OverseerStatusTest.java:30` | MRM TODO: relies on the classic queue-based Overseer's operation stats (overseer_operations counts) and leader id that the fork's reworked StateUpdates-based Overseer doesn't po… |
| `SystemCollectionCompatTest` | C | `core/src/test/org/apache/solr/cloud/SystemCollectionCompatTest.java:66` | MRM TODO: @BeforeClass setupSystemCollection fails at line 93 creating/initializing the .system collection — the fork's overseer rework changed .system collection setup, so the … |
| `TestLeaderElectionZkExpiry` | C | `core/src/test/org/apache/solr/cloud/TestLeaderElectionZkExpiry.java:34` | MRM TODO: after the session-expiry storm no Overseer leader is re-elected within the 15s window (assertTrue(found) at line ~90 fails). getLeaderNode reads the standard /overseer… |
| `TestRebalanceLeaders` | C | `core/src/test/org/apache/solr/cloud/TestRebalanceLeaders.java:49` | MRM TODO: the fork's StateUpdates-based Overseer doesn't implement ADDREPLICAPROP/DELETEREPLICAPROP — the state-update channel only carries replica state transitions (L/A/D/R/B)… |
| `TestSkipOverseerOperations` | C | `core/src/test/org/apache/solr/cloud/TestSkipOverseerOperations.java:39` | MRM TODO: needs the classic queue-based Overseer internals. getOverseerLeader() returns null because the fork's reworked leader election doesn't publish the leader id JSON into … |

## @Nightly-only failures — 6

**Recommendation:** Fail only under `-Dtests.nightly=true`, often at `@BeforeClass`. **Action:** run under nightly, root-cause, fix; keep `@Nightly`, drop `@Ignore` once green. Several LTR ones share one cause (`TestRerankBase.getFeatures()` static state) — fix once, unblock many.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `TestXLSXResponseWriter` | C | `contrib/extraction/src/test/org/apache/solr/handler/extraction/TestXLSXResponseWriter.java:49` | MRM TODO: @Nightly test fails under -Dtests.nightly=true at @BeforeClass schema load — "Error loading class 'solr.ClassicTokenizerFactory'" (the analysis-common tokenizer SPI is… |
| `TestFieldLengthFeature` | C | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestFieldLengthFeature.java:31` | MRM TODO: @Nightly LTR test fails under -Dtests.nightly=true — TestRerankBase.getFeatures() NPEs on the static SolrTestCaseJ4.solrConfig (null: setuptest() uses createJettyAndHa… |
| `TestFieldValueFeature` | C | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestFieldValueFeature.java:32` | MRM TODO: @Nightly LTR test fails under -Dtests.nightly=true — TestRerankBase.getFeatures() NPEs on the static SolrTestCaseJ4.solrConfig (null: setuptest() uses createJettyAndHa… |
| `TestOriginalScoreFeature` | C | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestOriginalScoreFeature.java:36` | MRM TODO: @Nightly LTR test fails under -Dtests.nightly=true — TestRerankBase.getFeatures() NPEs on the static SolrTestCaseJ4.solrConfig (null: setuptest() uses createJettyAndHa… |
| `TestLinearModel` | C | `contrib/ltr/src/test/org/apache/solr/ltr/model/TestLinearModel.java:42` | MRM TODO: @Nightly LTR test fails under -Dtests.nightly=true — TestRerankBase.getFeatures() NPEs on the static SolrTestCaseJ4.solrConfig (null: setuptest() uses createJettyAndHa… |
| `LeaderElectionTest` | C | `core/src/test/org/apache/solr/cloud/LeaderElectionTest.java:59` | MRM-TEST TODO: @Nightly+@Slow leader-election test (skips in the standard suite); the fork's reworked leader election needs this rewritten before it can run reliably under nightly. |

## WireMock harness needs updating — 2

**Recommendation:** `UnknownHostException`/DNS against the WireMock stub — stale mock host wiring, not a product bug. **Action:** update the WireMock base setup so stubbed endpoints resolve, then un-ignore. Self-contained, low risk.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `BaseSolrClientWireMockTest` | C | `solrj/src/test/org/apache/solr/client/solrj/impl/BaseSolrClientWireMockTest.java:86` | needs updating, java.net.UnknownHostException: http: Temporary failure in name resolution |
| `CloudHttp2SolrClientWireMockTest` | C | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientWireMockTest.java:49` | needs updating, java.net.UnknownHostException: http: Temporary failure in name resolution |

## Legacy base-class migration (`AbstractFullDistribZkTestBase` → bridge) — 19

**Recommendation:** Extend the legacy `AbstractFullDistribZkTestBase`, being replaced by `SolrCloudBridgeTestCase`. Can't pass until the base is ported. **Action: one coordinated migration** — finish the bridge base, un-ignore the batch together, fix fallout per-test. Highest payoff per effort; do NOT attempt individually.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `BasicDistributedZk2Test` | C | `core/src/test/org/apache/solr/cloud/BasicDistributedZk2Test.java:53` | MRM TODO: convert to bridge |
| `BasicDistributedZkTest` | C | `core/src/test/org/apache/solr/cloud/BasicDistributedZkTest.java:103` | MRM TODO: bridge to new test base class — extends the legacy AbstractFullDistribZkTestBase which the fork is migrating to SolrCloudBridgeTestCase; runs only under @Nightly and t… |
| `ChaosMonkeyNothingIsSafeTest` | C | `core/src/test/org/apache/solr/cloud/ChaosMonkeyNothingIsSafeTest.java:41` | MRM TODO:  bridge |
| `ChaosMonkeyNothingIsSafeWithPullReplicasTest` | C | `core/src/test/org/apache/solr/cloud/ChaosMonkeyNothingIsSafeWithPullReplicasTest.java:52` | MRM TODO: base class to bridge |
| `ChaosMonkeySafeLeaderWithPullReplicasTest` | C | `core/src/test/org/apache/solr/cloud/ChaosMonkeySafeLeaderWithPullReplicasTest.java:49` | MRM TODO: base class to bridge |
| `HttpPartitionOnCommitTest` | C | `core/src/test/org/apache/solr/cloud/HttpPartitionOnCommitTest.java:39` | MRM TODO: base class needs bridge |
| `HttpPartitionTest` | C | `core/src/test/org/apache/solr/cloud/HttpPartitionTest.java:81` | MRM TODO: convert to bridge base — extends the legacy AbstractFullDistribZkTestBase the fork is migrating to SolrCloudBridgeTestCase; runs only under @Nightly and the legacy bas… |
| `HttpPartitionWithTlogReplicasTest` | C | `core/src/test/org/apache/solr/cloud/HttpPartitionWithTlogReplicasTest.java:27` | MRM TODO: base class needs new bridge |
| `LeaderFailoverAfterPartitionTest` | C | `core/src/test/org/apache/solr/cloud/LeaderFailoverAfterPartitionTest.java:48` | MRM TODO: base class needs new bridge |
| `LeaderFailureAfterFreshStartTest` | C | `core/src/test/org/apache/solr/cloud/LeaderFailureAfterFreshStartTest.java:56` | MRM TODO: convert to bridge base class |
| `ReplicationFactorTest` | C | `core/src/test/org/apache/solr/cloud/ReplicationFactorTest.java:59` | MRM TODO: convert to bridge base test class — extends the legacy AbstractFullDistribZkTestBase the fork is migrating to SolrCloudBridgeTestCase; runs only under @Nightly and the… |
| `RestartWhileUpdatingTest` | C | `core/src/test/org/apache/solr/cloud/RestartWhileUpdatingTest.java:36` | MRM TODO: bridge base test class |
| `RollingRestartTest` | C | `core/src/test/org/apache/solr/cloud/RollingRestartTest.java:35` | MRM-TEST TODO: convert to bridge base test class |
| `TestDistribDocBasedVersion` | C | `core/src/test/org/apache/solr/cloud/TestDistribDocBasedVersion.java:39` | MRM TODO: doTestHardFail NPEs at cluster.getShardLeaderJetty (line 112) — slice.getLeader(liveNodes) returns null because the leader's raw state isn't published as LEADER in the… |
| `TestRandomRequestDistribution` | C | `core/src/test/org/apache/solr/cloud/TestRandomRequestDistribution.java:55` | MRM TODO: bridge it |
| `TlogReplayBufferedWhileIndexingTest` | C | `core/src/test/org/apache/solr/cloud/TlogReplayBufferedWhileIndexingTest.java:40` | MRM TODO: switch to bridge base class |
| `TestSolrConfigHandlerCloud` | C | `core/src/test/org/apache/solr/handler/TestSolrConfigHandlerCloud.java:43` | MRM TODO: - some race on adding a dump request handler and then the next command finding it TODO: switch to bridge test class |
| `SolrShardReporterTest` | C | `core/src/test/org/apache/solr/metrics/reporters/SolrShardReporterTest.java:42` | MRM TODO: extends the legacy AbstractFullDistribZkTestBase; "No registered leader found after 10s" (s1[leader=], StateUpdates {1=D,3=L}) — leader election not established + lega… |
| `TestAuthorizationFramework` | C | `core/src/test/org/apache/solr/security/TestAuthorizationFramework.java:45` | MRM TODO: extends the legacy AbstractFullDistribZkTestBase; "No registered leader found after 10s" (s2[leader=], StateUpdates {2=L,3=D}) — leader election not established + lega… |

## Other documented MRM TODO (per-test root cause) — 171

**Recommendation:** Fork-specific failures with a specific cause that doesn't fold into a shared family. **Action: per-test root cause** using the recorded comment as the starting hypothesis.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `TestLTROnSolrCloud` | C | `contrib/ltr/src/test/org/apache/solr/ltr/TestLTROnSolrCloud.java:42` | MRM TODO: |
| `TestLTRWithFacet` | C | `contrib/ltr/src/test/org/apache/solr/ltr/TestLTRWithFacet.java:30` | MRM TODO: |
| `TestParallelWeightCreation` | C | `contrib/ltr/src/test/org/apache/solr/ltr/TestParallelWeightCreation.java:25` | MRM TODO: |
| `TestNoMatchSolrFeature` | C | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestNoMatchSolrFeature.java:35` | MRM TODO: |
| `testParamsToMap()` | m | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestRankingFeature.java:127` | MRM TODO |
| `testParamsToMap()` | m | `contrib/ltr/src/test/org/apache/solr/ltr/feature/TestValueFeature.java:167` | MRM TODO: |
| `TestAdapterModel` | C | `contrib/ltr/src/test/org/apache/solr/ltr/model/TestAdapterModel.java:48` | MRM TODO: |
| `TestMultipleAdditiveTreesModel` | C | `contrib/ltr/src/test/org/apache/solr/ltr/model/TestMultipleAdditiveTreesModel.java:31` | MRM TODO: |
| `TestNeuralNetworkModel` | C | `contrib/ltr/src/test/org/apache/solr/ltr/model/TestNeuralNetworkModel.java:39` | MRM TODO: |
| `TestModelManager` | C | `contrib/ltr/src/test/org/apache/solr/ltr/store/rest/TestModelManager.java:37` | MRM TODO: |
| `TestModelManagerPersistence` | C | `contrib/ltr/src/test/org/apache/solr/ltr/store/rest/TestModelManagerPersistence.java:45` | MRM TODO: |
| `testPathIsAddedToContext()` | m | `core/src/test/org/apache/solr/client/solrj/embedded/TestEmbeddedSolrServerAdminHandler.java:39` | look into after changing thread local byte array in javabincodec to buffer pool |
| `testWatcher()` | m | `core/src/test/org/apache/solr/cloud/CollectionPropsTest.java:168` | MRM TODO: |
| `testMultipleWatchers()` | m | `core/src/test/org/apache/solr/cloud/CollectionPropsTest.java:208` | MRM TODO: |
| `testCreateAndDeleteAlias()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:353` | MRM TODO: |
| `testDeleteAliasedCollection()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:765` | MRM TODO: debug |
| `testModifyCollectionAttribute()` | m | `core/src/test/org/apache/solr/cloud/CollectionsAPISolrJTest.java:918` | MRM TODO: have to fix this up again |
| `deleteReplicaByCount()` | m | `core/src/test/org/apache/solr/cloud/DeleteReplicaTest.java:186` | MRM TODO |
| `testBasicAuth()` | m | `core/src/test/org/apache/solr/cloud/DistribDocExpirationUpdateProcessorTest.java:132` | MRM TODO: |
| `testGroupingSorting()` | m | `core/src/test/org/apache/solr/cloud/DocValuesNotIndexedTest.java:255` | MRM TODO: |
| `testGroupingDVOnlySortFirst()` | m | `core/src/test/org/apache/solr/cloud/DocValuesNotIndexedTest.java:353` | MRM TODO: |
| `ForceLeaderTest` | C | `core/src/test/org/apache/solr/cloud/ForceLeaderTest.java:50` | MRM TODO: |
| `ForceLeaderWithTlogReplicasTest` | C | `core/src/test/org/apache/solr/cloud/ForceLeaderWithTlogReplicasTest.java:22` | MRM TODO: |
| `testThatCantForwardToLeaderFails()` | m | `core/src/test/org/apache/solr/cloud/FullSolrCloudDistribCmdsTest.java:184` | MRM TODO: |
| `testIndexingOneDocPerRequestWithHttpSolrClient()` | m | `core/src/test/org/apache/solr/cloud/FullSolrCloudDistribCmdsTest.java:406` | MRM TODO |
| `testConcurrentIndexing()` | m | `core/src/test/org/apache/solr/cloud/FullSolrCloudDistribCmdsTest.java:485` | TODO http2 concurrent still crap, http1 slows down all http2 |
| `testMissingSplitKey()` | m | `core/src/test/org/apache/solr/cloud/MigrateRouteKeyTest.java:86` | MRM TODO: expects a "split.key cannot be null or empty" 400, but the fork's MIGRATE path fails earlier with "Could not find collection : <target>" (collections=[]) — the just-cr… |
| `testFailedMove()` | m | `core/src/test/org/apache/solr/cloud/MoveReplicaTest.java:188` | MRM TODO: |
| `doHashingTest()` | m | `core/src/test/org/apache/solr/cloud/ShardRoutingTest.java:106` | MRM TODO: |
| `doAtomicUpdate()` | m | `core/src/test/org/apache/solr/cloud/ShardRoutingTest.java:292` | MRM TODO: |
| `testLoadDocsIntoGettingStartedCollection()` | m | `core/src/test/org/apache/solr/cloud/SolrCloudExampleTest.java:75` | MRM TODO: |
| `TestAuthenticationFramework` | C | `core/src/test/org/apache/solr/cloud/TestAuthenticationFramework.java:48` | MRM TODO: createCollection under the pluggable auth framework fails (testBasics) — the fork's reworked authentication plugin wiring no longer passes the mock authenticator throu… |
| `testUploadWithScriptUpdateProcessor()` | m | `core/src/test/org/apache/solr/cloud/TestConfigSetsAPI.java:337` | MRM TODO: debug |
| `testList()` | m | `core/src/test/org/apache/solr/cloud/TestConfigSetsAPI.java:688` | MRM TODO: harden |
| `TestExactSharedStatsCacheCloud` | C | `core/src/test/org/apache/solr/cloud/TestExactSharedStatsCacheCloud.java:25` | MRM TODO: distributed cloud score differs from single-node control — fork's distributed term-stats (ExactStatsCache) doesn't reproduce identical IDF (parallel-commit/flush order… |
| `TestLRUStatsCacheCloud` | C | `core/src/test/org/apache/solr/cloud/TestLRUStatsCacheCloud.java:26` | MRM TODO: distributed cloud score differs from single-node control — fork's distributed term-stats doesn't reproduce identical IDF (parallel-commit/flush ordering in TestBaseSta… |
| `TestLocalStatsCacheCloud` | C | `core/src/test/org/apache/solr/cloud/TestLocalStatsCacheCloud.java:28` | MRM TODO: distributed cloud score differs from single-node control — fork's distributed term-stats doesn't reproduce identical IDF (parallel-commit/flush ordering in TestBaseSta… |
| `test()` | m | `core/src/test/org/apache/solr/cloud/TestOnReconnectListenerSupport.java:59` | MRM TODO: check this - i speed some stuff up, reload too fast? |
| `testRemoveAllWriterReplicas()` | m | `core/src/test/org/apache/solr/cloud/TestPullReplica.java:300` | MRM TODO: |
| `testDelegationTokenSolrClientWithUpdateRequests()` | m | `core/src/test/org/apache/solr/cloud/TestSolrCloudWithDelegationTokens.java:450` | MRM TODO: need to make proxy call compat with security |
| `testForwarding()` | m | `core/src/test/org/apache/solr/cloud/TestSolrCloudWithSecureImpersonation.java:304` | MRM TODO: HttpSolrCall#remoteQuery needs to handle this |
| `VMParamsZkACLAndCredentialsProvidersTest` | C | `core/src/test/org/apache/solr/cloud/VMParamsZkACLAndCredentialsProvidersTest.java:39` | MRM TODO: setUp fails — the fork's reworked SolrZkClient lifecycle (start()) + ZK ACL/credentials VM-params provider wiring changed, so the credentials/ACL provider tests no lon… |
| `ZkCLITest` | C | `core/src/test/org/apache/solr/cloud/ZkCLITest.java:58` | MRM TODO: Zk CLI behavior diverges in the fork — e.g. testList output format differs (expected "/ (c=1,v=1)" tree style but got "/[test [v=0]]"), several subcommands fail. |
| `testPublishAndWaitForDownStates()` | m | `core/src/test/org/apache/solr/cloud/ZkControllerTest.java:244` | MRM TODO: debug |
| `ZkNodePropsTest` | C | `core/src/test/org/apache/solr/cloud/ZkNodePropsTest.java:34` | MRM TODO: the javabin half of the round-trip can't work — the fork's ZkNodeProps implements only JSONWriter.Writable (not MapWriter/Map), so JavaBinCodec.writeVal serializes it … |
| `testRegisterTerm()` | m | `core/src/test/org/apache/solr/cloud/ZkShardTermsTest.java:143` | MRM TODO: I violated this recently, need to see if that is the right thing to do |
| `ZkSolrClientTest` | C | `core/src/test/org/apache/solr/cloud/ZkSolrClientTest.java:32` | MRM TODO: testZkCmdExectutor expects SessionExpiredException but the fork wraps ZK connection loss as SolrException "Cannot connect to ZK" (different connection-failure handling… |
| `testRestoreFailure()` | m | `core/src/test/org/apache/solr/cloud/api/collections/AbstractCloudBackupRestoreTestCase.java:162` | MRM TODO: debug |
| `testAsyncRequests()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIAsyncDistributedZkTest.java:103` | MRM TODO |
| `testAsyncIdRaceCondition()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIAsyncDistributedZkTest.java:201` | MRM TODO: investigate |
| `testCreateShouldFailOnExistingCore()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIDistClusterPerZkTest.java:134` | MRM TODO: we can speed this up, TJP ~ WIP: fails |
| `testCollectionsAPI()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CollectionsAPIDistClusterPerZkTest.java:232` | MRM TODO: |
| `testCustomCollectionsAPI()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CustomCollectionTest.java:64` | MRM TODO: counts are off |
| `testRouteFieldForImplicitRouter()` | m | `core/src/test/org/apache/solr/cloud/api/collections/CustomCollectionTest.java:128` | MRM TODO: something flkey here on doc counts at the end |
| `testSplitShardWithRule()` | m | `core/src/test/org/apache/solr/cloud/api/collections/ShardSplitTest.java:611` | MRM TODO: - need to fix state updates for rules |
| `testSplitShardWithRuleLink()` | m | `core/src/test/org/apache/solr/cloud/api/collections/ShardSplitTest.java:618` | MRM TODO: - need to fix state updates for rules |
| `testRecreateCollectionAfterFailure()` | m | `core/src/test/org/apache/solr/cloud/api/collections/TestCollectionAPI.java:1023` | after separating out HttpSolrCall into two classes for API V2, this and the other test here pass individually but not when run together |
| `SolrCoreCheckLockOnStartupTest` | C | `core/src/test/org/apache/solr/core/SolrCoreCheckLockOnStartupTest.java:38` | MRM TODO: fork no longer throws LockObtainFailedException on startup when the index is locked (default lock factory / startup lock-check behavior changed in the reworked core in… |
| `testRefCount()` | m | `core/src/test/org/apache/solr/core/SolrCoreTest.java:141` | MRM TODO: changed |
| `testRefCountMT()` | m | `core/src/test/org/apache/solr/core/SolrCoreTest.java:179` | MRM TODO: changed |
| `TestCoreDiscovery` | C | `core/src/test/org/apache/solr/core/TestCoreDiscovery.java:49` | MRM TODO: fork's CoreContainer core-discovery/transient-core semantics differ from upstream (transient cores loaded eagerly: "3 expected but 7 loaded"; loadOnStartup count misma… |
| `TestImplicitCoreProperties` | C | `core/src/test/org/apache/solr/core/TestImplicitCoreProperties.java:27` | MRM TODO: core init fails under the test SecurityManager with FilePermission denied writing the index dir under build/resources/test (data dir not redirected to a temp dir grant… |
| `testJmxUpdate()` | m | `core/src/test/org/apache/solr/core/TestJmxIntegration.java:160` | not finding the searcher mbean |
| `testJmxOnCoreReload()` | m | `core/src/test/org/apache/solr/core/TestJmxIntegration.java:226` | MRM TODO: debug |
| `testLazyLoad()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:107` | MRM TODO: harden |
| `testCachingLimit()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:207` | MRM TODO: debug |
| `testRace()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:281` | MRM TODO: harden |
| `testCreateSame()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:331` | MRM TODO: debug |
| `testCreateTransientFromAdmin()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:394` | MRM TODO: harden |
| `testBadConfigsGenerateErrors()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:476` | MRM TODO: debug |
| `testNoCommit()` | m | `core/src/test/org/apache/solr/core/TestLazyCores.java:806` | MRM TODO: debug |
| `testUserProp()` | m | `core/src/test/org/apache/solr/core/TestSolrConfigHandler.java:201` | MRM TODO: - debug, may not to poll a short while? |
| `testReqParams()` | m | `core/src/test/org/apache/solr/core/TestSolrConfigHandler.java:646` | MRM TODO: debug |
| `doTestDetails()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:322` | currently pulls back an empty response |
| `doTestIndexAndConfigReplication()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:541` | MRM TODO: |
| `doTestIndexFetchOnMasterRestart()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:676` | MRM TODO: |
| `doTestIndexFetchWithMasterUrl()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:796` | MRM TODO: |
| `doTestStressReplication()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:924` | MRM TODO: |
| `doTestRepeater()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:1091` | MRM TODO: |
| `doTestIndexAndConfigAliasReplication()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:1419` | MRM TODO: |
| `testRateLimitedReplication()` | m | `core/src/test/org/apache/solr/handler/TestReplicationHandler.java:1505` | this test is insufficient to test this feature - it was not even picking up the throttle setting to begin with |
| `TestSQLHandlerNonCloud` | C | `core/src/test/org/apache/solr/handler/TestSQLHandlerNonCloud.java:41` | MRM TODO: need to look into - may relate to the above |
| `CoreAdminHandlerTest` | C | `core/src/test/org/apache/solr/handler/admin/CoreAdminHandlerTest.java:53` | MRM TODO: @BeforeClass fails "Core should have been renamed!" — the fork's core RENAME semantics (transient/managed core lifecycle) diverge from upstream. Same family as TestCor… |
| `testMetricsSnapshot()` | m | `core/src/test/org/apache/solr/handler/admin/MBeansHandlerTest.java:155` | MRM TODO: |
| `testPropertyFilter()` | m | `core/src/test/org/apache/solr/handler/admin/MetricsHandlerTest.java:261` | MRM TODO: something is off since making search cache multi map item a weak ref - needs a tweak |
| `BadComponentTest` | C | `core/src/test/org/apache/solr/handler/component/BadComponentTest.java:28` | MRM TODO: fork's core-init error handling doesn't record the failed QueryElevationComponent init in CoreContainer.getCoreInitFailures() the way upstream does, so hasInitExceptio… |
| `testReloadDuringBuild()` | m | `core/src/test/org/apache/solr/handler/component/InfixSuggestersTest.java:102` | MRM TODO: don't think the test is right |
| `testShutdownDuringBuild()` | m | `core/src/test/org/apache/solr/handler/component/InfixSuggestersTest.java:119` | MRM TODO: don't think the test is right |
| `testZkConnected()` | m | `core/src/test/org/apache/solr/handler/component/SearchHandlerTest.java:138` | MRM TODO: not always working now |
| `testRequireZkConnected()` | m | `core/src/test/org/apache/solr/handler/component/SearchHandlerTest.java:185` | MRM TODO: not always working now |
| `testRebuildOnCommit()` | m | `core/src/test/org/apache/solr/handler/component/SpellCheckComponentTest.java:291` | MRM TODO: |
| `testThresholdTokenFrequency()` | m | `core/src/test/org/apache/solr/handler/component/SpellCheckComponentTest.java:304` | MRM TODO: |
| `doPerField()` | m | `core/src/test/org/apache/solr/handler/component/TermVectorComponentTest.java:275` | TODO MRM json failed query validation |
| `testSystemProperties()` | m | `core/src/test/org/apache/solr/metrics/JvmMetricsTest.java:112` | MRM TODO: system.properties gauge is null |
| `SolrGraphiteReporterTest` | C | `core/src/test/org/apache/solr/metrics/reporters/SolrGraphiteReporterTest.java:46` | MRM TODO: debug - the reporter shows up for the core but not the node |
| `testFacetOverPointFieldWithMinCount0()` | m | `core/src/test/org/apache/solr/request/TestFaceting.java:366` | MRM TODO: silly non thread safe spammy warning no longer in query |
| `testStreamUrl()` | m | `core/src/test/org/apache/solr/request/TestRemoteStreaming.java:79` | MRM TODO: HTTP1 off for the moment |
| `testStreamBodyDefaultAndConfigApi()` | m | `core/src/test/org/apache/solr/request/TestStreamBody.java:101` | MRM investigate after http2 fixes |
| `testPerf()` | m | `core/src/test/org/apache/solr/request/TestWriterPerf.java:162` | TODO MRM fail in test perf |
| `testJavabinCodecWithCharSeq()` | m | `core/src/test/org/apache/solr/response/TestBinaryResponseWriter.java:73` | MRM TODO: |
| `testPointKeyFieldType()` | m | `core/src/test/org/apache/solr/schema/ExternalFileFieldSortTest.java:65` | MRM TODO: - not throwing an exception |
| `testMultiBad()` | m | `core/src/test/org/apache/solr/search/TestFoldingMultitermQuery.java:294` | MRM TODO: debug - came after concurrent plugin loading |
| `testSearcherListeners()` | m | `core/src/test/org/apache/solr/search/TestIndexSearcher.java:228` | MRM TODO: |
| `testDontUseColdSearcher()` | m | `core/src/test/org/apache/solr/search/TestIndexSearcher.java:294` | MRM TODO: |
| `testBBox()` | m | `core/src/test/org/apache/solr/search/TestSolr4Spatial2.java:91` | MRM TODO: |
| `testMultiCollectionQuery()` | m | `core/src/test/org/apache/solr/search/stats/TestDistribIDF.java:176` | MRM TODO: |
| `AuditLoggerIntegrationTest` | C | `core/src/test/org/apache/solr/security/AuditLoggerIntegrationTest.java:83` | MRM TODO: nodes fail to start ("Exception in CoreContainer load") when the audit-logger plugin is configured — the fork's audit-logger init/security-config wiring diverges. |
| `basicTest()` | m | `core/src/test/org/apache/solr/security/BasicAuthOnSingleNodeTest.java:60` | MRM TODO: |
| `testDeleteSecurityJsonZnode()` | m | `core/src/test/org/apache/solr/security/BasicAuthOnSingleNodeTest.java:75` | MRM TODO: debug |
| `JWTAuthPluginIntegrationTest` | C | `core/src/test/org/apache/solr/security/JWTAuthPluginIntegrationTest.java:73` | MRM TODO: nodes fail to start ("Exception in CoreContainer load" via ParWork) when the JWT auth plugin is configured — the fork's JWT/security-config init wiring diverges. |
| `TestZkAclsWithHadoopAuth` | C | `core/src/test/org/apache/solr/security/hadoop/TestZkAclsWithHadoopAuth.java:50` | MRM TODO: need to enable zk acls for this test |
| `testHttpResponse()` | m | `core/src/test/org/apache/solr/servlet/ResponseHeaderTest.java:60` | MRM TODO: use Http2SolrClient#GET |
| `testSimple()` | m | `core/src/test/org/apache/solr/uninverting/TestDocTermOrds.java:98` | MRM TODO |
| `ParsingFieldUpdateProcessorsTest` | C | `core/src/test/org/apache/solr/update/processor/ParsingFieldUpdateProcessorsTest.java:52` | MRM TODO: debug this, get core close stuff too |
| `testJavaScriptCompatibility()` | m | `core/src/test/org/apache/solr/update/processor/StatelessScriptUpdateProcessorFactoryTest.java:239` | MRM-TEST TODO: investigate |
| `testConcurrentAdds()` | m | `core/src/test/org/apache/solr/update/processor/TestDocBasedVersionConstraints.java:443` | MRM TODO: - something not air tight about DocBasedVersionConstraintsProcessor? |
| `testBasic()` | m | `core/src/test/org/apache/solr/util/TestExportTool.java:61` | MRM TODO: debug |
| `testVeryLargeCluster()` | m | `core/src/test/org/apache/solr/util/TestExportTool.java:136` | MRM-Test TODO: debug |
| `testTechproductsExample()` | m | `core/src/test/org/apache/solr/util/TestSolrCLIRunExample.java:308` | MRM TODO finish http2 conversion |
| `testInteractiveSolrCloudExample()` | m | `core/src/test/org/apache/solr/util/TestSolrCLIRunExample.java:414` | MRM-Test TODO: look into this, loops a lot |
| `testUsingConsistentRandomization()` | m | `core/src/test/org/apache/solr/util/TestTestInjection.java:91` | MRM TODO |
| `TestDistributedTracing` | C | `core/src/test/org/apache/solr/util/tracing/TestDistributedTracing.java:46` | MRM TODO: - does not appear to be fully working, perhaps due to cluster property change .. |
| `testCanUploadConfigToZk()` | m | `solrj/src/test/org/apache/solr/client/ref_guide_examples/ZkConfigFilesTest.java:63` | MRM TODO |
| `testMergeIndexesByDirName()` | m | `solrj/src/test/org/apache/solr/client/solrj/MergeIndexesExampleTestBase.java:148` | MRM TODO |
| `testMergeMultipleRequest()` | m | `solrj/src/test/org/apache/solr/client/solrj/MergeIndexesExampleTestBase.java:179` | MRM TODO |
| `testSimple()` | m | `solrj/src/test/org/apache/solr/client/solrj/TestLBHttp2SolrClient.java:136` | MRM TODO |
| `testTwoServers()` | m | `solrj/src/test/org/apache/solr/client/solrj/TestLBHttp2SolrClient.java:184` | MRM TODO |
| `testReliability()` | m | `solrj/src/test/org/apache/solr/client/solrj/TestLBHttp2SolrClient.java:213` | MRM TODO |
| `testSimple()` | m | `solrj/src/test/org/apache/solr/client/solrj/TestLBHttpSolrClient.java:127` | MRM TODO |
| `testTwoServers()` | m | `solrj/src/test/org/apache/solr/client/solrj/TestLBHttpSolrClient.java:181` | MRM TODO |
| `testBadSetup()` | m | `solrj/src/test/org/apache/solr/client/solrj/embedded/SolrExampleJettyTest.java:58` | MRM TODO: ~ debug |
| `testUtf8QueryPerf()` | m | `solrj/src/test/org/apache/solr/client/solrj/embedded/SolrExampleJettyTest.java:120` | tests fail after this one passes? |
| `testCompression()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/BasicHttpSolrClientTest.java:205` | MRM TODO: changed for http2 |
| `testInterceptors()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/BasicHttpSolrClientTest.java:299` | MRM TODO: changed for http2 |
| `testRetry()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientRetryTest.java:59` | MRM TODO: metrics is disabled i think |
| `testAliasHandling()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:181` | TODO: aliases are still a bit whack, I'll look at them again some day ... mm |
| `preferLocalShardsTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:379` | MRM TODO: |
| `singleShardedPreferenceRules()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:467` | MRM TODO: |
| `testWrongZkChrootTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:755` | MRM TODO: ~ getting "org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /aliases.json" |
| `testVersionsAreReturned()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:781` | MRM TODO: something looks funky here |
| `testRetryUpdatesWhenClusterStateIsStale()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:860` | MRM TODO: |
| `preferReplicaTypesTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudHttp2SolrClientTest.java:940` | MRM TODO: feature not working |
| `testRetry()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientRetryTest.java:56` | MRM TODO: metrics not current enabled I think |
| `testParallelUpdateQTime()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:158` | MRM TODO: |
| `testOverwriteOption()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:175` | MRM TODO: |
| `preferLocalShardsTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:425` | MRM TODO: |
| `checkCollectionParameters()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:676` | MRM TODO: ~ hangs, possibly related to creating collections with processAsync |
| `testVersionsAreReturned()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:827` | MRM TODO: |
| `testInitializationWithSolrUrls()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:876` | MRM TODO: |
| `testRetryUpdatesWhenClusterStateIsStale()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:898` | MRM TODO: |
| `preferReplicaTypesTest()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/CloudSolrClientTest.java:981` | MRM TODO: |
| `testEnsureDocumentsSentToCorrectCollection()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/ConcurrentUpdateHttp2SolrClientMultiCollectionTest.java:67` | MRM TODO: debug |
| `Http2SolrClientCompatibilityTest` | C | `solrj/src/test/org/apache/solr/client/solrj/impl/Http2SolrClientCompatibilityTest.java:36` | MRM TODO: after lucene and solr TLP update issue |
| `Http2SolrClientTest` | C | `solrj/src/test/org/apache/solr/client/solrj/impl/Http2SolrClientTest.java:63` | MRM TODO: moved to jetty 10 from 9, needs update |
| `testTimeout()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/Http2SolrClientTest.java:81` | MRM TODO |
| `testSSLSystemProperties()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/HttpClientUtilTest.java:46` | MRM TODO: ~ check SSL related sys prop changes affecting this test |
| `testPoolSize()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/HttpSolrClientConPoolTest.java:65` | MRM TODO: after lucene and solr TLP update issue |
| `testLBClient()` | m | `solrj/src/test/org/apache/solr/client/solrj/impl/HttpSolrClientConPoolTest.java:112` | MRM TODO: after lucene and solr TLP update issue |
| `testGraphHandler()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/graph/GraphExpressionTest.java:856` | MRM TODO: debug |
| `testErrorPropagation()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/sql/JdbcTest.java:418` | ("Fix error checking") |
| `testPriorityStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamDecoratorTest.java:2453` | MRM TODO: maybe executor stop? |
| `testParallelPriorityStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamDecoratorTest.java:2526` | MRM TODO: maybe executor stop? |
| `testClassifyStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamDecoratorTest.java:3546` | MRM TODO: maybe executor stop? |
| `testKnnSearchStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamExpressionTest.java:695` | MRM: not finding doc1 in mlt |
| `testMultiCollection()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamExpressionTest.java:1800` | debug MRM TODO: |
| `testTopicStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamExpressionTest.java:2186` | MRM TODO: maybe executor stop? |
| `testParallelTopicStream()` | m | `solrj/src/test/org/apache/solr/client/solrj/io/stream/StreamExpressionTest.java:2352` | MRM TODO: maybe executor stop?13 |
| `testValidCoreRename()` | m | `solrj/src/test/org/apache/solr/client/solrj/request/TestCoreAdmin.java:181` | MRM TODO: core RENAME doesn't reflect in core list ([collection1, core0]) — fork transient/managed core RENAME semantics diverge (same family as core CoreAdminHandlerTest). |
| `testCoreSwap()` | m | `solrj/src/test/org/apache/solr/client/solrj/request/TestCoreAdmin.java:199` | MRM TODO: core SWAP semantics diverge in the fork's core management. |
| `testReloadCoreAfterFailure()` | m | `solrj/src/test/org/apache/solr/client/solrj/request/TestCoreAdmin.java:278` | MRM TODO: - causes an updatelog leak |
| `testUploadConfig()` | m | `solrj/src/test/org/apache/solr/common/cloud/TestZkConfigManager.java:68` | MRM TODO |

## Inherited from upstream Solr (pre-existing) — 5

**Recommendation:** Carry upstream JIRA refs (`SOLR-7964`, `SOLR-7814`) or upstream by-design limits (Remove-Date non-UTC, stream.body disabled, ConcatenateGraph XML char). **Action: lowest priority** — ignored in upstream too, not fork regressions. Track against the JIRA; revisit only if upstream fixed them.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `testContextFilterWithHighlight()` | m | `core/src/test/org/apache/solr/handler/component/SuggestComponentContextFilterQueryTest.java:234` | TODO: SOLR-7964 |
| `testAssertTagStreamingWithStreamBodyParam()` | m | `core/src/test/org/apache/solr/handler/tagger/EmbeddedSolrNoSerializeTest.java:129` | ("As of Solr 7, stream.body is disabled by default for security ") // DWS: dubious, IMO and it can't be enabled with EmbeddedSolrServer until SOLR-12126 |
| `testPartialMatching()` | m | `core/src/test/org/apache/solr/handler/tagger/TaggerTest.java:139` | TODO ConcatenateGraphFilter uses a special separator char that we can't put into XML (invalid char) |
| `testBoost()` | m | `core/src/test/org/apache/solr/search/join/TestScoreJoinQPScore.java:192` | ("SOLR-7814, also don't forget cover boost at testCacheHit()") |
| `testRemoveDateUsingDateType()` | m | `core/src/test/org/apache/solr/update/processor/AtomicUpdatesTest.java:630` | ("Remove Date is not supported in other formats than UTC") |

## Environment-specific / by-design not-applicable — 3

**Recommendation:** Legitimately not runnable as written: HDFS truncate unsupported (`HDFS-3107`), `NoLockFactory` makes a lock test meaningless. **Action:** convert to a *documented permanent skip* (`Assume`/`@AwaitsFix` w/ reason, or delete with rationale). Not fork regressions — stop counting as TODO.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `HdfsBackupRepositoryTest` | C | `core/src/test/org/apache/solr/core/backup/repository/HdfsBackupRepositoryTest.java:32` | MRM TODO: HDFS shared location in separate zk node. |
| `testObtainsLock()` | m | `core/src/test/org/apache/solr/index/hdfs/CheckHdfsIndexTest.java:136` | ("We explicitly use a NoLockFactory, so this test doesn't make sense.") |
| `testDropBuffered()` | m | `core/src/test/org/apache/solr/search/TestRecoveryHdfs.java:386` | ("HDFS-3107: no truncate support yet") |

## Abstract base classes (correctly ignored) — 2

**Recommendation:** Abstract classes the runner must not execute. **Action: none.** Correct usage. Optionally drop the redundant annotation since the class is `abstract` — cosmetic only.

| Test | Lvl | File:line | Recorded comment |
|------|:---:|-----------|------------------|
| `TestBaseStatsCacheCloud` | C | `core/src/test/org/apache/solr/cloud/TestBaseStatsCacheCloud.java:51` | ("Abstract classes should not be executed as tests") |
| `TestBaseStatsCache` | C | `core/src/test/org/apache/solr/search/stats/TestBaseStatsCache.java:26` | ("Abstract calls should not executed as test") |
