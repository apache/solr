/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.zero.process;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.MetadataCacheManager.ZeroCoreStage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around synchronization of concurrent indexing, pushes and pulls happening on a core of a
 * Zero collection {@link DocCollection#isZeroIndex()}
 */
@SuppressWarnings("LockNotBeforeTry")
public class ZeroCoreConcurrencyTest extends ZeroStoreSolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION_NAME = "zeroCollection";
  private static final String SHARD_NAME = "shard1";

  /**
   * Number of serial indexing iterations for each test. This is the main setting, queries and
   * failover iterations stop after indexing ends. Higher the value, longer the tests would run.
   */
  private static final int INDEXING_ITERATIONS = TEST_NIGHTLY ? 100 : 20;

  /** Maximum number of concurrent indexing requests per indexing iteration. */
  private static final int MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION = 10;

  /** Maximum number of docs per indexing request. */
  private static final int MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST = 100;

  /**
   * Indexing can fail because of leader failures (especially when test {@link
   * #includeFailovers()}). The test will re-attempt up till this number of times before bailing
   * out. For test to succeed, indexing request have to succeed in these many attempts.
   */
  private static final int MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST = 10;

  /** Maximum number of concurrent query requests per query iteration. */
  private static final int MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION = 10;

  /**
   * Querying is faster than indexing, to pace it better with indexing, a delay is added between
   * each query iteration.
   */
  private static final int DELAY_MS_BETWEEN_EACH_QUERY_ITERATION = 50;

  /** Minimum time between each failover. */
  private static final int DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION = 500;

  /** Manages test state from start to end. */
  private TestState testState;

  @Before
  public void setupTest() throws Exception {
    int numNodes = 4;

    System.setProperty(
        ZeroConfig.ZeroSystemProperty.maxFailedCorePullAttempts.getPropertyName(), "1");
    System.setProperty(ZeroConfig.ZeroSystemProperty.numCorePullAutoRetries.getPropertyName(), "1");
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullAttemptDelayS.getPropertyName(), "10");
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "10");

    setupCluster(numNodes);
    testState = new TestState();
    setupSolrNodesForTest();

    // One less than number of nodes.
    // The extra node will be used at the end of test to verify
    // the contents of Zero store by querying for all docs on a new replica.
    int numReplicas = numNodes - 1;
    // Later on we can consider choosing random number of shards and replicas.
    // To handle multiple shards, we need to update code where SHARD_NAME is used.
    setupZeroCollectionWithShardNames(COLLECTION_NAME, numReplicas, SHARD_NAME);
  }

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /** Tests that concurrent indexing succeed. */
  @Test
  public void testIndexing() throws Exception {
    final boolean includeDeletes = false;
    includeIndexing(includeDeletes);
    run();
  }

  /** Tests that concurrent indexing with concurrent queries succeed. */
  @Test
  public void testIndexingQueries() throws Exception {
    final boolean includeDeletes = false;
    includeIndexing(includeDeletes);
    includeQueries();
    run();
  }

  /** Tests that concurrent indexing with deletes and concurrent queries succeed. */
  @Test
  public void testIndexingQueriesDeletes() throws Exception {
    final boolean includeDeletes = true;
    includeIndexing(includeDeletes);
    includeQueries();
    run();
  }

  /**
   * Tests that concurrent indexing with deletes, concurrent queries and explicit failovers succeed.
   */
  // @Test
  // TODO: This test flaps time to time. The symptom of the failure is missing docs i.e. indexing is
  // declared successful but query could not reproduce all of the docs. Flapping was reproduced with
  // NRT collection on vanilla Solr 8.3.
  // Root cause unknown. Keeping disabled for now.
  public void todoTestIndexingQueriesDeletesFailovers() throws Exception {
    final boolean includeDeletes = true;
    includeIndexing(includeDeletes);
    includeQueries();
    includeFailovers();
    run();
  }

  /** Verifies that the correct locks are held when updating the Zero shard metadata */
  @SuppressWarnings("try")
  @Test
  public void testCorrectLocksHeld() throws Exception {
    JettySolrRunner runner = getFirstJettyRunnerWithAtLeastOneSolrCore();
    assertNotNull(runner);
    CoreContainer coreContainer = runner.getCoreContainer();
    ZeroStoreManager storeManager = coreContainer.getZeroStoreManager();
    MetadataCacheManager metadataCacheManager = storeManager.getMetadataCacheManager();
    String coreName = coreContainer.getLoadedCoreNames().get(0);
    // assert the update fails if no lock is held
    assertFalse(assertCanUpdateZeroVersion(metadataCacheManager, coreName));
    ZeroAccessLocks locks =
        metadataCacheManager.getOrCreateCoreMetadata(coreName).getZeroAccessLocks();

    try (AutoCloseable ignore = locks.acquirePullLock(5)) {
      // assert the update succeeds if the pull lock is held
      assertTrue(assertCanUpdateZeroVersion(metadataCacheManager, coreName));
    }

    try (AutoCloseable ignore = locks.acquireIndexingLock(5)) {
      try (AutoCloseable ignore2 = locks.acquirePushLock(5)) {
        // assert the update succeeds if the indexing lock and push lock are held
        assertTrue(assertCanUpdateZeroVersion(metadataCacheManager, coreName));
      }
    }

    try (AutoCloseable ignore = locks.acquirePushLock(5)) {
      // assert the update fails if only the push lock is held
      assertFalse(assertCanUpdateZeroVersion(metadataCacheManager, coreName));
    }

    try (AutoCloseable ignore = locks.acquireIndexingLock(5)) {
      // assert the update fails if only the indexing lock is held, not the push lock
      assertFalse(assertCanUpdateZeroVersion(metadataCacheManager, coreName));
    }
  }

  private JettySolrRunner getFirstJettyRunnerWithAtLeastOneSolrCore() {
    for (var runner : cluster.getJettySolrRunners()) {
      CoreContainer coreContainer = runner.getCoreContainer();
      if (!coreContainer.getLoadedCoreNames().isEmpty()) {
        return runner;
      }
    }
    return null;
  }

  private boolean assertCanUpdateZeroVersion(
      MetadataCacheManager metadataCacheManager, String coreName) {
    try {
      // arguments don't matter so hard coded here
      metadataCacheManager.updateCoreMetadata(coreName, true);
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("locks are not held by the current thread"));
      return false;
    }
    return true;
  }

  /**
   * It starts all the threads that are included in the test (indexing, queries and failovers) in
   * parallel. Then wait for them to finish (run length depends on {@link #INDEXING_ITERATIONS}). At
   * the end it makes sures that no critical section was breached and no unexpected error occurred.
   * Then verify the contents of Zero store by querying for all docs on a new replica.
   */
  private void run() throws Exception {
    testState.startIncludedThreads();
    testState.waitForThreadsToStop();
    analyzeCoreConcurrencyStagesForBreaches();
    testState.checkErrors();
    Replica newReplica = addReplica();
    queryNewReplicaAndVerifyAllDocsFound(newReplica);
  }

  /**
   * Adds a thread to test, that goes over {@link #INDEXING_ITERATIONS} or until it is interrupted.
   * In each iteration it creates between 1 and {@link
   * #MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION} threads by calling {@link
   * #createIndexingThreads(int, int, boolean)}, starts them concurrently and wait for them to
   * finish before going to next iteration. Each indexing thread adds between 1 and {@link
   * #MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST} docs.
   *
   * @param includeDeletes whether to randomly mark some docs for deletion and delete them in
   *     subsequent indexing requests or not
   */
  private void includeIndexing(boolean includeDeletes) {
    Thread t =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < INDEXING_ITERATIONS && !testState.stopRunning.get(); i++) {
                  int numIndexingThreads =
                      random().nextInt(MAX_NUM_OF_CONCURRENT_INDEXING_REQUESTS_PER_ITERATION) + 1;
                  int numDocsToAddPerThread =
                      random().nextInt(MAX_NUM_OF_DOCS_PER_INDEXING_REQUEST) + 1;
                  Thread[] indexingThreads =
                      createIndexingThreads(
                          numIndexingThreads, numDocsToAddPerThread, includeDeletes);
                  for (int j = 0; j < numIndexingThreads; j++) {
                    indexingThreads[j].start();
                  }
                  for (int j = 0; j < numIndexingThreads; j++) {
                    indexingThreads[j].join();
                  }
                  if (Thread.interrupted()) {
                    // we have been interrupted so we will stop running
                    testState.stopRunning.set(true);
                  }
                }
              } catch (Exception ex) {
                testState.indexingErrors.add(ex.getMessage());
              }
              // everything else stops running when indexing finishes
              testState.stopRunning.set(true);
            });
    testState.includeThread(t);
  }

  /**
   * Creates {@code numIndexingThreads} threads with each adding {@code numDocsToAddPerThread}.
   *
   * @param includeDeletes whether to randomly mark some docs for deletion and delete them in
   *     subsequent indexing requests or not
   */
  private Thread[] createIndexingThreads(
      int numIndexingThreads, int numDocsToAddPerThread, boolean includeDeletes) {
    log.info("numIndexingThreads={}", numIndexingThreads);
    Thread[] indexingThreads = new Thread[numIndexingThreads];
    for (int i = 0; i < numIndexingThreads && !testState.stopRunning.get(); i++) {
      indexingThreads[i] =
          new Thread(
              () -> {
                List<String> idsToAdd = new ArrayList<>();
                // prepare the list of docs to add and delete outside the reattempt loop
                for (int j = 0; j < numDocsToAddPerThread; j++) {
                  String docId = Integer.toString(testState.docIdGenerator.incrementAndGet());
                  idsToAdd.add(docId);
                }
                List<String> idsToDelete = testState.idBatchesToDelete.poll();

                // attempt until succeeded or max attempts
                for (int j = 0; j < MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST; j++) {
                  try {
                    String message =
                        "attempt="
                            + (j + 1)
                            + " numDocsToAdd="
                            + numDocsToAddPerThread
                            + " docsToAdd="
                            + idsToAdd;
                    if (idsToDelete != null) {
                      message +=
                          " numDocsToDelete=" + idsToDelete.size() + " docsToDelete=" + idsToDelete;
                    }
                    log.info(message);

                    UpdateRequest updateReq = new UpdateRequest();
                    for (String s : idsToAdd) {
                      updateReq.add("id", s);
                    }
                    if (includeDeletes && idsToDelete != null) {
                      updateReq.deleteById(idsToDelete);
                    }
                    processUpdateRequest(updateReq);

                    testState.numDocsIndexed.addAndGet(numDocsToAddPerThread);
                    if (idsToDelete != null) {
                      testState.idsDeleted.addAll(idsToDelete);
                    }

                    // randomly select some docs that can be deleted
                    if (includeDeletes) {
                      List<String> idsThatCanBeDeleted = new ArrayList<>();
                      for (String indexedId : idsToAdd) {
                        if (random().nextBoolean()) {
                          idsThatCanBeDeleted.add(indexedId);
                        }
                      }
                      if (!idsThatCanBeDeleted.isEmpty()) {
                        testState.idBatchesToDelete.offer(idsThatCanBeDeleted);
                      }
                    }
                    // indexing was successful, stop attempting
                    break;
                  } catch (Exception ex) {
                    // last attempt also failed, record the error
                    if (j == MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST - 1) {
                      StringWriter sw = new StringWriter();
                      ex.printStackTrace(new PrintWriter(sw));
                      testState.indexingErrors.add(sw.toString());
                    }
                  }
                }
              });
    }
    return indexingThreads;
  }

  /**
   * Sends update request to the server, randomly choosing whether to send it with commit=true or
   * not ZERO replica does not need an explicit commit since it always does an implicit hard commit
   * but still it is valid to send an update with or without a commit, therefore, testing both.
   */
  private void processUpdateRequest(UpdateRequest request) throws Exception {
    UpdateResponse response =
        random().nextBoolean()
            ? request.process(cluster.getSolrClient(), COLLECTION_NAME)
            : request.commit(cluster.getSolrClient(), COLLECTION_NAME);

    if (response.getStatus() != 0) {
      throw new RuntimeException("Update request failed with status=" + response.getStatus());
    }
  }

  /**
   * Adds a thread to test, that goes over iterations until the test is stopped {@link
   * TestState#stopRunning}. In each iteration it creates between 1 and {@link
   * #MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION} threads by calling {@link
   * #createQueryThreads(int)}, starts them concurrently and wait for them to finish before going to
   * next iteration. To pace it better with indexing, {@link #DELAY_MS_BETWEEN_EACH_QUERY_ITERATION}
   * delay is added between each query iteration.
   */
  private void includeQueries() {
    Thread t =
        new Thread(
            () -> {
              try {
                while (!testState.stopRunning.get()) {
                  int numQueryThreads =
                      random().nextInt(MAX_NUM_OF_CONCURRENT_QUERY_REQUESTS_PER_ITERATION) + 1;
                  Thread[] queryThreads = createQueryThreads(numQueryThreads);
                  for (int j = 0; j < numQueryThreads; j++) {
                    queryThreads[j].start();
                  }
                  for (int j = 0; j < numQueryThreads; j++) {
                    queryThreads[j].join();
                  }
                  Thread.sleep(DELAY_MS_BETWEEN_EACH_QUERY_ITERATION);
                }
              } catch (Exception ex) {
                testState.queryErrors.add(ex.getMessage());
              }
            });
    testState.includeThread(t);
  }

  /** Creates {@code numQueryThreads} threads with each querying all docs "*:*" */
  private Thread[] createQueryThreads(int numQueryThreads) {
    log.info("numQueryThreads={}", numQueryThreads);
    Thread[] queryThreads = new Thread[numQueryThreads];
    for (int i = 0; i < numQueryThreads && !testState.stopRunning.get(); i++) {
      queryThreads[i] =
          new Thread(
              () -> {
                try {
                  /*
                   * Don't have a way to ensure freshness of results yet. When we add something for
                   * query freshness later we may use that here.
                   *
                   * <p>{@link SolrProcessTracker#corePullTracker} cannot help in concurrent query
                   * scenarios since there is no one-to-one guarantee between query and an async
                   * pull.
                   */
                  cluster
                      .getSolrClient()
                      .query(COLLECTION_NAME, new ModifiableSolrParams().set("q", "*:*"));
                } catch (Exception ex) {
                  StringWriter sw = new StringWriter();
                  ex.printStackTrace(new PrintWriter(sw));
                  testState.queryErrors.add(sw.toString());
                }
              });
    }
    return queryThreads;
  }

  /**
   * Adds a thread to test, that goes over iterations until the test is stopped {@link
   * TestState#stopRunning}. In each iteration it failovers to new leader by calling {@link
   * #failOver()}. It waits for {@link #DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION} between each
   * iteration.
   */
  private void includeFailovers() {
    Thread t =
        new Thread(
            () -> {
              try {
                while (!testState.stopRunning.get()) {
                  failOver();
                  Thread.sleep(DELAY_MS_BETWEEN_EACH_FAILOVER_ITERATION);
                }
              } catch (Exception ex) {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                testState.failoverError = sw.toString();
              }
            });
    testState.includeThread(t);
  }

  /**
   * Kills the current leader and waits for the new leader to be selected and then brings back up
   * the killed leader as a follower replica. Before bringing back up the replica it randomly
   * decides to delete its core directory.
   */
  private void failOver() throws Exception {
    DocCollection collection = getCollection();
    Replica leaderReplicaBeforeSwitch = collection.getLeader(SHARD_NAME);
    final String leaderReplicaNameBeforeSwitch = leaderReplicaBeforeSwitch.getName();
    JettySolrRunner shardLeaderSolrRunnerBeforeSwitch =
        cluster.getReplicaJetty(leaderReplicaBeforeSwitch);
    File leaderIndexDirBeforeSwitch =
        new File(
            shardLeaderSolrRunnerBeforeSwitch.getCoreContainer().getCoreRootDirectory()
                + "/"
                + leaderReplicaBeforeSwitch.getCoreName());

    shardLeaderSolrRunnerBeforeSwitch.stop();
    cluster.waitForJettyToStop(shardLeaderSolrRunnerBeforeSwitch);
    waitForState(
        "Timed out waiting for new replica to become leader",
        COLLECTION_NAME,
        (liveNodes, collectionState) -> {
          Slice slice = collectionState.getSlice(SHARD_NAME);
          if (slice.getLeader() == null) {
            return false;
          }
          return !slice.getLeader().getName().equals(leaderReplicaNameBeforeSwitch);
        });

    if (random().nextBoolean()) {
      PathUtils.deleteDirectory(leaderIndexDirBeforeSwitch.toPath());
    }

    shardLeaderSolrRunnerBeforeSwitch.start();
    cluster.waitForNode(shardLeaderSolrRunnerBeforeSwitch, -1);

    waitForState(
        "Timed out waiting for restarted replica to become active",
        COLLECTION_NAME,
        (liveNodes, collectionState) -> {
          Slice slice = collectionState.getSlice(SHARD_NAME);
          return slice.getReplica(leaderReplicaNameBeforeSwitch).getState() == Replica.State.ACTIVE;
        });

    setupSolrProcess(shardLeaderSolrRunnerBeforeSwitch);
  }

  /**
   * Goes over all the lives of a node(node gets a new life on restart) and then goes over each
   * core's concurrency stages in each life. Logs the concurrency stages in the order they occurred
   * and then analyze those stages to make sure no critical section was breached.
   */
  private void analyzeCoreConcurrencyStagesForBreaches() {
    // Goes over each node
    for (Map.Entry<String, List<SolrProcessTracker>> nodeTracker :
        testState.solrNodesTracker.entrySet()) {
      String nodeName = nodeTracker.getKey();
      int lifeCountForNode = nodeTracker.getValue().size();
      // Goes over each life of a node
      for (int i = 0; i < lifeCountForNode; i++) {
        ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStageTracker =
            nodeTracker.getValue().get(i).coreConcurrencyStageTracker;
        if (coreConcurrencyStageTracker.isEmpty()) {
          log.info(
              "life {}/{} of node {} is empty", // node has no life
              i + 1,
              lifeCountForNode,
              nodeName);
        } else {
          // Goes over each core
          for (Map.Entry<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesEntry :
              coreConcurrencyStageTracker.entrySet()) {
            String coreName = coreConcurrencyStagesEntry.getKey();
            List<String> coreConcurrencyStages =
                new ArrayList<>(coreConcurrencyStagesEntry.getValue());
            // Log line is truncated beyond certain length, therefore, printing them in batches
            final int batchSize = 200;

            if (coreConcurrencyStages.isEmpty()) {
              final String message =
                  "No batches for core "
                      + coreName
                      + " of life "
                      + (i + 1)
                      + "/"
                      + lifeCountForNode
                      + " of node "
                      + nodeName;
              if (log.isInfoEnabled()) {
                log.info(message);
              }
            } else {
              int totalBatches = (coreConcurrencyStages.size() + batchSize - 1) / batchSize;
              int batchNumber = 1;
              for (int startIndex = 0;
                  startIndex < coreConcurrencyStages.size();
                  startIndex += batchSize) {
                int toIndex = Math.min(startIndex + batchSize, coreConcurrencyStages.size());
                List<String> sublist = coreConcurrencyStages.subList(startIndex, toIndex);
                // Building message separately because precommit check was acting silly
                final String message =
                    "batch "
                        + batchNumber++
                        + "/"
                        + totalBatches
                        + " of core "
                        + coreName
                        + " of life "
                        + (i + 1)
                        + "/"
                        + lifeCountForNode
                        + " of node "
                        + nodeName
                        + ".\n"
                        + sublist;
                if (log.isInfoEnabled()) {
                  log.info(message);
                }
              }
            }
            analyzeCoreConcurrencyStagesForBreaches(coreName, coreConcurrencyStages);
          }
        }
      }
    }
  }

  /**
   * Analyze core's concurrency stages to make sure no critical section was breached. Detail of
   * those critical sections can be found in {@link MetadataCacheManager}.
   */
  private void analyzeCoreConcurrencyStagesForBreaches(
      String coreName, List<String> coreConcurrencyStages) {

    int activePullers = 0; // number of threads that have started pulling and not finished
    int activeIndexers = 0; // number of threads that have started indexing and not finished
    int activePushers = 0; // number of threads that are actively pushing at any given time
    for (String s : coreConcurrencyStages) {
      String[] parts = s.split("\\.");
      MetadataCacheManager.ZeroCoreStage currentStage =
          MetadataCacheManager.ZeroCoreStage.valueOf(parts[1]);

      if (currentStage == MetadataCacheManager.ZeroCoreStage.PULL_STARTED) {
        activePullers++;
      } else if (currentStage == MetadataCacheManager.ZeroCoreStage.PULL_SUCCEEDED
          || currentStage == MetadataCacheManager.ZeroCoreStage.PULL_FAILED
          || currentStage == ZeroCoreStage.PULL_FAILED_WITH_CORRUPTION) {
        activePullers--;
      } else if (currentStage == MetadataCacheManager.ZeroCoreStage.LOCAL_INDEXING_STARTED) {
        activeIndexers++;
      } else if (currentStage == MetadataCacheManager.ZeroCoreStage.PUSH_STARTED) {
        activePushers++;
      } else if (currentStage == MetadataCacheManager.ZeroCoreStage.PUSH_FINISHED) {
        activePushers--;
      } else if (currentStage == MetadataCacheManager.ZeroCoreStage.INDEXING_BATCH_FINISHED) {
        activeIndexers--;
      }

      // making sure no other activity (including another pull) takes place during pull
      assertFalse(
          "Pull and indexing are interleaved, coreName=" + coreName + " currentStage=" + s,
          activePullers > 1 || (activePullers > 0 && (activeIndexers > 0 || activePushers > 0)));

      // making sure a push to Zero store is not disrupted by another push
      assertFalse(
          "Zero store push breached by another Zero store push, coreName="
              + coreName
              + " currentStage="
              + s,
          activePushers > 1);
    }
  }

  /** Adds a new replica. */
  private Replica addReplica() throws Exception {
    List<String> existingReplicas =
        getCollection().getSlice(SHARD_NAME).getReplicas().stream()
            .map(Replica::getName)
            .collect(Collectors.toList());
    // add another replica
    assertTrue(
        CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, SHARD_NAME, Replica.Type.ZERO)
            .process(cluster.getSolrClient())
            .isSuccess());
    // Verify that new replica is created
    waitForState(
        "Timed-out waiting for new replica to be created",
        COLLECTION_NAME,
        clusterShape(1, existingReplicas.size() + 1));

    Replica newReplica = null;

    for (Replica r : getCollection().getSlice(SHARD_NAME).getReplicas()) {
      if (!existingReplicas.contains(r.getName())) {
        newReplica = r;
        break;
      }
    }

    assertNotNull("Could not find new replica", newReplica);

    return newReplica;
  }

  /**
   * Directly query a new {@code replica} and verifies that the empty replica is correctly hydrated
   * from the Zero store with all the indexed docs (after accounting for deletions).
   */
  private void queryNewReplicaAndVerifyAllDocsFound(Replica replica) throws Exception {
    try (SolrClient replicaDirectClient =
        getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName())) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*").set("distrib", "false").set("rows", testState.numDocsIndexed.get());
      CountDownLatch latch = new CountDownLatch(1);
      Map<String, CountDownLatch> corePullTracker =
          testState.getCorePullTracker(replica.getNodeName());
      corePullTracker.put(replica.getCoreName(), latch);
      primingPullQuery(replicaDirectClient, params);

      assertTrue(latch.await(120, TimeUnit.SECONDS));

      QueryResponse resp = replicaDirectClient.query(params);
      List<String> docs =
          resp.getResults().stream()
              .map(r -> (String) r.getFieldValue("id"))
              .collect(Collectors.toList());
      assertEquals(
          "we did not ask for all the docs found", resp.getResults().getNumFound(), docs.size());
      docs.sort(Comparator.comparingInt(Integer::parseInt));
      List<String> docsExpected = new ArrayList<>();
      for (int i = 1; i <= testState.numDocsIndexed.get(); i++) {
        String docId = Integer.toString(i);
        if (!testState.idsDeleted.contains(docId)) {
          docsExpected.add(docId);
        }
      }
      if (log.isInfoEnabled()) {
        log.info("numDocsFound={} docsFound={}", docs.size(), docs);
      }
      assertEquals(
          "wrong docs",
          docsExpected.size() + docsExpected.toString(),
          docs.size() + docs.toString());
    }
  }

  /** Setup all the nodes for test. */
  private void setupSolrNodesForTest() {
    for (JettySolrRunner solrProcess : cluster.getJettySolrRunners()) {
      setupSolrProcess(solrProcess);
    }
  }

  /** Setup solr process for test(process is one life of a node, restarts starts a new life). */
  private void setupSolrProcess(JettySolrRunner solrProcess) {
    Map<String, CountDownLatch> corePullTracker = configureTestZeroProcessForNode(solrProcess);
    ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesTracker =
        new ConcurrentHashMap<>();
    configureTestMetadataCacheManagerForProcess(solrProcess, coreConcurrencyStagesTracker);

    SolrProcessTracker processTracker =
        new SolrProcessTracker(corePullTracker, coreConcurrencyStagesTracker);
    List<SolrProcessTracker> nodeTracker =
        testState.solrNodesTracker.computeIfAbsent(
            solrProcess.getNodeName(), k -> new ArrayList<>());
    nodeTracker.add(processTracker);
  }

  private DocCollection getCollection() {
    return cluster.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME);
  }

  /**
   * Setup {@link MetadataCacheManager} for the solr process so we can accumulate concurrency stages
   * a core goes through during test.
   */
  private void configureTestMetadataCacheManagerForProcess(
      JettySolrRunner solrProcess,
      ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStagesMap) {
    ZeroStoreManager manager = solrProcess.getCoreContainer().getZeroStoreManager();
    MetadataCacheManager metadataCacheManager =
        new MetadataCacheManager(manager.getConfig(), solrProcess.getCoreContainer()) {
          @Override
          public void recordState(SolrCore core, ZeroCoreStage stage) {
            super.recordState(core, stage);
            ConcurrentLinkedQueue<String> coreConcurrencyStages =
                coreConcurrencyStagesMap.computeIfAbsent(
                    coreName, k -> new ConcurrentLinkedQueue<>());
            coreConcurrencyStages.add(Thread.currentThread().getName() + "." + stage.name());
          }
        };
    setupTestMetadataCacheManager(metadataCacheManager, solrProcess);
  }

  /** Manages state for each test from start to end. */
  private static class TestState {
    /** Threads included in the test (indexing, queries and failovers). */
    private final List<Thread> includedThreads = new ArrayList<>();

    /** Indicator when to stop. It is set to true when either indexing is done or interrupted. */
    private final AtomicBoolean stopRunning = new AtomicBoolean(false);

    /** Used to provide unique id to each indexing doc. */
    private final AtomicInteger docIdGenerator = new AtomicInteger(0);

    /**
     * At any given moment how many minimum number of docs that have been indexed (it does not
     * account for deletion)
     */
    private final AtomicInteger numDocsIndexed = new AtomicInteger(0);

    /** Set of ids from indexed docs that can be deleted. */
    private final ConcurrentLinkedQueue<List<String>> idBatchesToDelete =
        new ConcurrentLinkedQueue<>();

    /** Ids that have been deleted. */
    private final ConcurrentLinkedQueue<String> idsDeleted = new ConcurrentLinkedQueue<>();

    /**
     * Error setting up indexing or those encountered by indexing on the last attempt {@link
     * #MAX_NUM_OF_ATTEMPTS_PER_INDEXING_REQUEST} of each batch.
     */
    private final ConcurrentLinkedQueue<String> indexingErrors = new ConcurrentLinkedQueue<>();

    /** Error setting up queries or those encountered by queries. */
    private final ConcurrentLinkedQueue<String> queryErrors = new ConcurrentLinkedQueue<>();

    /** Error encountered when failing over to a new leader. */
    private String failoverError = null;

    /**
     * Tracks the cores' pull and concurrency stage information for each life of a node (node gets a
     * new life on restart). Key is the node name.
     */
    private final Map<String, List<SolrProcessTracker>> solrNodesTracker = new HashMap<>();

    /** Gets the core pull tracker for current life of the node. */
    private Map<String, CountDownLatch> getCorePullTracker(String nodeName) {
      List<SolrProcessTracker> allLives = solrNodesTracker.get(nodeName);
      return allLives.get(allLives.size() - 1).corePullTracker;
    }

    /** Includes a thread into test. */
    private void includeThread(Thread t) {
      includedThreads.add(t);
    }

    /** Starts all the included threads. */
    private void startIncludedThreads() {
      for (Thread t : includedThreads) {
        t.start();
      }
    }

    /** Wait for all the included threads to stop. */
    private void waitForThreadsToStop() throws Exception {
      for (Thread t : includedThreads) {
        t.join();
      }
      if (log.isInfoEnabled()) {
        log.info(
            "docIdGenerator={} numDocsIndexed={} numDocsDeleted={}",
            docIdGenerator.get(),
            numDocsIndexed.get(),
            idsDeleted.size());
      }
    }

    /** Check if any error was encountered during the test. */
    private void checkErrors() {
      assertTrue(
          "indexingErrors=\n"
              + indexingErrors
              + "\n"
              + "queryErrors=\n"
              + queryErrors
              + "\n"
              + "failoverError=\n"
              + failoverError
              + "\n",
          indexingErrors.isEmpty() && queryErrors.isEmpty() && failoverError == null);
    }
  }

  /** Track cores' pull and concurrency stage information for one life of a node */
  private static class SolrProcessTracker {
    /** Per core pull tracker. Key is the core name. */
    private final Map<String, CountDownLatch> corePullTracker;

    /**
     * Per core concurrency stage tracker. Key is the core name.
     *
     * <p>For now we are only using single replica per node therefore it will only be single core
     * but it should be able to handle multiple replicas per node, if test chooses to setup that
     * way.
     */
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>>
        coreConcurrencyStageTracker;

    private SolrProcessTracker(
        Map<String, CountDownLatch> corePullTracker,
        ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> coreConcurrencyStageTracker) {
      this.corePullTracker = corePullTracker;
      this.coreConcurrencyStageTracker = coreConcurrencyStageTracker;
    }
  }
}
