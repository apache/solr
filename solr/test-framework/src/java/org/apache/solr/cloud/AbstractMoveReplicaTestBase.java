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

package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.IdUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMoveReplicaTestBase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // used by MoveReplicaHDFSTest
  protected boolean inPlaceMove = true;
  protected boolean isCollectionApiDistributed = false;

  @BeforeClass
  public static void before() {
    // set this longer than the default 4000. We know we're doing things that can
    // stress leader election, _but_ that's not what we're trying to test here, so
    // we want the tolerance on this to be loose. (If we want to tighten up leader
    // election, that's for a different test suite to do).

    // OTOH, if even at this tolerance we still get errors, then maybe the replica
    // move is _really_ messing something up, in which case we want to know it's
    // not just because we're being impatient!
    System.setProperty("zkReaderGetLeaderRetryTimeoutMs", "12000");
  }

  @AfterClass
  public static void after() {
    System.clearProperty("zkReaderGetLeaderRetryTimeoutMs");
  }

  protected String getConfigSet() {
    return "cloud-dynamic";
  }

  @Before
  public void beforeTest() throws Exception {
    inPlaceMove = true;

    configureCluster(4)
        .addConfig("conf1", configset(getConfigSet()))
        .addConfig("conf2", configset(getConfigSet()))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    // If Collection API is distributed let's not wait for Overseer.
    isCollectionApiDistributed =
        new CollectionAdminRequest.RequestApiDistributedProcessing()
            .process(cluster.getSolrClient())
            .getIsCollectionApiDistributed();
    if (isCollectionApiDistributed) {
      return;
    }

    NamedList<Object> overSeerStatus =
        cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    JettySolrRunner overseerJetty = null;
    String overseerLeader = (String) overSeerStatus.get("leader");
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerJetty = jetty;
        break;
      }
    }
    if (overseerJetty == null) {
      fail("no overseer leader!");
    }
  }

  @After
  public void afterTest() throws Exception {
    try {
      shutdownCluster();
    } finally {
      super.tearDown();
    }
  }

  private interface IndexFunction {
    int accept(int docId) throws IOException, SolrServerException;
  }

  private static boolean index(IndexFunction func, int docId, long retryDuration)
      throws InterruptedException, IOException, SolrServerException {
    try {
      func.accept(docId);
      return true;
    } catch (CloudSolrClient.RouteException e) {
      // we know the replica just moved back, so retry
    }
    long start = System.nanoTime();
    boolean success = false;
    do {
      Thread.sleep(100);
      try {
        func.accept(docId);
        success = true;
      } catch (CloudSolrClient.RouteException e) {
        // we know the replica just moved back, so retry
      }
    } while (!success && System.nanoTime() - start < retryDuration);
    return success;
  }

  private static int validateNumFound(IndexFunction func, int expectCount, long retryDuration)
      throws InterruptedException, SolrServerException, IOException {
    int actual = -1;
    try {
      actual = func.accept(-1);
      if (retryDuration == 0 || actual == expectCount) {
        return actual;
      }
    } catch (CloudSolrClient.RouteException e) {
      // we know the replica just moved back, so retry
    }
    long start = System.nanoTime();
    do {
      Thread.sleep(100);
      try {
        actual = func.accept(-1);
      } catch (CloudSolrClient.RouteException e) {
        // we know the replica just moved back, so retry
      }
    } while (actual != expectCount && System.nanoTime() - start < retryDuration);
    return actual;
  }

  @SuppressWarnings("try")
  protected void test(boolean inPlaceMove) throws Exception {
    this.inPlaceMove = inPlaceMove;
    String coll = getTestClass().getSimpleName() + "_coll_" + inPlaceMove;
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }
    int REPLICATION = random().nextInt(2) + 1;
    int N_SHARDS = random().nextInt(3) + 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();

    // random create tlog or pull type replicas with nrt
    int[] replicaTypeCounts = new int[] {1, 0, 0};
    for (int i = 1; i < REPLICATION; i++) {
      replicaTypeCounts[random().nextInt(replicaTypeCounts.length)]++;
    }
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(
            coll,
            "conf1",
            N_SHARDS,
            replicaTypeCounts[0],
            replicaTypeCounts[1],
            replicaTypeCounts[2]);
    create.setPerReplicaState(random().nextBoolean());
    cloudClient.request(create);

    Replica replica = getRandomReplica(coll, cloudClient);
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node)) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);
    String shardId = null;
    for (Slice slice : cloudClient.getClusterState().getCollection(coll).getSlices()) {
      if (slice.getReplicas().contains(replica)) {
        shardId = slice.getName();
      }
    }

    int sourceNumCores =
        getNumOfCores(cloudClient, replica.getNodeName(), coll, replica.getType().name());
    int targetNumCores = getNumOfCores(cloudClient, targetNode, coll, replica.getType().name());

    CollectionAdminRequest.MoveReplica moveReplica =
        createMoveReplicaRequest(coll, replica, targetNode);
    moveReplica.setInPlaceMove(inPlaceMove);
    String asyncId = IdUtils.randomId();

    ExecutorService background = ExecutorUtil.newMDCAwareCachedThreadPool("indexingPool");
    AtomicBoolean stop = new AtomicBoolean(false);
    try (Closeable execClose = () -> ExecutorUtil.shutdownAndAwaitTermination(background);
        Closeable stopBackground = () -> stop.set(true)) {

      AtomicBoolean pause = new AtomicBoolean(false);
      AtomicInteger expectCount = new AtomicInteger(-1);
      CountDownLatch[] cdl = new CountDownLatch[] {new CountDownLatch(2), new CountDownLatch(2)};

      Random indexRandom = new Random(random().nextLong());
      IndexFunction asyncCommit =
          (ignored) -> {
            cloudClient.commit(coll, false, false);
            return 0;
          };
      IndexFunction syncCommit =
          (ignored) -> {
            cloudClient.commit(coll);
            return 0;
          };
      IndexFunction indexDoc =
          (docId) -> {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", docId);
            cloudClient.add(coll, doc);
            return docId;
          };
      long retryDuration = TimeUnit.SECONDS.toNanos(10);
      Future<Object> indexing =
          background.submit(
              () -> {
                int docId = 0;
                try {
                  while (!stop.get()) {
                    assertTrue(index(indexDoc, docId++, retryDuration));
                    if (indexRandom.nextInt(10) == 0) {
                      assertTrue(index(asyncCommit, -1, retryDuration));
                    }
                    if (pause.get()) {
                      pause.set(false);
                      expectCount.set(docId);
                      assertTrue(index(syncCommit, -1, retryDuration));
                      cdl[0].countDown(); // commit complete
                      cdl[1].countDown(); // wait before proceeding
                      cdl[1].await();
                    }
                  }
                } catch (Throwable t) {
                  cdl[0].countDown();
                  cdl[1].countDown();
                  throw t;
                }
                expectCount.set(docId);
                assertTrue(index(syncCommit, -1, retryDuration));
                return null;
              });

      Thread.sleep(1000); // allow some docs in the index

      Future<Object> querying =
          background.submit(
              () -> {
                while (!stop.get()) {
                  try {
                    assertTrue(
                        cloudClient.query(coll, new SolrQuery("*:*")).getResults().getNumFound()
                            > 0);
                  } catch (Exception e) {
                    // we know there might be exceptions, but this is just to generate load.
                  }
                  Thread.sleep(100);
                }
                return null;
              });

      moveReplica.processAsync(asyncId, cloudClient);
      CollectionAdminRequest.RequestStatus requestStatus =
          CollectionAdminRequest.requestStatus(asyncId);
      // wait for async request success
      boolean success = false;
      for (int i = 0; i < 200; i++) {
        CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
        if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
          success = true;
          break;
        }
        assertNotSame(rsp.getRequestStatus(), RequestStatusState.FAILED);
        Thread.sleep(500);
      }
      assertTrue(success);
      assertEquals(
          "should be one less core on the source node!",
          sourceNumCores - 1,
          getNumOfCores(cloudClient, replica.getNodeName(), coll, replica.getType().name()));
      assertEquals(
          "should be one more core on target node!",
          targetNumCores + 1,
          getNumOfCores(cloudClient, targetNode, coll, replica.getType().name()));
      // wait for recovery
      boolean recovered = false;
      for (int i = 0; i < 300; i++) {
        DocCollection collState = getCollectionState(coll);
        log.debug("###### {}", collState);
        Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
        boolean allActive = true;
        boolean hasLeaders = true;
        if (replicas != null && !replicas.isEmpty()) {
          for (Replica r : replicas) {
            if (!r.getNodeName().equals(targetNode)) {
              continue;
            }
            if (!r.isActive(Collections.singleton(targetNode))) {
              log.info("Not active: {}", r);
              allActive = false;
            }
          }
        } else {
          allActive = false;
        }
        for (Slice slice : collState.getSlices()) {
          if (slice.getLeader() == null) {
            hasLeaders = false;
          }
        }
        if (allActive && hasLeaders) {
          // check the number of active replicas
          assertEquals("total number of replicas", REPLICATION, replicas.size());
          recovered = true;
          break;
        } else {
          log.info("--- waiting, allActive={}, hasLeaders={}", allActive, hasLeaders);
          Thread.sleep(1000);
        }
      }
      assertTrue("replica never fully recovered", recovered);

      pause.set(true);
      cdl[0].countDown();
      cdl[0].await();
      IndexFunction getNumFound =
          (ignored) -> {
            return (int) cloudClient.query(coll, new SolrQuery("*:*")).getResults().getNumFound();
          };
      boolean hasNonNrt = replicaTypeCounts[1] > 0 || replicaTypeCounts[2] > 0;
      long numFoundRetryDuration = hasNonNrt ? retryDuration : 0;
      int expect = expectCount.get();
      if (expect == -1) {
        // the only way this happens is if an exception is thrown in indexing, so get the original
        // exception instead of a misleading numFound assertion error
        indexing.get(2, TimeUnit.SECONDS);
      }
      int actualNumFound = validateNumFound(getNumFound, expect, numFoundRetryDuration);
      assertEquals("midpoint count wrong", expect, actualNumFound);
      cdl[1].countDown();

      moveReplica = createMoveReplicaRequest(coll, replica, targetNode, shardId);
      moveReplica.setInPlaceMove(inPlaceMove);
      moveReplica.process(cloudClient);
      checkNumOfCores(cloudClient, replica.getNodeName(), coll, sourceNumCores);
      // wait for recovery
      recovered = false;
      for (int i = 0; i < 300; i++) {
        DocCollection collState = getCollectionState(coll);
        log.debug("###### {}", collState);
        Collection<Replica> replicas = collState.getSlice(shardId).getReplicas();
        boolean allActive = true;
        boolean hasLeaders = true;
        if (replicas != null && !replicas.isEmpty()) {
          for (Replica r : replicas) {
            if (!r.getNodeName().equals(replica.getNodeName())) {
              continue;
            }
            if (!r.isActive(Collections.singleton(replica.getNodeName()))) {
              log.info("Not active yet: {}", r);
              allActive = false;
            }
          }
        } else {
          allActive = false;
        }
        for (Slice slice : collState.getSlices()) {
          if (slice.getLeader() == null) {
            hasLeaders = false;
          }
        }
        if (allActive && hasLeaders) {
          assertEquals("total number of replicas", REPLICATION, replicas.size());
          recovered = true;
          break;
        } else {
          Thread.sleep(1000);
        }
      }
      assertTrue("replica never fully recovered", recovered);

      stop.set(true);
      indexing.get();
      expect = expectCount.get();
      actualNumFound = validateNumFound(getNumFound, expect, numFoundRetryDuration);
      assertEquals("final count wrong", expect, actualNumFound);
      querying.get();
    }
  }

  @Test
  public void testFailedMove() throws Exception {
    String coll = getTestClass().getSimpleName() + "_failed_coll_" + inPlaceMove;
    int REPLICATION = 2;

    CloudSolrClient cloudClient = cluster.getSolrClient();

    // random create tlog or pull type replicas with nrt
    boolean isTlog = random().nextBoolean();
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(
            coll, "conf1", 2, 1, isTlog ? 1 : 0, !isTlog ? 1 : 0);
    cloudClient.request(create);

    addDocs(coll, 100);

    NamedList<Object> overSeerStatus =
        cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");

    // don't kill overseer in this test
    Replica replica;
    int count = 10;
    do {
      replica = getRandomReplica(coll, cloudClient);
    } while (!replica.getNodeName().equals(overseerLeader) && count-- > 0);
    assertNotNull("could not find non-overseer replica???", replica);
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node)
          && (isCollectionApiDistributed || !overseerLeader.equals(node))) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);
    CollectionAdminRequest.MoveReplica moveReplica =
        createMoveReplicaRequest(coll, replica, targetNode);
    moveReplica.setInPlaceMove(inPlaceMove);
    // start moving
    String asyncId = IdUtils.randomId();
    moveReplica.processAsync(asyncId, cloudClient);
    // shut down target node
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      if (cluster.getJettySolrRunner(i).getNodeName().equals(targetNode)) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }
    CollectionAdminRequest.RequestStatus requestStatus =
        CollectionAdminRequest.requestStatus(asyncId);
    // wait for async request success
    boolean success = true;
    for (int i = 0; i < 200; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      assertNotSame(
          rsp.getRequestStatus().toString(), rsp.getRequestStatus(), RequestStatusState.COMPLETED);
      if (rsp.getRequestStatus() == RequestStatusState.FAILED) {
        success = false;
        break;
      }
      Thread.sleep(500);
    }
    assertFalse(success);

    if (log.isInfoEnabled()) {
      log.info(
          "--- current collection state: {}", cloudClient.getClusterState().getCollection(coll));
    }
    assertEquals(
        100, cluster.getSolrClient().query(coll, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(
      String coll, Replica replica, String targetNode, String shardId) {
    return new CollectionAdminRequest.MoveReplica(coll, shardId, targetNode, replica.getNodeName());
  }

  private CollectionAdminRequest.MoveReplica createMoveReplicaRequest(
      String coll, Replica replica, String targetNode) {
    return new CollectionAdminRequest.MoveReplica(coll, replica.getName(), targetNode);
  }

  private Replica getRandomReplica(String coll, CloudSolrClient cloudClient) throws IOException {
    List<Replica> replicas = cloudClient.getClusterState().getCollection(coll).getReplicas();
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  private void checkNumOfCores(
      CloudSolrClient cloudClient, String nodeName, String collectionName, int expectedCores)
      throws IOException, SolrServerException {
    assertEquals(
        nodeName + " does not have expected number of cores",
        expectedCores,
        getNumOfCores(cloudClient, nodeName, collectionName));
  }

  private int getNumOfCores(CloudSolrClient cloudClient, String nodeName, String collectionName)
      throws IOException, SolrServerException {
    return getNumOfCores(cloudClient, nodeName, collectionName, null);
  }

  private int getNumOfCores(
      CloudSolrClient cloudClient, String nodeName, String collectionName, String replicaType)
      throws IOException, SolrServerException {
    try (SolrClient coreclient =
        getHttpSolrClient(ZkStateReader.from(cloudClient).getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      if (status.getCoreStatus().size() == 0) {
        return 0;
      }
      if (collectionName == null && replicaType == null) {
        return status.getCoreStatus().size();
      }
      // filter size by collection name
      int size = 0;
      for (Map.Entry<String, NamedList<Object>> stringNamedListEntry : status.getCoreStatus()) {
        if (collectionName != null) {
          String coll =
              (String) stringNamedListEntry.getValue().findRecursive("cloud", "collection");
          if (!collectionName.equals(coll)) {
            continue;
          }
        }
        if (replicaType != null) {
          String type =
              (String) stringNamedListEntry.getValue().findRecursive("cloud", "replicaType");
          if (!replicaType.equals(type)) {
            continue;
          }
        }
        size++;
      }
      return size;
    }
  }

  protected void addDocs(String collection, int numDocs) throws Exception {
    SolrClient solrClient = cluster.getSolrClient();
    for (int docId = 1; docId <= numDocs; docId++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docId);
      solrClient.add(collection, doc);
    }
    solrClient.commit(collection);
    Thread.sleep(5000);
  }
}
