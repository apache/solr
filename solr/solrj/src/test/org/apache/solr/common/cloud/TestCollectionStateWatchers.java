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

package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** @see TestDocCollectionWatcher */
@LuceneTestCase.Nightly
public class TestCollectionStateWatchers extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int CLUSTER_SIZE = 4;

  private static final int MAX_WAIT_TIMEOUT = 15; // seconds, only use for await -- NO SLEEP!!!

  @Before
  public void prepareCluster() throws Exception {
    configureCluster(CLUSTER_SIZE)
      .addConfig("config", SolrTestUtil.getFile("solrj/solr/collection1/conf").toPath())
      .configure();
  }
  
  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  private Future<Boolean> waitInBackground(String collection, long timeout, TimeUnit unit,
                                                  CollectionStatePredicate predicate) {
    return getTestExecutor().submit(() -> {
      try {
        cluster.getSolrClient().waitForState(collection, timeout, unit, predicate);
      } catch (InterruptedException | TimeoutException e) {
        log.error("Timed out waiting to see condition {} for collection {}", predicate, collection, e);
        return Boolean.FALSE;
      }
      return Boolean.TRUE;
    });
  }

  /**
   * Review finding #4 (architect re-verification, race "c"): the scoped state-plane watch must converge to
   * "armed iff this node is locally interested" even when registerCore/unregisterCore for the SAME
   * collection race across threads. The arm/release is not atomic with the collectionWatches membership
   * decision, so a self-correcting recheck in releaseLocalInterest reconciles a lost update. Assert the
   * quiescent invariant collectionWatches.containsKey(C) == statePlaneWatched.contains(C).
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testScopedWatchTracksInterestUnderConcurrentChurn() throws Exception {
    final String collection = "concurrentChurn";
    CollectionAdminRequest.createCollection(collection, "config", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    final ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    final java.lang.reflect.Field f = ZkStateReader.class.getDeclaredField("statePlaneWatched");
    f.setAccessible(true);
    final java.util.Set<String> statePlaneWatched = (java.util.Set<String>) f.get(reader);

    final int threads = 8;
    final int iters = 150;
    final CountDownLatch start = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicReference<Throwable> err =
        new java.util.concurrent.atomic.AtomicReference<>();
    final Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      final String core = "churnCore_" + t; // distinct per thread; the race is on the shared collection
      workers[t] = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < iters; i++) {
            reader.registerCore(collection, core);
            reader.unregisterCore(collection, core);
          }
        } catch (Throwable th) {
          err.compareAndSet(null, th);
        }
      });
      workers[t].start();
    }
    start.countDown();
    for (Thread w : workers) {
      w.join();
    }
    assertNull("worker thread threw", err.get());

    // Quiescent (all threads joined): every register was balanced by its unregister, so the collection is
    // not locally interesting and the scoped watch must have been reconciled away — no interested-but-
    // unwatched stall, no armed-but-uninterested leak.
    assertEquals(
        "scoped watch must be armed iff the collection is locally watched",
        reader.watched(collection),
        statePlaneWatched.contains(collection));
    assertFalse("balanced churn must leave no local interest", reader.watched(collection));

    // A lone register must leave it interested AND armed (the race's failure mode was interested-but-unwatched).
    reader.registerCore(collection, "finalCore");
    assertTrue(reader.watched(collection));
    assertTrue("a hosted core must hold an armed scoped watch", statePlaneWatched.contains(collection));

    reader.unregisterCore(collection, "finalCore");
    assertFalse(reader.watched(collection));
    assertFalse("releasing the last interest must release the scoped watch", statePlaneWatched.contains(collection));

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
  }

  @Test
  public void testCollectionWatchWithShutdownOfActiveNode() throws Exception {
    doTestCollectionWatchWithNodeShutdown(false);
  }
  
  @Test
  public void testCollectionWatchWithShutdownOfUnusedNode() throws Exception {
    doTestCollectionWatchWithNodeShutdown(true);
  }
  
  private void doTestCollectionWatchWithNodeShutdown(final boolean shutdownUnusedNode)
    throws Exception {

    CloudHttp2SolrClient client = cluster.getSolrClient();

    // note: one node in our cluster is unsed by collection
    CollectionAdminRequest.createCollection("testcollection", "config", CLUSTER_SIZE, 1)
      .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("testcollection", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                        (n, c) -> DocCollection.isFullyActive(n, c, CLUSTER_SIZE, 1));

    final JettySolrRunner extraJetty = cluster.startJettySolrRunner();
    final JettySolrRunner jettyToShutdown
      = shutdownUnusedNode ? extraJetty : cluster.getRandomJetty(random(), extraJetty);
    final int expectedNodesWithActiveReplicas = CLUSTER_SIZE - (shutdownUnusedNode ? 0 : 1);
    
    cluster.waitForAllNodes(MAX_WAIT_TIMEOUT);
    
    // shutdown a node and check that we get notified about the change
    final CountDownLatch latch = new CountDownLatch(1);
    client.registerCollectionStateWatcher("testcollection", (liveNodes, collectionState) -> {
      int nodesWithActiveReplicas = 0;
      log.info("State changed: {}", collectionState);
      for (Slice slice : collectionState) {
        for (Replica replica : slice) {
          if (replica.isActive(liveNodes))
            nodesWithActiveReplicas++;
        }
      }
      if (liveNodes.size() == CLUSTER_SIZE && expectedNodesWithActiveReplicas == nodesWithActiveReplicas) {
        latch.countDown();
        return true;
      }
      return false;
    });

    cluster.stopJettySolrRunner(jettyToShutdown);
    cluster.waitForJettyToStop(jettyToShutdown);
    
    assertTrue("CollectionStateWatcher was never notified of cluster change",
               latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));

//    await("CollectionStateWatcher wasn't cleared after completion")
//        .atMost(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS)
//        .until(() -> client.getZkStateReader().getStateWatchers("testcollection").isEmpty());
  }

  @Test
  public void testStateWatcherChecksCurrentStateOnRegister() throws Exception {

    CloudHttp2SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("currentstate", "config", 1, 1)
      .processAndWait(client, MAX_WAIT_TIMEOUT);

    final CountDownLatch latch = new CountDownLatch(1);
    client.registerCollectionStateWatcher("currentstate", (n, c) -> {
      latch.countDown();
      return false;
    });

    assertTrue("CollectionStateWatcher isn't called on new registration",
               latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));
    assertEquals("CollectionStateWatcher should be retained",
                 1, client.getZkStateReader().getStateWatchers("currentstate").size());

    final CountDownLatch latch2 = new CountDownLatch(1);
    client.registerCollectionStateWatcher("currentstate", (n, c) -> {
      latch2.countDown();
      return true;
    });

    assertTrue("CollectionStateWatcher isn't called when registering for already-watched collection",
               latch.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));

//    await("CollectionStateWatcher should be removed")
//        .atMost(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS)
//        .until(() -> client.getZkStateReader().getStateWatchers("currentstate").size() == 1);
  }

  @Test
  public void testWaitForStateChecksCurrentState() throws Exception {

    CloudHttp2SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("waitforstate", "config", 1, 1)
      .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("waitforstate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    // several goes, to check that we're not getting delayed state changes
    for (int i = 0; i < 10; i++) {
      try {
        client.waitForState("waitforstate", 1, TimeUnit.SECONDS,
                            (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));
      } catch (TimeoutException e) {
        fail("waitForState should return immediately if the predicate is already satisfied");
      }
    }
  }

  @Test
  public void testCanWaitForNonexistantCollection() throws Exception {

    Future<Boolean> future = waitInBackground("delayed", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                                              (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));

    CollectionAdminRequest.createCollection("delayed", "config", 1, 1)
      .processAndWait(cluster.getSolrClient(), MAX_WAIT_TIMEOUT);

    assertTrue("waitForState was not triggered by collection creation", future.get());

  }

  @Test
  public void testPredicateFailureTimesOut() throws Exception {
    CloudHttp2SolrClient client = cluster.getSolrClient();
    LuceneTestCase.expectThrows(TimeoutException.class, () -> {
      client.waitForState("nosuchcollection", 1, TimeUnit.SECONDS,
                          ((liveNodes, collectionState) -> false));
    });

//    await("Watchers for collection should be removed after timeout")
//        .atMost(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS)
//        .until(() -> client.getZkStateReader().getStateWatchers("nosuchcollection").isEmpty());
  }

  @Test
  public void testWaitForStateWatcherIsRetainedOnPredicateFailure() throws Exception {

    CloudHttp2SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection("falsepredicate", "config", 4, 1)
      .processAndWait(client, MAX_WAIT_TIMEOUT);

    client.waitForState("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                        (n, c) -> DocCollection.isFullyActive(n, c, 4, 1));

    final CountDownLatch firstCall = new CountDownLatch(1);

    // stop a node, then add a watch waiting for all nodes to be back up
    JettySolrRunner node1 = cluster.stopJettySolrRunner(random().nextInt
                                                        (cluster.getJettySolrRunners().size()));
    
    cluster.waitForJettyToStop(node1);

    Future<Boolean> future = waitInBackground("falsepredicate", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                                              (liveNodes, collectionState) -> {
          firstCall.countDown();
          return DocCollection.isFullyActive(liveNodes, collectionState, 4, 1);
        });

    // first, stop another node; the watch should not be fired after this!
    JettySolrRunner node2 = cluster.stopJettySolrRunner(random().nextInt
                                                        (cluster.getJettySolrRunners().size()));

    // now start them both back up
    cluster.startJettySolrRunner(node1);
    assertTrue("CollectionStateWatcher not called",
               firstCall.await(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS));
    cluster.startJettySolrRunner(node2);

    Boolean result = future.get();
    assertTrue("Did not see a fully active cluster", result);
  }

  @Test
  public void testWatcherIsRemovedAfterTimeout() throws Exception {
    CloudHttp2SolrClient client = cluster.getSolrClient();
    assertTrue("There should be no watchers for a non-existent collection!",
               client.getZkStateReader().getStateWatchers("no-such-collection").isEmpty());

    LuceneTestCase.expectThrows(TimeoutException.class, () -> {
      client.waitForState("no-such-collection", 10, TimeUnit.MILLISECONDS,
                          (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));
    });

    // waitForState removes its watcher synchronously in a finally block, so the watch must be gone.
    assertTrue("Watcher for a non-existent collection should be removed after timeout",
               client.getZkStateReader().getStateWatchers("no-such-collection").isEmpty());

    // Stronger invariant: the timed-out watch must not leave a permanent doomed LazyCollectionRef
    // behind. Such a phantom ref (for a collection that never existed) lingers in getCollectionRefs()
    // forever and makes the next overseer's ZkStateWriter.init abort with NoNode -- which leaves a
    // just-killed shard leaderless (root cause of the TestTlogReplica.testKillLeader
    // "Expect new leader" flake).
    assertFalse("waitForState timeout leaked a doomed lazy ref for a non-existent collection",
                client.getZkStateReader().getCollectionRefs().containsKey("no-such-collection"));
  }

  @Test
  public void testDeletionsTriggerWatches() throws Exception {
    CollectionAdminRequest.createCollection("tobedeleted", "config", 1, 1)
      .process(cluster.getSolrClient());
    
    Future<Boolean> future =
        waitInBackground("tobedeleted", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS, (l, c) -> c == null);

    CollectionAdminRequest.deleteCollection("tobedeleted").process(cluster.getSolrClient());

    assertTrue("CollectionStateWatcher not notified of delete call", future.get());
  }
  
  @Test
  public void testLiveNodeChangesTriggerWatches() throws Exception {
    final CloudHttp2SolrClient client = cluster.getSolrClient();
    
    CollectionAdminRequest.createCollection("test_collection", "config", 1, 1).process(client);

    Future<Boolean> future = waitInBackground("test_collection", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                                              (l, c) -> (l.size() == 1 + CLUSTER_SIZE));
    
    JettySolrRunner unusedJetty = cluster.startJettySolrRunner();
    cluster.waitForNode(unusedJetty, 30);

    assertTrue("CollectionStateWatcher not notified of new node", future.get());

//    await("CollectionStateWatcher should be removed").atMost(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS)
//        .until(() -> client.getZkStateReader().getStateWatchers("test_collection").size() == 0);

    future = waitInBackground("test_collection", MAX_WAIT_TIMEOUT, TimeUnit.SECONDS,
                              (l, c) -> (l.size() == CLUSTER_SIZE));

    cluster.stopJettySolrRunner(unusedJetty);

    cluster.waitForJettyToStop(unusedJetty);
    
    assertTrue("CollectionStateWatcher not notified of node lost", future.get());

//    await("CollectionStateWatcher should be removed").atMost(MAX_WAIT_TIMEOUT, TimeUnit.SECONDS)
//        .until(() -> client.getZkStateReader().getStateWatchers("test_collection").size() == 0);
  }
}
