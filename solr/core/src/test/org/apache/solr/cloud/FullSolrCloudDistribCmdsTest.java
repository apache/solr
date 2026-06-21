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

import java.lang.invoke.MethodHandles;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Super basic testing, no shard restarting or anything.
 */
@Slow
@LuceneTestCase.Nightly // MRM TODO: flakey + using testConcurrentIndexing as custom test
public class FullSolrCloudDistribCmdsTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(1);

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);

    System.setProperty("solr.tests.maxBufferedDocs", "10000");
    System.setProperty("solr.tests.ramBufferSizeMB", "-1");
    System.setProperty("solr.tests.mergePolicyFactory", "solr.LogDocMergePolicyFactory");

    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");
    System.setProperty("distribUpdateSoTimeout", "10000");
    System.setProperty("socketTimeout", "15000");
    System.setProperty("connTimeout", "5000");
    System.setProperty("solr.test.socketTimeout.default", "15000");
    System.setProperty("solr.connect_timeout.default", "5000");
    System.setProperty("solr.so_commit_timeout.default", "15000");
    System.setProperty("solr.httpclient.defaultConnectTimeout", "5000");
    System.setProperty("solr.httpclient.defaultSoTimeout", "15000");

    System.setProperty("solr.httpclient.retries", "1");
    System.setProperty("solr.retries.on.forward", "1");
    System.setProperty("solr.retries.to.followers", "1");

    System.setProperty("solr.waitForState", "10"); // secs

    System.setProperty("solr.default.collection_op_timeout", "15000");

    System.setProperty("solr.autoCommit.maxTime", "5000");
   // System.setProperty("disable.v2.api", "true");


    // use a 5 node cluster so with a typical 2x2 collection one node isn't involved
    // helps to randomly test edge cases of hitting a node not involved in collection
    configureCluster(TEST_NIGHTLY ? 5 : 6).configure();
  }

  @After
  public void purgeAllCollections() {
    cluster.getSolrClient().setDefaultCollection(null);
  }


  @AfterClass
  public static void after() throws Exception {
    cluster.getZkServer().writeZkMonLayout("afterTest");
    shutdownCluster();
  }

  /**
   * Creates a new 2x2 collection using a unique name, blocking until it's state is fully active, 
   * and sets that collection as the default on the cluster's default CloudHttp2SolrClient.
   * 
   * @return the name of the new collection
   */
  public static String createAndSetNewDefaultCollection() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String name = "test_collection_" + NAME_COUNTER.getAndIncrement();
    CollectionAdminRequest.createCollection(name, "_default", 2, 2).setMaxShardsPerNode(10)
                 .process(cloudClient);
    // createCollection().process() returns once the collection is created, but not necessarily once
    // every replica has finished its initial sync and reached ACTIVE. Wait for all 4 replicas to be
    // active before returning so callers that immediately stream heavy updates don't race a replica's
    // initial recovery (which, under a fast stream, peer-syncs with 'no frame of reference' and can
    // leave the just-active replica momentarily behind the leader).
    cluster.waitForActiveCollection(name, 2, 4);
    cloudClient.setDefaultCollection(name);
    return name;
  }
  
  @Test
  public void testBasicUpdates() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createAndSetNewDefaultCollection();
    
    // add a doc, update it, and delete it
    addUpdateDelete(collectionName, "doc1");
    assertEquals(0, cloudClient.query(params("q","*:*")).getResults().getNumFound());
    
    // add 2 docs in a single request
    addTwoDocsInOneRequest(collectionName, "doc2", "doc3");
    assertEquals(2, cloudClient.query(params("q","*:*")).getResults().getNumFound());

    // 2 deletes in a single request...
    assertEquals(0, (new UpdateRequest().deleteById("doc2").deleteById("doc3"))
                 .process(cloudClient).getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    
    assertEquals(0, cloudClient.query(params("q","*:*")).getResults().getNumFound());
    
    // add a doc that we will then delete later after adding two other docs (all before next commit).
    assertEquals(0, cloudClient.add(
        SolrTestCaseJ4.sdoc("id", "doc4", "content_s", "will_delete_later")).getStatus());
    assertEquals(0, cloudClient.add(SolrTestCaseJ4.sdocs(SolrTestCaseJ4.sdoc("id", "doc5"),
        SolrTestCaseJ4.sdoc("id", "doc6"))).getStatus());
    assertEquals(0, cloudClient.deleteById("doc4").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(0, cloudClient.query(params("q", "id:doc4")).getResults().getNumFound());
    assertEquals(1, cloudClient.query(params("q", "id:doc5")).getResults().getNumFound());
    assertEquals(1, cloudClient.query(params("q", "id:doc6")).getResults().getNumFound());
    assertEquals(2, cloudClient.query(params("q","*:*")).getResults().getNumFound());
    
   // checkShardConsistency(params("q","*:*", "rows", "9999","_trace","post_doc_5_6"));

    // delete everything....
    assertEquals(0, cloudClient.deleteByQuery("*:*").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(0, cloudClient.query(params("q","*:*")).getResults().getNumFound());

   // checkShardConsistency(params("q","*:*", "rows", "9999","_trace","delAll"));

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }

  @LuceneTestCase.Nightly
  public void testThatCantForwardToLeaderFails() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = "test_collection_" + NAME_COUNTER.getAndIncrement();
    cloudClient.setDefaultCollection(collectionName);
    
    // get a random node for use in our collection before creating the one we'll partition..
    final JettySolrRunner otherLeader = cluster.getRandomJetty(random());
    // pick a (second) random node (which may be the same) for sending updates to
    // (if it's the same, we're testing routing from another shard, if diff we're testing routing
    // from a non-collection node)
    final String indexingUrl = cluster.getRandomJetty(random()).getProxyBaseUrl() + '/' + collectionName;

    // create a new node for the purpose of killing it...
    final JettySolrRunner leaderToPartition = cluster.startJettySolrRunner();
    final SocketProxy proxy = new SocketProxy();
    // Track the primary failure so a cleanup failure in the finally below can never MASK it. Without
    // this, a teardown deleteCollection that fails with "Could not find collection" (which happens
    // when the body failed before/while fully creating the collection, e.g. under heavy load)
    // replaces the real cause and makes the failure undiagnosable.
    Throwable primary = null;
    try {

      // HACK: we have to stop the node in order to enable the proxy, in order to then restart the node
      // (in order to then "partition it" later via the proxy)
      cluster.stopJettySolrRunner(leaderToPartition);

      leaderToPartition.setProxyPort(proxy.getListenPort());
      cluster.startJettySolrRunner(leaderToPartition);
      proxy.open(new URI(leaderToPartition.getBaseUrl()));
      try {
        log.info("leaderToPartition's Proxy: {}", proxy);

        // create a 2x1 collection using a nodeSet that includes our leaderToPartition...
        assertEquals(RequestStatusState.COMPLETED,
            CollectionAdminRequest.createCollection(collectionName, 2, 1).setCreateNodeSet(leaderToPartition.getNodeName() + "," + otherLeader.getNodeName())
                .processAndWait(cloudClient, DEFAULT_TIMEOUT));

        { // HACK: Check the leaderProps for the shard hosted on the node we're going to kill...
          final Replica leaderProps = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName)
              .getLeaderReplicas(leaderToPartition.getNodeName()).get(0);

          // No point in this test if these aren't true...
          // NOTE (fork): getLeaderReplicas() already filters via Slice.getLeader()
          // (getRawState()==LEADER), so leaderProps is guaranteed to be the leader.
          // The STATE_PROP/Slice.LEADER prop is stripped from Replica in this fork
          // (always null), so assert leadership via getRawState() instead.
          assertEquals("Sanity check: leaderProps isn't a leader?: " + leaderProps.toString(),
              Replica.State.LEADER, leaderProps.getRawState());
          assertTrue("Sanity check: leaderProps isn't using the proxy port?: " + leaderProps,
              leaderProps.getCoreUrl().contains(String.valueOf(proxy.getListenPort())));
        }

        // create client to send our updates to...
        Http2SolrClient indexClient = cloudClient.getHttpClient();

        // Sanity check: we should be able to send a bunch of updates that work right now...

        UpdateRequest req = new UpdateRequest();
        req.setBasePath(indexingUrl);
        Random random = random();
        for (int i = 0; i < 100; i++) {
          req.add(SolrTestCaseJ4.sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random, 200)));
          UpdateResponse rsp = req.process(indexClient, collectionName);
          assertEquals(0, rsp.getStatus());
        }

        log.info("Closing leaderToPartition's proxy: {}", proxy);
        proxy.close(); // NOTE: can't use halfClose, won't ensure a garunteed failure
        final SolrException e = LuceneTestCase.expectThrows(SolrException.class, () -> {
          // start at 50 so that we have some "updates" to previous docs and some "adds"...
          for (int i = 50; i < 250; i++) {
            // Pure random odds of all of these docs belonging to the live shard are 1 in 2**200...
            // Except we know the hashing algorithm isn't purely random,
            // So the actual odds are "0" unless the hashing algorithm is changed to suck badly...
            // Use req (which has basePath=indexingUrl set) so Http2SolrClient knows the destination.
            req.clear();
            req.add(SolrTestCaseJ4.sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random, 200)));
            final UpdateResponse rsp = req.process(indexClient, collectionName);
            // if the update didn't throw an exception, it better be a success..
            assertEquals(0, rsp.getStatus());
          }
        });
        assertEquals(500, e.code());
      } finally {
        proxy.close(); // don't leak this port
      }
    } catch (Throwable t) {
      primary = t;
      // precise rethrow without widening to Throwable (this method only declares throws Exception)
      if (t instanceof Error) throw (Error) t;
      if (t instanceof RuntimeException) throw (RuntimeException) t;
      throw (Exception) t;
    } finally {
      // Best-effort teardown. Each step runs independently so one failure can't skip the rest, and
      // NO cleanup failure is allowed to mask a primary test failure -- if the body already threw we
      // attach cleanup failures as suppressed exceptions instead of replacing the real cause.
      // Stop leaderToPartition and reopen its proxy so cleanup can contact all shard nodes; the proxy
      // was closed to simulate a partition, and Solr can't delete the collection while a shard leader
      // is unreachable (gets 500), so reopening first allows clean teardown.
      Exception cleanupError = null;
      try { proxy.reopen(); } catch (Exception c) { cleanupError = c; }
      try {
        CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
      } catch (Exception c) { cleanupError = chainCleanupError(cleanupError, c); }
      try { cluster.stopJettySolrRunner(leaderToPartition); } // don't let this jetty bleed into other tests
      catch (Exception c) { cleanupError = chainCleanupError(cleanupError, c); }
      try { cluster.startJettySolrRunner(); } catch (Exception c) { cleanupError = chainCleanupError(cleanupError, c); }
      if (cleanupError != null) {
        if (primary != null) primary.addSuppressed(cleanupError); else throw cleanupError;
      }
    }
  }

  /** Collect multiple best-effort cleanup failures without losing any (later ones are suppressed). */
  private static Exception chainCleanupError(Exception existing, Exception next) {
    if (existing == null) return next;
    existing.addSuppressed(next);
    return existing;
  }
  
  /**  NOTE: uses the cluster's CloudHttp2SolrClient and assumes default collection has been set */
  private static void addTwoDocsInOneRequest(String collection, String docIdA, String docIdB) throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();

    assertEquals(0, cloudClient.add(SolrTestCaseJ4.sdocs(SolrTestCaseJ4.sdoc("id", docIdA),
        SolrTestCaseJ4.sdoc("id", docIdB))).getStatus());

    assertEquals(0, cloudClient.commit().getStatus());
    
    assertEquals(2, cloudClient.query(params("q","id:(" + docIdA + " OR " + docIdB + ")")
                                      ).getResults().getNumFound());

    // replicas may still be recovering from leaders
    cluster.waitForActiveCollection(collection, 2, 4);
    checkShardConsistency(params("q","*:*", "rows", "99","_trace","two_docs"));
  }

  /**  NOTE: uses the cluster's CloudHttp2SolrClient and assumes default collection has been set */
  private static void addUpdateDelete(String collection, String docId) throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();

    // add the doc, confirm we can query it...
    assertEquals(0, cloudClient.add(SolrTestCaseJ4.sdoc("id", docId, "content_t", "originalcontent")).getStatus());
    assertEquals(0, cloudClient.commit().getStatus());

    cluster.waitForActiveCollection(collection, 2, 4);

    assertEquals(1, cloudClient.query(params("q", "id:" + docId)).getResults().getNumFound());
    assertEquals(1, cloudClient.query(params("q", "content_t:originalcontent")).getResults().getNumFound());
    assertEquals(1,
                 cloudClient.query(params("q", "content_t:originalcontent AND id:" + docId))
                 .getResults().getNumFound());


    // replicas may still be recovering from leaders

    //checkShardConsistency(params("q","id:" + docId, "rows", "99","_trace","original_doc"));
    
    // update doc
    assertEquals(0, cloudClient.add(SolrTestCaseJ4.sdoc("id", docId, "content_t", "updatedcontent")).getStatus());
    assertEquals(0, cloudClient.commit().getStatus());
    
    // confirm we can query the doc by updated content and not original...
    assertEquals(0, cloudClient.query(params("q", "content_t:originalcontent")).getResults().getNumFound());
    assertEquals(1, cloudClient.query(params("q", "content_t:updatedcontent")).getResults().getNumFound());
    assertEquals(1,
                 cloudClient.query(params("q", "content_t:updatedcontent AND id:" + docId))
                 .getResults().getNumFound());
    
    // delete the doc, confim it no longer matches in queries...
    assertEquals(0, cloudClient.deleteById(docId).getStatus());
    assertEquals(0, cloudClient.commit(collection).getStatus());
    
    assertEquals(0, cloudClient.query(params("q", "id:" + docId)).getResults().getNumFound());
    assertEquals(0, cloudClient.query(params("q", "content_t:updatedcontent")).getResults().getNumFound());

    checkShardConsistency(params("q","id:" + docId, "rows", "99","_trace","del_updated_doc"));
  }

  @Test
  public long testIndexQueryDeleteHierarchical() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createAndSetNewDefaultCollection();
    
    // index
    long docId = 42;
    int topDocsNum = LuceneTestCase.atLeast(TEST_NIGHTLY ? 5 : 2);
    int childsNum = (TEST_NIGHTLY ? 5 : 2)+random().nextInt(TEST_NIGHTLY ? 5 : 2);
    for (int i = 0; i < topDocsNum; ++i) {
      UpdateRequest uReq = new UpdateRequest();
      SolrInputDocument topDocument = new SolrInputDocument();
      topDocument.addField("id", docId++);
      topDocument.addField("type_s", "parent");
      topDocument.addField(i + "parent_f1_s", "v1");
      topDocument.addField(i + "parent_f2_s", "v2");
      
      
      for (int index = 0; index < childsNum; ++index) {
        docId = addChildren("child", topDocument, index, false, docId);
      }
      
      uReq.add(topDocument);
      assertEquals(i + "/" + docId,
                   0, uReq.process(cloudClient).getStatus());
    }
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    // replicas may still be recovering from leaders; wait before per-replica consistency check
    cluster.waitForActiveCollection(collectionName, 2, 4);

    checkShardConsistency(params("q","*:*", "rows", "9999","_trace","added_all_top_docs_with_kids"));
    
    // query
    
    // parents
    assertEquals(topDocsNum,
                 cloudClient.query(new SolrQuery("type_s:parent")).getResults().getNumFound());
    
    // childs 
    assertEquals(topDocsNum * childsNum,
                 cloudClient.query(new SolrQuery("type_s:child")).getResults().getNumFound());
                 
    
    // grandchilds
    //
    //each topDoc has t childs where each child has x = 0 + 2 + 4 + ..(t-1)*2 grands
    //x = 2 * (1 + 2 + 3 +.. (t-1)) => arithmetic summ of t-1 
    //x = 2 * ((t-1) * t / 2) = t * (t - 1)
    assertEquals(topDocsNum * childsNum * (childsNum - 1),
                 cloudClient.query(new SolrQuery("type_s:grand")).getResults().getNumFound());
    
    //delete
    assertEquals(0, cloudClient.deleteByQuery("*:*").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(0, cloudClient.query(params("q","*:*")).getResults().getNumFound());

    checkShardConsistency(params("q","*:*", "rows", "9999","_trace","delAll"));
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    return docId;
  }

  
  /**
   * Recursive helper function for building out child and grandchild docs
   */
  private static long addChildren(String prefix, SolrInputDocument topDocument, int childIndex, boolean lastLevel, long docId) {
    SolrInputDocument childDocument = new SolrInputDocument();
    childDocument.addField("id", docId++);
    childDocument.addField("type_s", prefix);
    for (int index = 0; index < childIndex; ++index) {
      childDocument.addField(childIndex + prefix + index + "_s", childIndex + "value"+ index);
    }   
  
    if (!lastLevel) {
      for (int i = 0; i < childIndex * 2; ++i) {
        docId = addChildren("grand", childDocument, i, true, docId);
      }
    }
    topDocument.addChildDocument(childDocument);
    return docId;
  }

  @Test
  public void testIndexingOneDocPerRequestWithHttpSolrClient() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createAndSetNewDefaultCollection();
    
    final int numDocs = LuceneTestCase.atLeast(TEST_NIGHTLY ? 50 : 15);
    for (int i = 0; i < numDocs; i++) {
      UpdateRequest uReq;
      uReq = new UpdateRequest();
      assertEquals(0, cloudClient.add
                   (SolrTestCaseJ4.sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random(), 200))).getStatus());
    }
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(numDocs, cloudClient.query(params("q","*:*")).getResults().getNumFound());

    // replicas may still be recovering from leaders; wait for them to become active
    // before comparing per-replica doc sets (checkShardConsistency requires all replicas
    // to have caught up to their leader).
    cluster.waitForActiveCollection(collectionName, 2, 4);

    checkShardConsistency(params("q","*:*", "rows", String.valueOf(1 + numDocs),"_trace","addAll"));
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }

  public void testIndexingBatchPerRequestWithHttpSolrClient() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createAndSetNewDefaultCollection();
    cluster.waitForActiveCollection(collectionName, 2, 4);
    final int numDocsPerBatch = SolrTestUtil.atLeast(5);
    final int numBatchesPerThread = SolrTestUtil.atLeast(5);
      
    final CountDownLatch abort = new CountDownLatch(1);
    class BatchIndexer implements Runnable {
      private boolean keepGoing() {
        return 0 < abort.getCount();
      }
      
      final int name;
      public BatchIndexer(int name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        try {
          for (int batchId = 0; batchId < numBatchesPerThread && keepGoing(); batchId++) {
            final UpdateRequest req = new UpdateRequest();
            for (int docId = 0; docId < numDocsPerBatch && keepGoing(); docId++) {
              req.add(SolrTestCaseJ4.sdoc("id", "indexer" + name + "_" + batchId + "_" + docId,
                           "test_t", TestUtil.randomRealisticUnicodeString(random(), 200)));
            }
            assertEquals(0, req.process(cloudClient).getStatus());
          }
        } catch (Throwable e) {
          abort.countDown();
          throw new RuntimeException(e);
        }
      }
    };
    final ExecutorService executor = Executors.newCachedThreadPool(new SolrNamedThreadFactory("batchIndexing"));
    final int numThreads = random().nextInt(TEST_NIGHTLY ? 4 : 2) + 1;
    final List<Future<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(new BatchIndexer(i)));
    }
    final int totalDocsExpected = numThreads * numBatchesPerThread * numDocsPerBatch;

    executor.shutdown();

    for (Future result : futures) {
     // assertFalse(result.isCancelled());
     // assertTrue(result.isDone());
      // all we care about is propogating any possibile execution exception...
      final Object ignored = result.get();
    }

    ExecutorUtil.shutdownAndAwaitTermination(executor);

    cluster.waitForActiveCollection(collectionName, 2, 4);
    cloudClient.commit();
    assertEquals(totalDocsExpected, cloudClient.query(params("q","*:*")).getResults().getNumFound());
    checkShardConsistency(params("q","*:*", "rows", String.valueOf(totalDocsExpected), "_trace","batches_done"));
  }

  // NOTE (fork): The @Ignore was removed. Both former blockers are resolved by production fixes made
  // elsewhere on branch rip: the concurrent HTTP/2 update path no longer drops queued adds (the
  // DistributedUpdateProcessor async double-dispatch race and the JettySolrRunner/Http2SolrClient
  // EatWhatYouKill reservedThreads(0) deadlock fixes), and the full cluster stop/start +
  // checkShardConsistency now succeeds. Verified stable across nightly runs.
  @Test
  public void testConcurrentIndexing() throws Exception {
    final CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createAndSetNewDefaultCollection();

    final int numDocs = 15000;//TEST_NIGHTLY ? atLeast(500) : 59;
    final JettySolrRunner nodeToUpdate = cluster.getRandomJetty(random());
    // NOTE: ConcurrentUpdateHttp2SolrClient appends "update" to its base URL (see
    // Http2SolrClient.initOutStream: basePath + "update"), so the base URL must be the
    // collection base ending in '/', NOT ".../update" (which would produce "/updateupdate"
    // -> Null Request Handler -> 0 docs indexed).
    try (ConcurrentUpdateHttp2SolrClient indexClient
         = SolrTestCaseJ4.getConcurrentUpdateSolrClient(cloudClient.getHttpClient(), nodeToUpdate.getBaseUrl() + "/" + collectionName + "/", 10, 4)) {
      Random random = random();
      for (int i = 0; i < numDocs; i++) {
        indexClient.add(SolrTestCaseJ4.sdoc("id", i, "text_t",
                             TestUtil.randomRealisticUnicodeString(random, 200)));
      }
      indexClient.blockUntilFinished();
    }

    cluster.waitForActiveCollection(collectionName, 2, 4);

    assertEquals(0, cluster.getSolrClient().commit().getStatus());

    long found = cloudClient.query(params("q", "*:*")).getResults().getNumFound();


    cluster.waitForActiveCollection(collectionName, 2, 4);

    assertEquals(numDocs + " found " + found, numDocs, found);
    cluster.getSolrClient().getZkStateReader().checkShardConsistency(collectionName);

    cluster.stopJettyRunners();
    cluster.startJettyRunners();

    cluster.getSolrClient().getZkStateReader().checkShardConsistency(collectionName);
    //checkShardConsistency(params("q","*:*", "rows", ""+(1 + numDocs),"_trace","addAll"));
    //CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }
  
  /**
   * Inspects the cluster to determine all active shards/replicas for the default collection then,
   * executes a <code>distrib=false</code> query using the specified params, and compares the resulting 
   * {@link SolrDocumentList}, failing if any replica does not agree with it's leader.
   *
   * @see #cluster
   * @see CloudInspectUtil#showDiff 
   */
  private static void checkShardConsistency(final SolrParams params) throws Exception {
    // TODO: refactor into static in CloudInspectUtil w/ DocCollection param?
    // TODO: refactor to take in a BiFunction<QueryResponse,QueryResponse,Boolean> ?
    
    final SolrParams perReplicaParams = SolrParams.wrapDefaults(params("distrib", "false"),
                                                                params);
    final DocCollection collection = cluster.getSolrClient().getZkStateReader()
      .getClusterState().getCollection(cluster.getSolrClient().getDefaultCollection());
    log.info("Checking shard consistency via: {}", perReplicaParams);
    for (Map.Entry<String,Slice> entry : collection.getActiveSlicesMap().entrySet()) {
      final String shardName = entry.getKey();
      final Slice slice = entry.getValue();
      log.info("Checking: {} -> {}", shardName, slice);
      final Replica leader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collection.getName(), shardName, 5000);
      try (Http2SolrClient leaderClient = SolrTestCaseJ4.getHttpSolrClient(leader.getCoreUrl())) {
        final SolrDocumentList leaderResults = leaderClient.query(perReplicaParams).getResults();
        log.debug("Shard {}: Leader results: {}", shardName, leaderResults);
        for (Replica replica : slice) {
          try (Http2SolrClient replicaClient = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
            final SolrDocumentList replicaResults = replicaClient.query(perReplicaParams).getResults();
            if (log.isDebugEnabled()) {
              log.debug("Shard {}: Replica ({}) results: {}", shardName, replica.getName(), replicaResults);
            }
            assertEquals("inconsistency w/leader: shard=" + shardName + "core=" + replica.getName(),
                         Collections.emptySet(),
                         CloudInspectUtil.showDiff(leaderResults, replicaResults,
                                                   shardName + " leader: " + leader.getCoreUrl(),
                                                   shardName + ": " + replica.getCoreUrl()));
          }
        }
      }
    }
  }

}
