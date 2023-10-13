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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.ReplicaStateProps;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Super basic testing, no shard restarting or anything. */
public class FullSolrCloudDistribCmdsTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicInteger NAME_COUNTER = new AtomicInteger(1);

  @BeforeClass
  public static void setupCluster() throws Exception {
    // use a 5 node cluster so with a typical 2x2 collection one node isn't involved
    // helps to randomly test edge cases of hitting a node not involved in collection
    configureCluster(5).configure();
  }

  @After
  public void purgeAllCollections() throws Exception {
    cluster.deleteAllCollections();
  }

  /**
   * Creates a new 2x2 collection using a unique name, blocking until it's state is fully active.
   *
   * @return the name of the new collection
   */
  public static String createNewCollection() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String name = "test_collection_" + NAME_COUNTER.getAndIncrement();
    assertTrue(
        CollectionAdminRequest.createCollection(name, "_default", 2, 2)
            .process(cloudClient)
            .isSuccess());
    ZkStateReader.from(cloudClient)
        .waitForState(
            name,
            DEFAULT_TIMEOUT,
            TimeUnit.SECONDS,
            (n, c) -> DocCollection.isFullyActive(n, c, 2, 2));
    return name;
  }

  @Test
  public void testBasicUpdates() throws Exception {
    final String collectionName = createNewCollection();
    final CloudSolrClient cloudClient = cluster.getSolrClient();

    // add a doc, update it, and delete it
    addUpdateDelete(collectionName, "doc1");
    assertEquals(
        0, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    // add 2 docs in a single request
    addTwoDocsInOneRequest(collectionName, "doc2", "doc3");
    assertEquals(
        2, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    // 2 deletes in a single request...
    assertEquals(
        0,
        (new UpdateRequest().deleteById("doc2").deleteById("doc3"))
            .process(cloudClient, collectionName)
            .getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(
        0, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    // add a doc that we will then delete later after adding two other docs (all before next
    // commit).
    assertEquals(
        0,
        cloudClient
            .add(collectionName, sdoc("id", "doc4", "content_s", "will_delete_later"))
            .getStatus());
    assertEquals(
        0,
        cloudClient.add(collectionName, sdocs(sdoc("id", "doc5"), sdoc("id", "doc6"))).getStatus());
    assertEquals(0, cloudClient.deleteById(collectionName, "doc4").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(
        0, cloudClient.query(collectionName, params("q", "id:doc4")).getResults().getNumFound());
    assertEquals(
        1, cloudClient.query(collectionName, params("q", "id:doc5")).getResults().getNumFound());
    assertEquals(
        1, cloudClient.query(collectionName, params("q", "id:doc6")).getResults().getNumFound());
    assertEquals(
        2, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    checkShardConsistency(
        collectionName, params("q", "*:*", "rows", "9999", "_trace", "post_doc_5_6"));

    // delete everything....
    assertEquals(0, cloudClient.deleteByQuery(collectionName, "*:*").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(
        0, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    checkShardConsistency(collectionName, params("q", "*:*", "rows", "9999", "_trace", "delAll"));
  }

  public void testDeleteByIdImplicitRouter() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String testCollectionName =
        "implicit_collection_without_routerfield_" + NAME_COUNTER.getAndIncrement();
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                testCollectionName, "_default", "shard1,shard2", 2)
            .processAndWait(cloudClient, DEFAULT_TIMEOUT));
    ZkStateReader.from(cloudClient)
        .waitForState(
            testCollectionName,
            DEFAULT_TIMEOUT,
            TimeUnit.SECONDS,
            (n, c1) -> DocCollection.isFullyActive(n, c1, 2, 2));

    final DocCollection docCol = cloudClient.getClusterState().getCollection(testCollectionName);
    try (SolrClient shard1 = getHttpSolrClient(docCol.getSlice("shard1").getLeader().getCoreUrl());
        SolrClient shard2 = getHttpSolrClient(docCol.getSlice("shard2").getLeader().getCoreUrl())) {

      // Add three documents to shard1
      shard1.add(sdoc("id", "1", "title", "s1 one"));
      shard1.add(sdoc("id", "2", "title", "s1 two"));
      shard1.add(sdoc("id", "3", "title", "s1 three"));
      shard1.commit();
      final AtomicInteger docCounts1 = new AtomicInteger(3);

      // Add two documents to shard2
      shard2.add(sdoc("id", "4", "title", "s2 four"));
      shard2.add(sdoc("id", "5", "title", "s2 five"));
      shard2.commit();
      final AtomicInteger docCounts2 = new AtomicInteger(2);

      // A re-usable helper to verify that the expected number of documents can be found on each
      // shard...
      Runnable checkShardCounts =
          () -> {
            try {
              // including cloudClient helps us test view from other nodes that aren't the
              // leaders...
              for (SolrClient c : Arrays.asList(cloudClient, shard1, shard2)) {

                ModifiableSolrParams params = params("q", "*:*");
                if (c instanceof CloudSolrClient) {
                  params.add("collection", testCollectionName);
                }
                assertEquals(
                    docCounts1.get() + docCounts2.get(),
                    c.query(params).getResults().getNumFound());

                assertEquals(
                    docCounts1.get(),
                    c.query(params.set("shards", "shard1")).getResults().getNumFound());
                assertEquals(
                    docCounts2.get(),
                    c.query(params.set("shards", "shard2")).getResults().getNumFound());

                assertEquals(
                    docCounts1.get() + docCounts2.get(),
                    c.query(params.set("shards", "shard2,shard1")).getResults().getNumFound());
              }

              assertEquals(
                  docCounts1.get(),
                  shard1.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound());
              assertEquals(
                  docCounts2.get(),
                  shard2.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound());

            } catch (Exception sse) {
              throw new RuntimeException(sse);
            }
          };
      checkShardCounts.run();

      { // Send a delete request for a doc on shard1 to core hosting shard1 with NO routing info
        // Should delete (implicitly) since doc is (implicitly) located on this shard
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("1");
        shard1.request(deleteRequest);
        shard1.commit();
        docCounts1.decrementAndGet();
      }
      checkShardCounts.run();

      { // Send a delete request to core hosting shard1 with a route param for a document that is
        // actually in shard2 should delete.
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("4").withRoute("shard2");
        shard1.request(deleteRequest);
        shard1.commit();
        docCounts2.decrementAndGet();
      }
      checkShardCounts.run();

      { // Send a delete request to core hosting shard1 with NO route param for a document that is
        // actually in shard2. Shouldn't delete, since deleteById requests are not broadcast to all
        // shard leaders. (This is effectively a request to delete "5" if and only if it is on
        // shard1)
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("5");
        shard1.request(deleteRequest);
        shard1.commit();
      }
      checkShardCounts.run();

      { // Multiple deleteById commands for different shards in a single request
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("2", "shard1");
        deleteRequest.deleteById("5", "shard2");
        shard1.request(deleteRequest);
        shard1.commit();
        docCounts1.decrementAndGet();
        docCounts2.decrementAndGet();
      }
      checkShardCounts.run();
    }
  }

  public void testRTGCompositeRouterWithRouterField() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String testCollectionName =
        "composite_collection_with_routerfield_" + NAME_COUNTER.getAndIncrement();
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.createCollection(testCollectionName, "_default", 2, 2)
            .setRouterName("compositeId")
            .setRouterField("routefield_s")
            .setShards("shard1,shard2")
            .processAndWait(cloudClient, DEFAULT_TIMEOUT));
    ZkStateReader.from(cloudClient)
        .waitForState(
            testCollectionName,
            DEFAULT_TIMEOUT,
            TimeUnit.SECONDS,
            (n, c1) -> DocCollection.isFullyActive(n, c1, 2, 2));

    // Add a few documents with diff routes
    cloudClient.add(testCollectionName, sdoc("id", "1", "routefield_s", "europe"));
    cloudClient.add(testCollectionName, sdoc("id", "3", "routefield_s", "europe"));
    cloudClient.add(testCollectionName, sdoc("id", "5", "routefield_s", "africa"));
    cloudClient.add(testCollectionName, sdoc("id", "7", "routefield_s", "africa"));
    cloudClient.commit(testCollectionName);

    var docsNoRoute = cloudClient.getById(testCollectionName, List.of("3"));
    assertEquals(0, docsNoRoute.getNumFound());

    var params = new ModifiableSolrParams();
    params.set("_route_", "europe");
    var docsWRoute = cloudClient.getById(testCollectionName, List.of("3"), params);
    assertEquals(1, docsWRoute.getNumFound());
  }

  public void testDeleteByIdCompositeRouterWithRouterField() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String testCollectionName =
        "composite_collection_with_routerfield_" + NAME_COUNTER.getAndIncrement();
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.createCollection(testCollectionName, "_default", 2, 2)
            .setRouterName("compositeId")
            .setRouterField("routefield_s")
            .setShards("shard1,shard2")
            .processAndWait(cloudClient, DEFAULT_TIMEOUT));
    ZkStateReader.from(cloudClient)
        .waitForState(
            testCollectionName,
            DEFAULT_TIMEOUT,
            TimeUnit.SECONDS,
            (n, c1) -> DocCollection.isFullyActive(n, c1, 2, 2));

    final DocCollection docCol = cloudClient.getClusterState().getCollection(testCollectionName);
    try (SolrClient shard1 = getHttpSolrClient(docCol.getSlice("shard1").getLeader().getCoreUrl());
        SolrClient shard2 = getHttpSolrClient(docCol.getSlice("shard2").getLeader().getCoreUrl())) {

      // Add six documents w/diff routes (all sent to shard1 leader's core)
      shard1.add(sdoc("id", "1", "routefield_s", "europe"));
      shard1.add(sdoc("id", "3", "routefield_s", "europe"));
      shard1.add(sdoc("id", "5", "routefield_s", "africa"));
      shard1.add(sdoc("id", "7", "routefield_s", "europe"));
      shard1.add(sdoc("id", "9", "routefield_s", "europe"));
      shard1.add(sdoc("id", "11", "routefield_s", "africa"));
      shard1.commit();

      // Add four documents w/diff routes (all sent to shard2 leader's core)
      shard2.add(sdoc("id", "8", "routefield_s", "africa"));
      shard2.add(sdoc("id", "6", "routefield_s", "europe"));
      shard2.add(sdoc("id", "4", "routefield_s", "africa"));
      shard2.add(sdoc("id", "2", "routefield_s", "europe"));
      shard2.commit();

      final AtomicInteger docCountsEurope = new AtomicInteger(6);
      final AtomicInteger docCountsAfrica = new AtomicInteger(4);

      // A re-usable helper to verify that the expected number of documents can be found based on
      // _route_ key...
      Runnable checkShardCounts =
          () -> {
            try {
              // including cloudClient helps us test view from other nodes that aren't the
              // leaders...
              for (SolrClient c : Arrays.asList(cloudClient, shard1, shard2)) {

                ModifiableSolrParams params = params("q", "*:*");
                if (c instanceof CloudSolrClient) {
                  params.add("collection", testCollectionName);
                }
                assertEquals(
                    docCountsEurope.get() + docCountsAfrica.get(),
                    c.query(params).getResults().getNumFound());

                assertEquals(
                    docCountsEurope.get(),
                    c.query(params.set("_route_", "europe")).getResults().getNumFound());
                assertEquals(
                    docCountsAfrica.get(),
                    c.query(params.set("_route_", "africa")).getResults().getNumFound());
              }
            } catch (Exception sse) {
              throw new RuntimeException(sse);
            }
          };
      checkShardCounts.run();

      { // Send a delete request to core hosting shard1 with a route param for a document that was
        // originally added via core on shard2
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("4", "africa");
        shard1.request(deleteRequest);
        shard1.commit();
        docCountsAfrica.decrementAndGet();
      }
      checkShardCounts.run();

      { // Multiple deleteById commands with different routes in a single request
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("2", "europe");
        deleteRequest.deleteById("5", "africa");
        shard1.request(deleteRequest);
        shard1.commit();
        docCountsEurope.decrementAndGet();
        docCountsAfrica.decrementAndGet();
      }
      checkShardCounts.run();

      // Tests for distributing delete by id when route is missing from the request
      { // Send a delete request with no route to shard1 for document on shard2, should be
        // distributed
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("8");
        shard1.request(deleteRequest);
        shard1.commit();
        docCountsAfrica.decrementAndGet();
      }
      checkShardCounts.run();

      { // Multiple deleteById commands with missing route in a single request, should be
        // distributed
        final UpdateRequest deleteRequest = new UpdateRequest();
        deleteRequest.deleteById("6");
        deleteRequest.deleteById("11");
        shard1.request(deleteRequest);
        shard1.commit();
        docCountsEurope.decrementAndGet();
        docCountsAfrica.decrementAndGet();
      }
      checkShardCounts.run();
    }
  }

  public void testThatCantForwardToLeaderFails() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = "test_collection_" + NAME_COUNTER.getAndIncrement();

    // get a random node for use in our collection before creating the one we'll partition
    final JettySolrRunner otherLeader = cluster.getRandomJetty(random());
    // pick a (second) random node (which may be the same) for sending updates to
    // (if it's the same, we're testing routing from another shard, if diff we're testing routing
    // from a non-collection node)
    final String indexingUrl =
        cluster.getRandomJetty(random()).getProxyBaseUrl() + "/" + collectionName;

    // create a new node for the purpose of killing it...
    final JettySolrRunner leaderToPartition = cluster.startJettySolrRunner();
    try {
      cluster.waitForNode(leaderToPartition, DEFAULT_TIMEOUT);

      // HACK: we have to stop the node in order to enable the proxy, in order to then restart the
      // node
      // (in order to then "partition it" later via the proxy)
      final SocketProxy proxy = new SocketProxy();
      cluster.stopJettySolrRunner(leaderToPartition);
      cluster.waitForJettyToStop(leaderToPartition);
      leaderToPartition.setProxyPort(proxy.getListenPort());
      cluster.startJettySolrRunner(leaderToPartition);
      proxy.open(leaderToPartition.getBaseUrl().toURI());
      try {
        log.info("leaderToPartition's Proxy: {}", proxy);

        cluster.waitForNode(leaderToPartition, DEFAULT_TIMEOUT);
        // create a 2x1 collection using a nodeSet that includes our leaderToPartition...
        assertEquals(
            RequestStatusState.COMPLETED,
            CollectionAdminRequest.createCollection(collectionName, 2, 1)
                .setCreateNodeSet(leaderToPartition.getNodeName() + "," + otherLeader.getNodeName())
                .processAndWait(cloudClient, DEFAULT_TIMEOUT));

        ZkStateReader.from(cloudClient)
            .waitForState(
                collectionName,
                DEFAULT_TIMEOUT,
                TimeUnit.SECONDS,
                (n, c) -> DocCollection.isFullyActive(n, c, 2, 1));

        { // HACK: Check the leaderProps for the shard hosted on the node we're going to kill...
          final Replica leaderProps =
              cloudClient
                  .getClusterState()
                  .getCollection(collectionName)
                  .getLeaderReplicas(leaderToPartition.getNodeName())
                  .get(0);
          // No point in this test if these aren't true...
          assertNotNull(
              "Sanity check: leaderProps isn't a leader?: " + leaderProps.toString(),
              leaderProps.getStr(ReplicaStateProps.LEADER));
          assertTrue(
              "Sanity check: leaderProps isn't using the proxy port?: " + leaderProps,
              leaderProps.getCoreUrl().contains("" + proxy.getListenPort()));
        }

        // create client to send our updates to...
        try (SolrClient indexClient = getHttpSolrClient(indexingUrl)) {

          // Sanity check: we should be able to send a bunch of updates that work right now...
          for (int i = 0; i < 100; i++) {
            final UpdateResponse rsp =
                indexClient.add(
                    sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random(), 200)));
            assertEquals(0, rsp.getStatus());
          }

          log.info("Closing leaderToPartition's proxy: {}", proxy);
          proxy.close(); // NOTE: can't use halfClose, won't ensure a guaranteed failure

          final SolrException e =
              expectThrows(
                  SolrException.class,
                  () -> {
                    // start at 50 so that we have some "updates" to previous docs and some
                    // "adds"...
                    for (int i = 50; i < 250; i++) {
                      // Pure random odds of all of these docs belonging to the live shard are 1 in
                      // 2**200...
                      // Except we know the hashing algorithm isn't purely random,
                      // So the actual odds are "0" unless the hashing algorithm is changed to suck
                      // badly...
                      final UpdateResponse rsp =
                          indexClient.add(
                              sdoc(
                                  "id",
                                  i,
                                  "text_t",
                                  TestUtil.randomRealisticUnicodeString(random(), 200)));
                      // if the update didn't throw an exception, it better be a success
                      assertEquals(0, rsp.getStatus());
                    }
                  });
          assertEquals(500, e.code());
        }
      } finally {
        proxy.close(); // don't leak this port
      }
    } finally {
      cluster.stopJettySolrRunner(leaderToPartition); // don't let this jetty bleed into other tests
      cluster.waitForJettyToStop(leaderToPartition);
    }
  }

  /** NOTE: uses the cluster's CloudSolrClient and assumes default collection has been set */
  private void addTwoDocsInOneRequest(String collectionName, String docIdA, String docIdB)
      throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    assertNotNull(collectionName);

    assertEquals(
        0,
        cloudClient.add(collectionName, sdocs(sdoc("id", docIdA), sdoc("id", docIdB))).getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(
        2,
        cloudClient
            .query(collectionName, params("q", "id:(" + docIdA + " OR " + docIdB + ")"))
            .getResults()
            .getNumFound());

    checkShardConsistency(collectionName, params("q", "*:*", "rows", "99", "_trace", "two_docs"));
  }

  /** NOTE: uses the cluster's CloudSolrClient and assumes default collection has been set */
  private void addUpdateDelete(String collectionName, String docId) throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();

    // add the doc, confirm we can query it...
    assertEquals(
        0,
        cloudClient
            .add(collectionName, sdoc("id", docId, "content_t", "originalcontent"))
            .getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(
        1,
        cloudClient.query(collectionName, params("q", "id:" + docId)).getResults().getNumFound());
    assertEquals(
        1,
        cloudClient
            .query(collectionName, params("q", "content_t:originalcontent"))
            .getResults()
            .getNumFound());
    assertEquals(
        1,
        cloudClient
            .query(collectionName, params("q", "content_t:originalcontent AND id:" + docId))
            .getResults()
            .getNumFound());

    checkShardConsistency(
        collectionName, params("q", "id:" + docId, "rows", "99", "_trace", "original_doc"));

    // update doc
    assertEquals(
        0,
        cloudClient
            .add(collectionName, sdoc("id", docId, "content_t", "updatedcontent"))
            .getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    // confirm we can query the doc by updated content and not original
    assertEquals(
        0,
        cloudClient
            .query(collectionName, params("q", "content_t:originalcontent"))
            .getResults()
            .getNumFound());
    assertEquals(
        1,
        cloudClient
            .query(collectionName, params("q", "content_t:updatedcontent"))
            .getResults()
            .getNumFound());
    assertEquals(
        1,
        cloudClient
            .query(collectionName, params("q", "content_t:updatedcontent AND id:" + docId))
            .getResults()
            .getNumFound());

    // delete the doc, confirm it no longer matches in queries
    assertEquals(0, cloudClient.deleteById(collectionName, docId).getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    assertEquals(
        0,
        cloudClient.query(collectionName, params("q", "id:" + docId)).getResults().getNumFound());
    assertEquals(
        0,
        cloudClient
            .query(collectionName, params("q", "content_t:updatedcontent"))
            .getResults()
            .getNumFound());

    checkShardConsistency(
        collectionName, params("q", "id:" + docId, "rows", "99", "_trace", "del_updated_doc"));
  }

  public long testIndexQueryDeleteHierarchical() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createNewCollection();

    // index
    long docId = 42;
    int topDocsNum = atLeast(5);
    int childsNum = 5 + random().nextInt(5);
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
      assertEquals(i + "/" + docId, 0, uReq.process(cloudClient, collectionName).getStatus());
    }
    assertEquals(0, cloudClient.commit(collectionName).getStatus());

    checkShardConsistency(
        collectionName,
        params("q", "*:*", "rows", "9999", "_trace", "added_all_top_docs_with_kids"));

    // query

    // parents
    assertEquals(
        topDocsNum,
        cloudClient
            .query(collectionName, new SolrQuery("type_s:parent"))
            .getResults()
            .getNumFound());

    // children
    assertEquals(
        topDocsNum * childsNum,
        cloudClient
            .query(collectionName, new SolrQuery("type_s:child"))
            .getResults()
            .getNumFound());

    // grandchildren
    //
    // each topDoc has t children where each child has x = 0 + 2 + 4 + ..(t-1)*2 grandchildren
    // x = 2 * (1 + 2 + 3 +.. (t-1)) => arithmetic sum of t-1
    // x = 2 * ((t-1) * t / 2) = t * (t - 1)
    assertEquals(
        topDocsNum * childsNum * (childsNum - 1),
        cloudClient
            .query(collectionName, new SolrQuery("type_s:grand"))
            .getResults()
            .getNumFound());

    // delete
    assertEquals(0, cloudClient.deleteByQuery(collectionName, "*:*").getStatus());
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(
        0, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    checkShardConsistency(collectionName, params("q", "*:*", "rows", "9999", "_trace", "delAll"));

    return docId;
  }

  /** Recursive helper function for building out child and grandchild docs */
  private long addChildren(
      String prefix, SolrInputDocument topDocument, int childIndex, boolean lastLevel, long docId) {
    SolrInputDocument childDocument = new SolrInputDocument();
    childDocument.addField("id", docId++);
    childDocument.addField("type_s", prefix);
    for (int index = 0; index < childIndex; ++index) {
      childDocument.addField(childIndex + prefix + index + "_s", childIndex + "value" + index);
    }

    if (!lastLevel) {
      for (int i = 0; i < childIndex * 2; ++i) {
        docId = addChildren("grand", childDocument, i, true, docId);
      }
    }
    topDocument.addChildDocument(childDocument);
    return docId;
  }

  public void testIndexingOneDocPerRequestWithHttpSolrClient() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createNewCollection();

    final int numDocs = atLeast(50);
    for (int i = 0; i < numDocs; i++) {
      assertEquals(
          0,
          cloudClient
              .add(
                  collectionName,
                  sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random(), 200)))
              .getStatus());
    }
    assertEquals(0, cloudClient.commit(collectionName).getStatus());
    assertEquals(
        numDocs, cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

    checkShardConsistency(
        collectionName, params("q", "*:*", "rows", "" + (1 + numDocs), "_trace", "addAll"));
  }

  public void testIndexingBatchPerRequestWithHttpSolrClient() throws Exception {

    final String collectionName = createNewCollection();
    final CloudSolrClient cloudClient = cluster.getSolrClient(collectionName);

    final int numDocsPerBatch = atLeast(5);
    final int numBatchesPerThread = atLeast(5);

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
              req.add(
                  sdoc(
                      "id",
                      "indexer" + name + "_" + batchId + "_" + docId,
                      "test_t",
                      TestUtil.randomRealisticUnicodeString(random(), 200)));
            }
            assertEquals(0, req.process(cloudClient).getStatus());
          }
        } catch (Throwable e) {
          abort.countDown();
          throw new RuntimeException(e);
        }
      }
    }

    final ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("batchIndexing");
    final int numThreads = random().nextInt(TEST_NIGHTLY ? 4 : 2) + 1;
    final List<Future<?>> futures = new ArrayList<>(numThreads);
    for (int i = 0; i < numThreads; i++) {
      futures.add(executor.submit(new BatchIndexer(i)));
    }
    final int totalDocsExpected = numThreads * numBatchesPerThread * numDocsPerBatch;
    ExecutorUtil.shutdownAndAwaitTermination(executor);

    for (Future<?> result : futures) {
      assertFalse(result.isCancelled());
      assertTrue(result.isDone());
      // all we care about is propagating any possible execution exception...
      final Object ignored = result.get();
    }

    cloudClient.commit();
    assertEquals(
        totalDocsExpected, cloudClient.query(params("q", "*:*")).getResults().getNumFound());
    checkShardConsistency(
        collectionName,
        params("q", "*:*", "rows", "" + totalDocsExpected, "_trace", "batches_done"));
  }

  public void testConcurrentIndexing() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    final String collectionName = createNewCollection();

    final int numDocs = atLeast(50);
    final JettySolrRunner nodeToUpdate = cluster.getRandomJetty(random());
    try (ConcurrentUpdateSolrClient indexClient =
        new ConcurrentUpdateSolrClient.Builder(
                nodeToUpdate.getProxyBaseUrl() + "/" + collectionName)
            .withQueueSize(10)
            .withThreadCount(2)
            .build()) {

      for (int i = 0; i < numDocs; i++) {
        indexClient.add(
            sdoc("id", i, "text_t", TestUtil.randomRealisticUnicodeString(random(), 200)));
      }
      indexClient.blockUntilFinished();

      assertEquals(0, indexClient.commit().getStatus());
      assertEquals(
          numDocs,
          cloudClient.query(collectionName, params("q", "*:*")).getResults().getNumFound());

      checkShardConsistency(
          collectionName, params("q", "*:*", "rows", "" + (1 + numDocs), "_trace", "addAll"));
    }
  }

  /**
   * Inspects the cluster to determine all active shards/replicas for the default collection then,
   * executes a <code>distrib=false</code> query using the specified params, and compares the
   * resulting {@link SolrDocumentList}, failing if any replica does not agree with its leader.
   *
   * @see #cluster
   * @see CloudInspectUtil#showDiff
   */
  private void checkShardConsistency(String collectionName, final SolrParams params)
      throws Exception {
    // TODO: refactor into static in CloudInspectUtil w/ DocCollection param?
    // TODO: refactor to take in a BiFunction<QueryResponse,QueryResponse,Boolean> ?

    final SolrParams perReplicaParams = SolrParams.wrapDefaults(params("distrib", "false"), params);
    final DocCollection collection =
        cluster.getSolrClient().getClusterState().getCollection(collectionName);
    log.info("Checking shard consistency via: {}", perReplicaParams);
    for (Map.Entry<String, Slice> entry : collection.getActiveSlicesMap().entrySet()) {
      final String shardName = entry.getKey();
      final Slice slice = entry.getValue();
      log.info("Checking: {} -> {}", shardName, slice);
      final Replica leader = entry.getValue().getLeader();
      try (SolrClient leaderClient = getHttpSolrClient(leader.getCoreUrl())) {
        final SolrDocumentList leaderResults = leaderClient.query(perReplicaParams).getResults();
        log.debug("Shard {}: Leader results: {}", shardName, leaderResults);
        for (Replica replica : slice) {
          try (SolrClient replicaClient = getHttpSolrClient(replica.getCoreUrl())) {
            final SolrDocumentList replicaResults =
                replicaClient.query(perReplicaParams).getResults();
            if (log.isDebugEnabled()) {
              log.debug(
                  "Shard {}: Replica ({}) results: {}",
                  shardName,
                  replica.getCoreName(),
                  replicaResults);
            }
            assertEquals(
                "inconsistency w/leader: shard=" + shardName + "core=" + replica.getCoreName(),
                Collections.emptySet(),
                CloudInspectUtil.showDiff(
                    leaderResults,
                    replicaResults,
                    shardName + " leader: " + leader.getCoreUrl(),
                    shardName + ": " + replica.getCoreUrl()));
          }
        }
      }
    }
  }
}
