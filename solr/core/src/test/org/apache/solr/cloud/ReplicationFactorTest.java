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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests a client application's ability to get replication factor information back from the cluster
 * after an add or update.
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class ReplicationFactorTest extends AbstractFullDistribZkTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ReplicationFactorTest() {
    super();
    sliceCount = 3;
    fixShardCount(3);
  }

  /**
   * Overrides the parent implementation so that we can configure a socket proxy to sit in front of
   * each Jetty server, which gives us the ability to simulate network partitions without having to
   * fuss with IPTables (which is not very cross-platform friendly).
   */
  @Override
  public JettySolrRunner createJetty(
      File solrHome,
      String dataDir,
      String shardList,
      String solrConfigOverride,
      String schemaOverride,
      Replica.Type replicaType)
      throws Exception {

    return createProxiedJetty(
        solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, replicaType);
  }

  @Test
  public void test() throws Exception {
    log.info("replication factor test running");
    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    // test a 1x3 collection
    log.info("Testing replication factor handling for repfacttest_c8n_1x3");
    testRf3();

    waitForThingsToLevelOut(30, TimeUnit.SECONDS);

    // test handling when not using direct updates
    log.info("Now testing replication factor handling for repfacttest_c8n_2x2");
    testRf2NotUsingDirectUpdates();

    waitForThingsToLevelOut(30, TimeUnit.SECONDS);
    if (log.isInfoEnabled()) {
      log.info(
          "replication factor testing complete! final clusterState is: {}",
          cloudClient.getClusterState());
    }
  }

  protected void testRf2NotUsingDirectUpdates() throws Exception {
    int numShards = 2;
    int replicationFactor = 2;
    String testCollectionName = "repfacttest_c8n_2x2";
    String shardId = "shard1";

    createCollectionWithRetry(testCollectionName, numShards, replicationFactor);

    List<Replica> replicas =
        ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
    assertEquals("Expected active 1 replicas for " + testCollectionName, 1, replicas.size());

    List<SolrInputDocument> batch = new ArrayList<>(10);
    for (int i = 0; i < 15; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }

    // send directly to the leader using HttpSolrServer instead of CloudSolrServer (to test support
    // for non-direct updates)
    UpdateRequest up = new UpdateRequest();
    up.add(batch);

    Replica leader = ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, shardId);

    sendNonDirectUpdateRequestReplicaWithRetry(leader, up, 2, testCollectionName);
    sendNonDirectUpdateRequestReplicaWithRetry(replicas.get(0), up, 2, testCollectionName);

    // Ensure nothing is tricky about a delete operation where only one shard needs to delete
    // anything.
    sendNonDirectDeletesRequestReplicaWithRetry(
        leader, getSomeIds(1), 2, getSomeIds(1), 2, testCollectionName);
    sendNonDirectDeletesRequestReplicaWithRetry(
        replicas.get(0), getSomeIds(1), 2, getSomeIds(1), 2, testCollectionName);

    sendNonDirectDeletesRequestReplicaWithRetry(
        leader, getSomeIds(2), 2, getSomeIds(2), 2, testCollectionName);
    sendNonDirectDeletesRequestReplicaWithRetry(
        replicas.get(0), getSomeIds(2), 2, getSomeIds(2), 2, testCollectionName);

    // so now kill the replica of shard2 and verify the achieved rf is only 1
    List<Replica> shard2Replicas =
        ensureAllReplicasAreActive(testCollectionName, "shard2", numShards, replicationFactor, 30);
    assertEquals("Expected active 1 replicas for " + testCollectionName, 1, replicas.size());

    getProxyForReplica(shard2Replicas.get(0)).close();

    Thread.sleep(2000);

    // shard1 will have rf=2 but shard2 will only have rf=1
    sendNonDirectUpdateRequestReplicaWithRetry(leader, up, 1, testCollectionName);
    sendNonDirectUpdateRequestReplicaWithRetry(replicas.get(0), up, 1, testCollectionName);

    // Whether the replication factor is 1 or 2 in the delete-by-id case depends on whether the doc
    // IDs happen to fall on a single shard or not.

    Set<Integer> byIDs;
    byIDs = getSomeIds(2);
    sendNonDirectDeletesRequestReplicaWithRetry(
        leader,
        byIDs,
        calcByIdRf(byIDs, testCollectionName, "shard2"),
        getSomeIds(2),
        1,
        testCollectionName);
    byIDs = getSomeIds(2);
    sendNonDirectDeletesRequestReplicaWithRetry(
        replicas.get(0),
        byIDs,
        calcByIdRf(byIDs, testCollectionName, "shard2"),
        getSomeIds(2),
        1,
        testCollectionName);
    // heal the partition
    getProxyForReplica(shard2Replicas.get(0)).reopen();
    Thread.sleep(2000);
  }

  // When doing a delete by id operation, it's tricky, very tricky. If any document we're deleting
  // by ID goes
  // to shardWithOne, then the replication factor we return will be 1.
  private int calcByIdRf(Set<Integer> byIDs, String testCollectionName, String shardWithOne) {
    ZkController zkController = jettys.get(0).getCoreContainer().getZkController();
    DocCollection coll = zkController.getClusterState().getCollection(testCollectionName);
    int retval = 2;
    for (int id : byIDs) {
      DocRouter router = coll.getRouter();
      if (shardWithOne.equals(
          router.getTargetSlice(Integer.toString(id), null, null, null, coll).getName())) {
        retval = 1;
      }
    }
    return retval;
  }

  // Get the delete tests to use disjoint documents although
  int idFloor = random().nextInt(100) + 1000;

  // Randomize documents, so we exercise requests landing on replicas that have (or don't)
  // particular
  // documents. Yeah, this will go on forever if you ask for more than 100, but it suffices.
  private Set<Integer> getSomeIds(int count) {
    Set<Integer> ids = new HashSet<>();
    while (ids.size() < count) {
      ids.add(idFloor + random().nextInt(100));
    }
    idFloor += 100 + count;
    return ids;
  }

  protected void sendNonDirectDeletesRequestReplicaWithRetry(
      Replica rep,
      Set<Integer> byIdsSet,
      int expectedRfByIds,
      Set<Integer> byQueriesSet,
      int expectedRfDBQ,
      String collectionName)
      throws Exception {
    // First add the docs indicated
    List<String> byIdsList = new ArrayList<>();
    List<String> byQueryList = new ArrayList<>();
    List<SolrInputDocument> batch = new ArrayList<>(10);
    for (int myId : byIdsSet) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, myId);
      doc.addField("a_t", "hello" + id);
      batch.add(doc);
      byIdsList.add(Integer.toString(myId));
    }
    for (int myId : byQueriesSet) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, myId);
      doc.addField("a_t", "hello" + id);
      batch.add(doc);
      byQueryList.add(Integer.toString(myId));
    }

    // Add the docs.
    sendDocsWithRetry(collectionName, batch, expectedRfDBQ, 5, 1);

    // Delete the docs by ID indicated
    UpdateRequest req = new UpdateRequest();
    req.deleteById(byIdsList);
    sendNonDirectUpdateRequestReplicaWithRetry(rep, req, expectedRfByIds, collectionName);

    // Delete the docs by query indicated.
    req = new UpdateRequest();
    req.deleteByQuery("id:(" + String.join(" OR ", byQueryList) + ")");
    sendNonDirectUpdateRequestReplicaWithRetry(rep, req, expectedRfDBQ, collectionName);
  }

  protected void sendNonDirectUpdateRequestReplicaWithRetry(
      Replica replica, UpdateRequest up, int expectedRf, String collection) throws Exception {
    try {
      sendNonDirectUpdateRequestReplica(replica, up, expectedRf, collection);
      Thread.sleep(100); // Let the system settle down before retrying
    } catch (Exception e) {
      sendNonDirectUpdateRequestReplica(replica, up, expectedRf, collection);
    }
  }

  protected void sendNonDirectUpdateRequestReplica(
      Replica replica, UpdateRequest up, int expectedRf, String collection) throws Exception {
    ZkCoreNodeProps zkProps = new ZkCoreNodeProps(replica);
    String url = zkProps.getBaseUrl() + "/" + collection;
    try (SolrClient solrServer = getHttpSolrClient(url)) {
      NamedList<?> resp = solrServer.request(up);
      NamedList<?> hdr = (NamedList<?>) resp.get("responseHeader");
      Integer batchRf = (Integer) hdr.get(UpdateRequest.REPFACT);
      // Note that this also tests if we're wonky and return an achieved rf greater than the number
      // of live replicas.
      assertEquals(
          "Expected rf="
              + expectedRf
              + " for batch but got "
              + batchRf
              + "; clusterState: "
              + printClusterStateInfo(),
          (int) batchRf,
          expectedRf);
    }
  }

  protected void testRf3() throws Exception {
    final int numShards = 1;
    final int replicationFactor = 3;
    final String testCollectionName = "repfacttest_c8n_1x3";
    final String shardId = "shard1";
    final int minRf = 2;

    createCollectionWithRetry(testCollectionName, numShards, replicationFactor);

    List<Replica> replicas =
        ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
    assertEquals("Expected 2 active replicas for " + testCollectionName, 2, replicas.size());

    log.info("Indexing docId=1");
    int rf = sendDoc(testCollectionName, 1);
    assertRf(3, "all replicas should be active", rf);

    // Uses cloudClient to do it's work
    doDBIdWithRetry(
        testCollectionName, 3, 5, "deletes should have propagated to all 3 replicas", 1);
    doDBQWithRetry(testCollectionName, 3, 5, "deletes should have propagated to all 3 replicas", 1);

    log.info("Closing one proxy port");
    getProxyForReplica(replicas.get(0)).close();

    log.info("Indexing docId=2");
    rf = sendDoc(testCollectionName, 2);
    assertRf(2, "one replica should be down", rf);

    // Uses cloudClient to do it's work
    doDBQWithRetry(testCollectionName, 2, 5, "deletes should have propagated to 2 replicas", 1);
    doDBIdWithRetry(testCollectionName, 2, 5, "deletes should have propagated to 2 replicas", 1);

    // SOLR-13599 sanity check if problem is related to sending a batch
    List<SolrInputDocument> batch = new ArrayList<>(15);
    for (int i = 30; i < 45; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }
    log.info("Indexing batch of documents (30-45)");
    int batchRf = sendDocsWithRetry(testCollectionName, batch, minRf, 5, 1);
    assertRf(2, "batch should have succeeded, only one replica should be down", batchRf);

    log.info("Closing second proxy port");
    getProxyForReplica(replicas.get(1)).close();

    log.info("Indexing docId=3");
    rf = sendDoc(testCollectionName, 3);
    assertRf(1, "both replicas should be down", rf);

    doDBQWithRetry(testCollectionName, 1, 5, "deletes should have propagated to only 1 replica", 1);
    doDBIdWithRetry(
        testCollectionName, 1, 5, "deletes should have propagated to only 1 replica", 1);

    // heal the partitions
    log.info("Re-opening closed proxy ports");
    getProxyForReplica(replicas.get(0)).reopen();
    getProxyForReplica(replicas.get(1)).reopen();

    Thread.sleep(2000); // give time for the healed partition to get propagated

    ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);

    log.info("Indexing docId=4");
    rf = sendDoc(testCollectionName, 4);
    assertRf(3, "all replicas have been healed", rf);

    doDBQWithRetry(testCollectionName, 3, 5, "delete should have propagated to all 3 replicas", 1);
    doDBIdWithRetry(testCollectionName, 3, 5, "delete should have propagated to all 3 replicas", 1);

    // now send a batch
    batch = new ArrayList<>(10);
    for (int i = 5; i < 15; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }

    log.info("Indexing batch of documents (5-14)");
    batchRf = sendDocsWithRetry(testCollectionName, batch, minRf, 5, 1);
    assertRf(3, "batch add should have succeeded on all replicas", batchRf);

    doDBQWithRetry(
        testCollectionName, 3, 5, "batch deletes should have propagated to all 3 replica", 15);
    doDBIdWithRetry(
        testCollectionName, 3, 5, "batch deletes should have propagated to all 3 replica", 15);

    // add some chaos to the batch
    log.info("Closing one proxy port (again)");
    getProxyForReplica(replicas.get(0)).close();

    // send a single doc (again)
    // SOLR-13599 sanity check if problem is related to "re-closing" a port on the proxy
    log.info("Indexing docId=5");
    rf = sendDoc(testCollectionName, 5);
    assertRf(2, "doc should have succeeded, only one replica should be down", rf);

    // now send a batch (again)
    batch = new ArrayList<>(10);
    for (int i = 15; i < 30; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }
    log.info("Indexing batch of documents (15-29)");
    batchRf = sendDocsWithRetry(testCollectionName, batch, minRf, 5, 1);
    assertRf(2, "batch should have succeeded, only one replica should be down", batchRf);

    doDBQWithRetry(
        testCollectionName, 2, 5, "deletes should have propagated to only 1 replica", 15);
    doDBIdWithRetry(
        testCollectionName, 2, 5, "deletes should have propagated to only 1 replica", 15);

    // close the 2nd replica, and send a 3rd batch with expected achieved rf=1
    log.info("Closing second proxy port (again)");
    getProxyForReplica(replicas.get(1)).close();

    batch = new ArrayList<>(10);
    for (int i = 30; i < 45; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, String.valueOf(i));
      doc.addField("a_t", "hello" + i);
      batch.add(doc);
    }

    batchRf = sendDocsWithRetry(testCollectionName, batch, minRf, 5, 1);
    assertRf(
        1,
        "batch should have succeeded on the leader only (both replicas should be down)",
        batchRf);

    doDBQWithRetry(
        testCollectionName, 1, 5, "deletes should have propagated to only 1 replica", 15);
    doDBIdWithRetry(
        testCollectionName, 1, 5, "deletes should have propagated to only 1 replica", 15);

    getProxyForReplica(replicas.get(0)).reopen();
    getProxyForReplica(replicas.get(1)).reopen();

    Thread.sleep(2000);
    ensureAllReplicasAreActive(testCollectionName, shardId, numShards, replicationFactor, 30);
  }

  protected void addDocs(String collection, Set<Integer> docIds, int expectedRf, int retries)
      throws Exception {

    Integer[] idList = docIds.toArray(new Integer[0]);
    if (idList.length == 1) {
      sendDoc(collection, idList[0]);
      return;
    }
    List<SolrInputDocument> batch = new ArrayList<>(10);
    for (int docId : idList) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField(id, docId);
      doc.addField("a_t", "hello" + docId);
      batch.add(doc);
    }
    sendDocsWithRetry(collection, batch, expectedRf, retries, 1);
  }

  protected void doDBQWithRetry(
      String collection, int expectedRf, int retries, String msg, int docsToAdd) throws Exception {
    Set<Integer> docIds = getSomeIds(docsToAdd);
    addDocs(collection, docIds, expectedRf, retries);
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(
        "id:(" + docIds.stream().map(String::valueOf).collect(Collectors.joining(" OR ")) + ")");
    doDelete(collection, req, msg, expectedRf, retries);
  }

  protected void doDBIdWithRetry(
      String collection, int expectedRf, int retries, String msg, int docsToAdd) throws Exception {
    Set<Integer> docIds = getSomeIds(docsToAdd);
    addDocs(collection, docIds, expectedRf, retries);
    UpdateRequest req = new UpdateRequest();
    req.deleteById(docIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
    doDelete(collection, req, msg, expectedRf, retries);
  }

  protected void doDelete(
      String collection, UpdateRequest req, String msg, int expectedRf, int retries)
      throws IOException, SolrServerException, InterruptedException {
    int achievedRf = -1;
    for (int idx = 0; idx < retries; ++idx) {
      NamedList<Object> response = cloudClient.request(req, collection);
      achievedRf = cloudClient.getMinAchievedReplicationFactor(collection, response);
      if (achievedRf == expectedRf) return;
      Thread.sleep(1000);
    }
    assertEquals(msg, expectedRf, achievedRf);
  }

  protected int sendDoc(String collectionName, int docId) throws Exception {
    UpdateRequest up = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);
    up.add(doc);
    return runAndGetAchievedRf(collectionName, up);
  }

  private int runAndGetAchievedRf(String collectionName, UpdateRequest up)
      throws SolrServerException, IOException {
    NamedList<Object> response = cloudClient.request(up, collectionName);
    return cloudClient.getMinAchievedReplicationFactor(collectionName, response);
  }

  protected void assertRf(int expected, String explain, int actual) throws Exception {
    if (actual != expected) {
      String assertionFailedMessage =
          String.format(
              Locale.ENGLISH, "Expected rf=%d because %s but got %d", expected, explain, actual);
      fail(assertionFailedMessage + "; clusterState: " + printClusterStateInfo());
    }
  }

  void createCollectionWithRetry(String testCollectionName, int numShards, int replicationFactor)
      throws IOException, SolrServerException, InterruptedException, TimeoutException {
    CollectionAdminResponse resp =
        createCollection(testCollectionName, "conf1", numShards, replicationFactor);

    if (resp.getResponse().get("failure") != null) {
      Thread.sleep(5000); // let system settle down. This should be very rare.

      CollectionAdminRequest.deleteCollection(testCollectionName).process(cloudClient);

      resp = createCollection(testCollectionName, "conf1", numShards, replicationFactor);

      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
  }
}
