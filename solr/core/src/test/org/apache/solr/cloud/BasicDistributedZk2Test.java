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

import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class BasicDistributedZk2Test extends SolrCloudBridgeTestCase {
  private static final String ONE_NODE_COLLECTION = "onenodecollection";
  private final boolean onlyLeaderIndexes = random().nextBoolean();

  @BeforeClass
  public static void setupBasicDistribZk2Test() {
    sliceCount = 2;
    numJettys = 4;
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  @Test
  public void test() throws Exception {
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      testNodeWithoutCollectionForwarding();

      indexr(id, 1, i1, 100, tlong, 100, t1,
          "now is the time for all good men", "foo_f", 1.414f, "foo_b", "true",
          "foo_d", 1.414d);

      commit();

      long docId = testUpdateAndDelete();

      // index a bad doc...
      SolrTestCaseUtil.expectThrows(SolrException.class, () -> indexr(t1, "a doc with no id"));

      // try indexing to a leader that has no replicas up
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.getLeaderRetry(COLLECTION, SHARD2);

      checkShardConsistency(SHARD1);
      checkShardConsistency(SHARD2);

      testFinished = true;
    } finally {
      if (!testFinished) {
        // test failed — log for diagnosis
      }
    }
  }

  private void testNodeWithoutCollectionForwarding() throws Exception {
    assertEquals(0, CollectionAdminRequest
        .createCollection(ONE_NODE_COLLECTION, "_default", 1, 1)
        .setCreateNodeSet("")
        .process(cloudClient).getStatus());
    assertTrue(CollectionAdminRequest
        .addReplicaToShard(ONE_NODE_COLLECTION, SHARD1)
        .setCoreName(ONE_NODE_COLLECTION + "core")
        .process(cloudClient).isSuccess());

    waitForRecoveriesToFinish(ONE_NODE_COLLECTION);

    cloudClient.getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(),
        ONE_NODE_COLLECTION, SHARD1, 30000, true);

    int docs = 2;
    for (SolrClient client : clients) {
      final String clientUrl = getBaseUrl((Http2SolrClient) client);
      addAndQueryDocs(clientUrl, docs);
      docs += 2;
    }
  }

  // 2 docs added every call
  private void addAndQueryDocs(final String baseUrl, int docs)
      throws Exception {

    SolrQuery query = new SolrQuery("*:*");

    try (Http2SolrClient qclient = getClient(baseUrl + "/onenodecollection" + "core")) {

      // add a doc
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docs);
      qclient.add(doc);
      qclient.commit();

      QueryResponse results = qclient.query(query);
      assertEquals(docs - 1, results.getResults().getNumFound());
    }

    try (Http2SolrClient qclient = getClient(baseUrl + "/onenodecollection")) {
      QueryResponse results = qclient.query(query);
      assertEquals(docs - 1, results.getResults().getNumFound());

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", docs + 1);
      qclient.add(doc);
      qclient.commit();

      query = new SolrQuery("*:*");
      query.set("rows", 0);
      results = qclient.query(query);
      assertEquals(docs, results.getResults().getNumFound());
    }
  }

  private long testUpdateAndDelete() throws Exception {
    long docId = 99999999L;
    indexr("id", docId, t1, "originalcontent");

    commit();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", t1 + ":originalcontent");
    QueryResponse results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());

    // update doc
    indexr("id", docId, t1, "updatedcontent");

    commit();

    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());

    params.set("q", t1 + ":updatedcontent");

    results = clients.get(0).query(params);
    assertEquals(1, results.getResults().getNumFound());

    UpdateRequest uReq = new UpdateRequest();
    uReq.deleteById(Long.toString(docId)).process(clients.get(0));

    commit();

    results = clients.get(0).query(params);
    assertEquals(0, results.getResults().getNumFound());
    return docId;
  }

  private void testDebugQueries() throws Exception {
    handle.put("explain", SKIPVAL);
    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);
    handle.put("track", SKIP);
    query("q", "now their fox sat had put", "fl", "*,score",
        CommonParams.DEBUG_QUERY, "true");
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG_QUERY, "true");
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY);
  }

}
