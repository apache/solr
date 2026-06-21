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
package org.apache.solr.cloud.api.collections;

import org.apache.solr.SolrTestCaseUtil;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests the Cloud Collections API.
 */
public class CollectionsAPIDistributedZkTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static volatile String configSet = "cloud-minimal";
  protected static String getConfigSet() {
    return configSet;
  }

  @BeforeClass
  public static void beforeCollectionsAPIDistributedZkTest() throws Exception {
    // we don't want this test to have zk timeouts
    System.setProperty("zkClientTimeout", "60000");
    if (TEST_NIGHTLY) {
      System.setProperty("createCollectionWaitTimeTillActive", "10");
      TestInjection.randomDelayInCoreCreation = "true:5";
    }

    configureCluster(TEST_NIGHTLY ? 4 : 2)
        .addConfig("conf", SolrTestUtil.configset(getConfigSet()))
        .addConfig("conf2", SolrTestUtil.configset(getConfigSet()))
        .withSolrXml(SolrTestUtil.TEST_PATH().resolve("solr.xml"))
        .configure();
  }
  
  @AfterClass
  public static void afterCollectionsAPIDistributedZkTest() throws Exception {
    if (cluster != null) cluster.shutdown();
    cluster = null;
  }

  @Test
  public void testBadActionNames() {
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testNoConfigSetExist() throws Exception {
    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      CollectionAdminRequest.createCollection("noconfig", "conf123", 1, 1).process(cluster.getSolrClient());
    });

    // in both cases, the collection should have default to the core name
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains("noconfig"));
  }

  @Test
  public void testMissingRequiredParameters() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("numShards", 2);
    // missing required collection parameter
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }


  @Test
  public void testZeroNumShards() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("name", "acollection");
    params.set(REPLICATION_FACTOR, 10);
    params.set("numShards", 0);
    params.set("collection.configName", "conf");

    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testoMissingNumShards() {
    // No numShards should fail
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("name", "acollection");
    params.set(REPLICATION_FACTOR, 10);
    params.set("collection.configName", "conf");

    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testReadOnlyCollection() throws Exception {
    int NUM_DOCS = 10;
    final String collectionName = "readOnlyTest";
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
            .process(solrClient);

    // Wait for all 4 replicas to be active before indexing: indexing into a collection whose replicas
    // are still coming up races doc forwarding and can leave a replica missing docs, so the later
    // distributed *:* query (which hits one replica per shard) returns fewer than NUM_DOCS.
    cluster.waitForActiveCollection(collectionName, 2, 4);

    solrClient.setDefaultCollection(collectionName);


    // verify that indexing works
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);
    solrClient.commit();
    // verify the docs exist on every replica, not just whichever one a distributed query happens to
    // hit (which can race a follower still catching up under load).
    waitForAllReplicasDocCount(collectionName, NUM_DOCS);

    // index more but don't commit
    docs.clear();
    for (int i = NUM_DOCS; i < NUM_DOCS << 1; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);

    // NOTE (fork): shards are auto-named "s1"/"s2" (not "shard1"/"shard2"). The DocCollection
    // correctly reports s1[leader=...]; using "shard1" here makes getLeaderRetry wait on a
    // non-existent slice and time out.
    Replica leader = solrClient.getZkStateReader().getLeaderRetry(collectionName, "s1", 15000);

    final AtomicReference<Long> coreStartTime = new AtomicReference<>(getCoreStatus(leader).getCoreStartTime().getTime());

    // Check for value change
    CollectionAdminRequest.modifyCollection(collectionName,
            Collections.singletonMap(ZkStateReader.READ_ONLY, "true"))
            .process(solrClient);

    // MODIFYCOLLECTION is applied asynchronously via the overseer/StateUpdates pipeline, so the
    // client's watched cluster state may briefly lag behind the modify response. Wait for the
    // READ_ONLY property to propagate before asserting on it.
    RetryUtil.retryUntil("Timed out waiting for READ_ONLY to propagate", 30, 1000, TimeUnit.MILLISECONDS,
        () -> "true".equals(String.valueOf(solrClient.getZkStateReader().getClusterState()
            .getCollection(collectionName).get(ZkStateReader.READ_ONLY))));

    DocCollection coll = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertNotNull(coll.toString(), coll.get(ZkStateReader.READ_ONLY));
    assertEquals(coll.toString(), coll.get(ZkStateReader.READ_ONLY).toString(), "true");

    // wait for the expected collection reload
    RetryUtil.retryUntil("Timed out waiting for core to reload", 30, 1000, TimeUnit.MILLISECONDS, () -> {
      long restartTime = 0;
      try {
        restartTime = getCoreStatus(leader).getCoreStartTime().getTime();
      } catch (Exception e) {
        log.warn("Exception getting core start time: {}", e.getMessage());
        return false;
      }
      return restartTime > coreStartTime.get();
    });

    coreStartTime.set(getCoreStatus(leader).getCoreStartTime().getTime());

    // Wait for all replicas to be active again after the read-only-induced reload, so the
    // distributed query does not race a replica still recovering/reloading.
    solrClient.getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, (n, c) -> {
      if (c == null) return false;
      for (Slice s : c.getSlices()) {
        for (Replica r : s.getReplicas()) {
          if (n.contains(r.getNodeName()) && !r.isActive(n)) return false;
        }
      }
      return true;
    });

    // check for docs - reloading should have committed the new docs
    // this also verifies that searching works in read-only mode
    waitForAllReplicasDocCount(collectionName, NUM_DOCS * 2);

    // try sending updates
    try {
      solrClient.add(new SolrInputDocument("id", "shouldFail"));
      fail("add() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }
    try {
      solrClient.deleteById("shouldFail");
      fail("deleteById() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }
    try {
      solrClient.deleteByQuery("id:shouldFail");
      fail("deleteByQuery() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }
    try {
      solrClient.commit();
      fail("commit() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }
    try {
      solrClient.optimize();
      fail("optimize() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }
    try {
      solrClient.rollback();
      fail("rollback() should fail in read-only mode");
    } catch (Exception e) {
      // expected - ignore
    }

    // Check for removing value
    // setting to empty string is equivalent to removing the property, see SOLR-12507
    CollectionAdminRequest.modifyCollection(collectionName,
            Collections.singletonMap(ZkStateReader.READ_ONLY, ""))
            .process(cluster.getSolrClient());
    // As with setting it, removal of READ_ONLY propagates asynchronously through the
    // overseer/StateUpdates pipeline; wait for the client's watched state to reflect it.
    RetryUtil.retryUntil("Timed out waiting for READ_ONLY removal to propagate", 30, 1000, TimeUnit.MILLISECONDS,
        () -> cluster.getSolrClient().getZkStateReader().getClusterState()
            .getCollection(collectionName).get(ZkStateReader.READ_ONLY) == null);
    coll = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
    assertNull(coll.toString(), coll.get(ZkStateReader.READ_ONLY));

    // wait for the expected collection reload
    RetryUtil.retryUntil("Timed out waiting for core to reload", 30, 1000, TimeUnit.MILLISECONDS, () -> {
      long restartTime = 0;
      try {
        restartTime = getCoreStatus(leader).getCoreStartTime().getTime();
      } catch (Exception e) {
        log.warn("Exception getting core start time: {}", e.getMessage());
        return false;
      }
      return restartTime > coreStartTime.get();
    });

    // Wait for all replicas to be active again after the read-only-off reload, so the add below
    // does not race a replica that has not yet reloaded out of read-only mode (403).
    cluster.getSolrClient().getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, (n, c) -> {
      if (c == null) return false;
      for (Slice s : c.getSlices()) {
        for (Replica r : s.getReplicas()) {
          if (n.contains(r.getNodeName()) && !r.isActive(n)) return false;
        }
      }
      return true;
    });

    // check that updates are working now
    docs.clear();
    for (int i = NUM_DOCS << 1; i < NUM_DOCS * 3; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);
    solrClient.commit();
    waitForAllReplicasDocCount(collectionName, NUM_DOCS * 3);
  }

  @Test
  public void testDeleteNonExistentCollection() throws Exception {

    SolrTestCaseUtil.expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteCollection("unknown_collection").process(cluster.getSolrClient());
    });

    // create another collection should still work
    CollectionAdminRequest.createCollection("acollectionafterbaddelete", "conf", 1, 2).setMaxShardsPerNode(100)
            .process(cluster.getSolrClient());
  }
}
