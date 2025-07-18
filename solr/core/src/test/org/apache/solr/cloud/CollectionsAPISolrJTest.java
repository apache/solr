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

import static java.util.Arrays.asList;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.NUM_SHARDS_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.DEFAULTS;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionsAPISolrJTest extends SolrCloudTestCase {
  private static final int TIMEOUT = 3000;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTest() throws Exception {
    // System.setProperty("metricsEnabled", "true");
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .addConfig("conf2", configset("cloud-dynamic"))
        .configure();
  }

  /**
   * When a config name is not specified during collection creation, the _default should be used.
   */
  @Test
  public void testCreateWithDefaultConfigSet() throws Exception {
    String collectionName = getSaferTestName();
    CollectionAdminResponse response =
        CollectionAdminRequest.createCollection(collectionName, 2, 2)
            .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 4);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (String coreName : coresStatus.keySet()) {
      NamedList<Integer> status = coresStatus.get(coreName);
      assertEquals(0, (int) status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }
    // Sometimes multiple cores land on the same node so it's less than 4
    int nodesCreated = response.getCollectionNodesStatus().size();
    // Use of _default configset should generate a warning for data-driven functionality in
    // production use
    assertTrue(
        response.getWarning() != null
            && response.getWarning().contains("NOT RECOMMENDED for production use"));

    response =
        CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(nodesStatus.toString(), nodesCreated, nodesStatus.size());

    waitForState(
        "Expected " + collectionName + " to disappear from cluster state",
        collectionName,
        Objects::isNull);
  }

  @Test
  public void testCreateCollWithDefaultClusterPropertiesNewFormat() throws Exception {
    String COLL_NAME = "CollWithDefaultClusterProperties";
    try {
      V2Response rsp =
          new V2Request.Builder("/cluster/properties")
              .withMethod(SolrRequest.METHOD.PUT)
              .withPayload(
                  "{\"defaults\": {\"collection\": {\"numShards\": 2, \"nrtReplicas\": 2}}}")
              .build()
              .process(cluster.getSolrClient());

      for (int i = 0; i < 300; i++) {
        Map<?, ?> m = cluster.getZkStateReader().getClusterProperty(DEFAULTS, null);
        if (m != null) break;
        Thread.sleep(10);
      }
      Object clusterProperty =
          cluster
              .getZkStateReader()
              .getClusterProperty(List.of(DEFAULTS, COLLECTION, NUM_SHARDS_PROP), null);
      assertEquals("2", String.valueOf(clusterProperty));
      clusterProperty =
          cluster
              .getZkStateReader()
              .getClusterProperty(List.of(DEFAULTS, COLLECTION, NRT_REPLICAS), null);
      assertEquals("2", String.valueOf(clusterProperty));
      CollectionAdminResponse response =
          CollectionAdminRequest.createCollection(COLL_NAME, "conf", null, null, null, null)
              .process(cluster.getSolrClient());
      assertEquals(0, response.getStatus());
      assertTrue(response.isSuccess());
      cluster.waitForActiveCollection(COLL_NAME, 2, 4);

      DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(COLL_NAME);
      Map<String, Slice> slices = coll.getSlicesMap();
      assertEquals(2, slices.size());
      for (Slice slice : slices.values()) {
        assertEquals(2, slice.getReplicas().size());
      }
      CollectionAdminRequest.deleteCollection(COLL_NAME).process(cluster.getSolrClient());

      // unset only a single value
      rsp =
          new V2Request.Builder("/cluster/properties")
              .withMethod(SolrRequest.METHOD.PUT)
              .withPayload(
                  "{\n"
                      + "  \"defaults\" : {\n"
                      + "    \"collection\": {\n"
                      + "      \"nrtReplicas\": null\n"
                      + "    }\n"
                      + "  }\n"
                      + "}")
              .build()
              .process(cluster.getSolrClient());
      // we use a timeout so that the change made in ZK is reflected in the watched copy inside
      // ZkStateReader
      TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, new TimeSource.NanoTimeSource());
      while (!timeOut.hasTimedOut()) {
        clusterProperty =
            cluster
                .getZkStateReader()
                .getClusterProperty(List.of(DEFAULTS, COLLECTION, NRT_REPLICAS), null);
        if (clusterProperty == null) break;
      }
      assertNull(clusterProperty);

      rsp =
          new V2Request.Builder("/cluster/properties")
              .withMethod(SolrRequest.METHOD.PUT)
              .withPayload("{\"defaults\": {\"collection\": null}}")
              .build()
              .process(cluster.getSolrClient());
      // assert that it is really gone in both old and new paths
      timeOut = new TimeOut(5, TimeUnit.SECONDS, new TimeSource.NanoTimeSource());
      while (!timeOut.hasTimedOut()) {
        clusterProperty =
            cluster
                .getZkStateReader()
                .getClusterProperty(List.of(DEFAULTS, COLLECTION, NUM_SHARDS_PROP), null);
        if (clusterProperty == null) break;
      }
      assertNull(clusterProperty);
    } finally {
      V2Response rsp =
          new V2Request.Builder("/cluster/properties")
              .withMethod(SolrRequest.METHOD.PUT)
              .withPayload("{\"defaults\": null}")
              .build()
              .process(cluster.getSolrClient());
    }
  }

  @Test
  public void testCreateAndDeleteCollection() throws Exception {
    String collectionName = getSaferTestName();
    CollectionAdminResponse response =
        CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(4, coresStatus.size());
    for (String coreName : coresStatus.keySet()) {
      NamedList<Integer> status = coresStatus.get(coreName);
      assertEquals(0, (int) status.get("status"));
      assertTrue(status.get("QTime") > 0);
    }

    // Sometimes multiple cores land on the same node so it's less than 4
    int nodesCreated = response.getCollectionNodesStatus().size();
    response =
        CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    // Delete could have been sent before the collection was finished coming online
    assertEquals(nodesStatus.toString(), nodesCreated, nodesStatus.size());

    waitForState(
        "Expected " + collectionName + " to disappear from cluster state",
        collectionName,
        Objects::isNull);

    // Test Creating a new collection.
    collectionName = "solrj_test2";

    response =
        CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState(
        "Expected " + collectionName + " to appear in cluster state",
        collectionName,
        Objects::nonNull);
  }

  @Test
  public void testCloudInfoInCoreStatus() throws IOException, SolrServerException {
    String collectionName = getSaferTestName();
    CollectionAdminResponse response =
        CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    cluster.waitForActiveCollection(collectionName, 2, 4);

    String nodeName = response._getStr("success[0]/key");
    String corename = response._getStr(asList("success", nodeName, "core"), null);

    try (SolrClient coreClient =
        getHttpSolrClient(cluster.getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(corename, coreClient);
      assertEquals(
          collectionName, status._get(asList("status", corename, "cloud", "collection"), null));
      assertNotNull(status._get(asList("status", corename, "cloud", "shard"), null));
      assertNotNull(status._get(asList("status", corename, "cloud", "replica"), null));
    }
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-13021")
  public void testCreateAndDeleteShard() throws Exception {
    // Create an implicit collection
    String collectionName = "solrj_implicit";
    CollectionAdminResponse response =
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                collectionName, "conf", "shardA,shardB", 1, 1, 1)
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    cluster.waitForActiveCollection(collectionName, 2, 6);

    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(6, coresStatus.size());

    // Add a shard to the implicit collection
    response =
        CollectionAdminRequest.createShard(collectionName, "shardC")
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState(
        "Wait for shard to be visible",
        collectionName,
        c -> c != null && c.getSlice("shardC") != null);
    coresStatus = response.getCollectionCoresStatus();
    assertEquals(3, coresStatus.size());
    int replicaTlog = 0;
    int replicaNrt = 0;
    int replicaPull = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("shardC_replica_t")) replicaTlog++;
      else if (coreName.contains("shardC_replica_n")) replicaNrt++;
      else replicaPull++;
    }
    assertEquals(1, replicaNrt);
    assertEquals(1, replicaTlog);
    assertEquals(1, replicaPull);

    response =
        CollectionAdminRequest.deleteShard(collectionName, "shardC")
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> nodesStatus = response.getCollectionNodesStatus();
    assertEquals(1, nodesStatus.size());
  }

  @Test
  public void testCreateAndDeleteAlias() throws IOException, SolrServerException {

    final String collection = "aliasedCollection";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());

    CollectionAdminResponse response =
        CollectionAdminRequest.createAlias("solrj_alias", collection)
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    response = CollectionAdminRequest.deleteAlias("solrj_alias").process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
  }

  @Test
  public void testSplitShard() throws Exception {
    String collectionName = getSaferTestName();
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 2);

    CollectionAdminResponse response =
        CollectionAdminRequest.splitShard(collectionName)
            .setShardName("shard1")
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    int shard10 = 0;
    int shard11 = 0;
    for (String coreName : coresStatus.keySet()) {
      assertEquals(0, (int) coresStatus.get(coreName).get("status"));
      if (coreName.contains("_shard1_0")) shard10++;
      else shard11++;
    }
    assertEquals(1, shard10);
    assertEquals(1, shard11);

    waitForState(
        "Expected all shards to be active and parent shard to be removed",
        collectionName,
        (n, c) -> {
          if (c.getSlice("shard1").getState() == Slice.State.ACTIVE) return false;
          for (Replica r : c.getReplicas()) {
            if (r.isActive(n) == false) return false;
          }
          return true;
        });

    // Test splitting using split.key
    response =
        CollectionAdminRequest.splitShard(collectionName)
            .setSplitKey("b!")
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    waitForState(
        "Expected 5 slices to be active", collectionName, c -> c.getActiveSlices().size() == 5);
  }

  @Test
  public void testCreateCollectionWithPropertyParam() throws Exception {
    String collectionName = getSaferTestName();

    Path tmpDir = createTempDir("testPropertyParamsForCreate");
    Path dataDir = tmpDir.resolve("dataDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    Path ulogDir = tmpDir.resolve("ulogDir-" + TestUtil.randomSimpleString(random(), 1, 5));
    cluster.getJettySolrRunners().forEach(j -> j.getCoreContainer().getAllowPaths().add(tmpDir));

    CollectionAdminResponse response =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
            .withProperty(CoreAdminParams.DATA_DIR, dataDir.toString())
            .withProperty(CoreAdminParams.ULOG_DIR, ulogDir.toString())
            .process(cluster.getSolrClient());

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    cluster.waitForActiveCollection(collectionName, 1, 1);

    Map<String, NamedList<Integer>> coresStatus = response.getCollectionCoresStatus();
    assertEquals(1, coresStatus.size());

    DocCollection testCollection = getCollectionState(collectionName);

    Replica replica1 = testCollection.getReplicas().iterator().next();
    final var coreStatus = getCoreStatus(replica1);

    assertEquals(Path.of(coreStatus.dataDir).toString(), dataDir.toString());
  }

  @Test
  public void testAddAndDeleteReplica() throws Exception {
    String collectionName = getSaferTestName();
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 2);

    ArrayList<String> nodeList =
        new ArrayList<>(cluster.getSolrClient().getClusterState().getLiveNodes());
    Collections.shuffle(nodeList, random());
    final String node = nodeList.get(0);

    CollectionAdminResponse response =
        CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
            .setNode(node)
            .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 3);

    Replica newReplica = grabNewReplica(response, getCollectionState(collectionName));
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    assertEquals(newReplica.getNodeName(), node);

    // Test DELETEREPLICA
    response =
        CollectionAdminRequest.deleteReplica(collectionName, "shard1", newReplica.getName())
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState(
        "Expected replica " + newReplica.getName() + " to vanish from cluster state",
        collectionName,
        c -> c.getSlice("shard1").getReplica(newReplica.getName()) == null);
  }

  private Replica grabNewReplica(CollectionAdminResponse response, DocCollection docCollection) {
    String replicaName = response.getCollectionCoresStatus().keySet().iterator().next();
    Optional<Replica> optional =
        docCollection.getReplicas().stream()
            .filter(replica -> replicaName.equals(replica.getCoreName()))
            .findAny();
    if (optional.isPresent()) {
      return optional.get();
    }
    throw new AssertionError("Can not find " + replicaName + " from " + docCollection);
  }

  @Test
  public void testClusterProp() throws IOException, SolrServerException {

    // sanity check our expected default
    final ClusterProperties props = new ClusterProperties(zkClient());

    CollectionAdminResponse response =
        CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, "42")
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals(
        "Cluster property was not set",
        "42",
        props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null));

    // Unset ClusterProp that we set.
    CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null)
        .process(cluster.getSolrClient());
    assertNull(
        "Cluster property was not unset",
        props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null));

    response =
        CollectionAdminRequest.setClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, "1")
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertEquals(
        "Cluster property was not set",
        "1",
        props.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, null));
  }

  @Test
  public void testCollectionProp() throws InterruptedException, IOException, SolrServerException {
    String collectionName = getSaferTestName();
    final String propName = "testProperty";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 4);

    // Check for value change
    CollectionAdminRequest.setCollectionProperty(collectionName, propName, "false")
        .process(cluster.getSolrClient());
    checkCollectionProperty(collectionName, propName, "false");

    // Check for removing value
    CollectionAdminRequest.setCollectionProperty(collectionName, propName, null)
        .process(cluster.getSolrClient());
    checkCollectionProperty(collectionName, propName, null);
  }

  private void checkCollectionProperty(String collection, String propertyName, String propertyValue)
      throws InterruptedException {
    TimeOut timeout = new TimeOut(TIMEOUT, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      Thread.sleep(10);
      if (Objects.equals(
          cluster.getZkStateReader().getCollectionProperties(collection).get(propertyName),
          propertyValue)) {
        return;
      }
    }

    fail("Timed out waiting for cluster property value");
  }

  private void indexSomeDocs(String collectionName) throws SolrServerException, IOException {
    SolrClient client = cluster.getSolrClient();
    byte[] binData = collectionName.getBytes(StandardCharsets.UTF_8);
    // index some docs
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      doc.addField("number_i", i);
      doc.addField("number_l", i);
      doc.addField("number_f", i);
      doc.addField("number_d", i);
      doc.addField("number_ti", i);
      doc.addField("number_tl", i);
      doc.addField("number_tf", i);
      doc.addField("number_td", i);
      doc.addField("store", (i * 5) + "," + (i * 5));
      doc.addField("boolean_b", true);
      doc.addField("multi_int_with_docvals", i);
      doc.addField("string_s", String.valueOf(i));
      doc.addField("tv_mv_string", "this is a test " + i);
      doc.addField("timestamp_dt", new Date());
      doc.addField("timestamp_tdt", new Date());
      doc.addField("payload", binData);
      client.add(collectionName, doc);
    }
    client.commit(collectionName);
  }

  private void assertRspPathNull(SolrResponse rsp, String... pathSegments) {
    assertNull(Utils.getObjectByPath(rsp.getResponse(), false, Arrays.asList(pathSegments)));
  }

  private void assertRspPathNotNull(SolrResponse rsp, String... pathSegments) {
    assertNotNull(Utils.getObjectByPath(rsp.getResponse(), false, Arrays.asList(pathSegments)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testColStatus() throws Exception {
    String collectionName = getSaferTestName();
    CollectionAdminRequest.createCollection(collectionName, "conf2", 2, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 4);
    indexSomeDocs(collectionName);

    // Returns basic info if no additional flags are set
    CollectionAdminRequest.ColStatus req = CollectionAdminRequest.collectionStatus(collectionName);
    CollectionAdminResponse rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    assertNotNull(rsp.getResponse().get(collectionName));
    assertNotNull(rsp.getResponse()._get(List.of(collectionName, "properties"), null));
    final var collPropMap =
        (Map<String, Object>) rsp.getResponse()._get(List.of(collectionName, "properties"), null);
    assertEquals("conf2", collPropMap.get("configName"));
    assertEquals(2L, collPropMap.get("nrtReplicas"));
    assertEquals("0", collPropMap.get("tlogReplicas"));
    assertEquals("0", collPropMap.get("pullReplicas"));
    assertEquals(
        2,
        ((NamedList<Object>) rsp.getResponse()._get(List.of(collectionName, "shards"), null))
            .size());
    assertNotNull(
        rsp.getResponse()._get(List.of(collectionName, "shards", "shard1", "leader"), null));
    // Ensure more advanced info is not returned
    assertNull(
        rsp.getResponse()
            ._get(List.of(collectionName, "shards", "shard1", "leader", "segInfos"), null));

    // Returns segment metadata iff requested
    req = CollectionAdminRequest.collectionStatus(collectionName);
    req.setWithSegments(true);
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    assertNotNull(rsp.getResponse().get(collectionName));
    assertRspPathNotNull(
        rsp, collectionName, "shards", "shard1", "leader", "segInfos", "segments", "_0");
    // Ensure field, size, etc. information isn't returned if only segment data was requested
    assertRspPathNull(
        rsp, collectionName, "shards", "shard1", "leader", "segInfos", "segments", "_0", "fields");
    assertRspPathNull(
        rsp,
        collectionName,
        "shards",
        "shard1",
        "leader",
        "segInfos",
        "segments",
        "_0",
        "largestFiles");

    // Returns segment metadata and file-size info iff requested
    // (Note that 'sizeInfo=true' should implicitly enable segments=true)
    req = CollectionAdminRequest.collectionStatus(collectionName);
    req.setWithSizeInfo(true);
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    assertRspPathNotNull(rsp, collectionName);
    assertRspPathNotNull(
        rsp, collectionName, "shards", "shard1", "leader", "segInfos", "segments", "_0");
    assertRspPathNotNull(
        rsp,
        collectionName,
        "shards",
        "shard1",
        "leader",
        "segInfos",
        "segments",
        "_0",
        "largestFiles");
    // Ensure field, etc. information isn't returned if only segment+size data was requested
    assertRspPathNull(
        rsp, collectionName, "shards", "shard1", "leader", "segInfos", "segments", "_0", "fields");

    // Set all flags and ensure everything is returned as expected
    req = CollectionAdminRequest.collectionStatus(collectionName);
    req.setWithSegments(true);
    req.setWithFieldInfo(true);
    req.setWithCoreInfo(true);
    req.setWithSizeInfo(true);
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    @SuppressWarnings({"unchecked"})
    List<Object> nonCompliant =
        (List<Object>) rsp.getResponse()._get(List.of(collectionName, "schemaNonCompliant"), null);
    assertEquals(nonCompliant.toString(), 1, nonCompliant.size());
    assertTrue(nonCompliant.toString(), nonCompliant.contains("(NONE)"));
    @SuppressWarnings({"unchecked"})
    final var segInfos =
        (Map<String, Object>)
            Utils.getObjectByPath(
                rsp.getResponse(),
                false,
                List.of(collectionName, "shards", "shard1", "leader", "segInfos"));
    assertNotNull(
        Utils.toJSONString(rsp),
        Utils.getObjectByPath(segInfos, false, List.of("info", "core", "startTime")));
    assertNotNull(
        Utils.toJSONString(rsp),
        Utils.getObjectByPath(segInfos, false, List.of("fieldInfoLegend")));
    assertNotNull(
        Utils.toJSONString(rsp),
        Utils.getObjectByPath(segInfos, false, List.of("segments", "_0", "fields", "id", "flags")));

    // test for replicas not active - SOLR-13882
    DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(collectionName);
    Replica firstReplica = coll.getSlice("shard1").getReplicas().iterator().next();
    String firstNode = firstReplica.getNodeName();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getNodeName().equals(firstNode)) {
        cluster.stopJettySolrRunner(jetty);
      }
    }
    rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    Number down =
        (Number)
            Utils.getObjectByPath(
                rsp.getResponse(),
                false,
                List.of(collectionName, "shards", "shard1", "replicas", "down"));
    assertTrue(
        "should be some down replicas, but there were none in shard1:" + rsp, down.intValue() > 0);

    // test for a collection with implicit router
    String implicitColl = "implicitColl";
    CollectionAdminRequest.createCollection(implicitColl, "conf2", 2, 1)
        .setRouterName("implicit")
        .setRouterField("routerField")
        .setShards("shardA,shardB")
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(implicitColl, 2, 2);
    req = CollectionAdminRequest.collectionStatus(implicitColl);
    rsp = req.process(cluster.getSolrClient());
    assertNotNull(rsp.getResponse().get(implicitColl));
    assertRspPathNotNull(rsp, implicitColl, "shards", "shardA");
    assertRspPathNotNull(rsp, implicitColl, "shards", "shardB");
  }

  @Test
  public void testColStatusCollectionName() throws Exception {
    final String[] collectionNames = {"collectionStatusTest_1", "collectionStatusTest_2"};
    for (String collectionName : collectionNames) {
      CollectionAdminRequest.createCollection(collectionName, "conf2", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collectionName, 1, 1);
    }
    // assert only one collection is returned using the solrj colstatus interface
    CollectionAdminRequest.ColStatus req =
        CollectionAdminRequest.collectionStatus(collectionNames[0]);
    CollectionAdminResponse rsp = req.process(cluster.getSolrClient());
    assertNotNull(rsp.getResponse().get(collectionNames[0]));
    assertNull(rsp.getResponse().get(collectionNames[1]));

    req = CollectionAdminRequest.collectionStatus(collectionNames[1]);
    rsp = req.process(cluster.getSolrClient());
    assertNotNull(rsp.getResponse().get(collectionNames[1]));
    assertNull(rsp.getResponse().get(collectionNames[0]));

    // assert passing null collection fails
    expectThrows(
        NullPointerException.class,
        "Passing null to collectionStatus should result in an NPE",
        () -> CollectionAdminRequest.collectionStatus(null));

    // assert passing non-existent collection returns no collections
    req = CollectionAdminRequest.collectionStatus("doesNotExist");
    rsp = req.process(cluster.getSolrClient());
    assertNull(rsp.getResponse().get(collectionNames[0]));
    assertNull(rsp.getResponse().get(collectionNames[1]));

    // assert collectionStatuses returns all collections
    req = CollectionAdminRequest.collectionStatuses();
    rsp = req.process(cluster.getSolrClient());
    assertNotNull(rsp.getResponse().get(collectionNames[1]));
    assertNotNull(rsp.getResponse().get(collectionNames[0]));
  }

  /**
   * Unit test for the v2 API: GET /api/collections/$collName
   *
   * <p>Uses the OAS-generated SolrRequest/SolrResponse API binding.
   */
  @Test
  public void testV2BasicCollectionStatus() throws Exception {
    final String simpleCollName = "simpleCollection";
    CollectionAdminRequest.createCollection(simpleCollName, "conf2", 2, 1, 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(simpleCollName, 2, 6);
    indexSomeDocs(simpleCollName);

    final var simpleResponse =
        new CollectionsApi.GetCollectionStatus(simpleCollName).process(cluster.getSolrClient());
    assertEquals(simpleCollName, simpleResponse.name);
    assertEquals(2, simpleResponse.shards.size());
    assertEquals(Integer.valueOf(2), simpleResponse.activeShards);
    assertEquals(Integer.valueOf(0), simpleResponse.inactiveShards);
    assertEquals(Integer.valueOf(1), simpleResponse.properties.nrtReplicas);
    assertEquals(Integer.valueOf(1), simpleResponse.properties.replicationFactor);
    assertEquals(Integer.valueOf(1), simpleResponse.properties.pullReplicas);
    assertEquals(Integer.valueOf(1), simpleResponse.properties.tlogReplicas);
    assertNotNull(simpleResponse.shards.get("shard1").leader);
    assertNull(simpleResponse.shards.get("shard1").leader.segInfos);

    // Ensure segment data present when request sets 'segments=true' flag
    final var segmentDataRequest = new CollectionsApi.GetCollectionStatus(simpleCollName);
    segmentDataRequest.setSegments(true);
    final var segmentDataResponse = segmentDataRequest.process(cluster.getSolrClient());
    var segmentData = segmentDataResponse.shards.get("shard1").leader.segInfos;
    assertNotNull(segmentData);
    assertTrue(segmentData.info.numSegments > 0); // Expect at least one segment
    assertEquals(segmentData.info.numSegments.intValue(), segmentData.segments.size());
    assertEquals(Version.LATEST.toString(), segmentData.info.commitLuceneVersion);
    // Ensure field, size, etc. data not provided
    assertNull(segmentData.segments.get("_0").fields);
    assertNull(segmentData.segments.get("_0").largestFilesByName);

    // Ensure file-size data present when request sets sizeInfo flag
    final var segmentFileSizeRequest = new CollectionsApi.GetCollectionStatus(simpleCollName);
    segmentFileSizeRequest.setSizeInfo(true);
    final var segmentFileSizeResponse = segmentFileSizeRequest.process(cluster.getSolrClient());
    segmentData = segmentFileSizeResponse.shards.get("shard1").leader.segInfos;
    assertNotNull(segmentData);
    final var largeFileList = segmentData.segments.get("_0").largestFilesByName;
    assertNotNull(largeFileList);
    // Hard to assert what the largest index files should be, but:
    //   - there should be at least 1 entry and...
    //   - all keys/values should be non-empty
    assertTrue(largeFileList.size() > 0);
    largeFileList.forEach(
        (fileName, size) -> {
          assertThat(fileName, is(not(emptyString())));
          assertThat(size, is(not(emptyString())));
        });
    // Ensure field, etc. data not provided
    assertNull(segmentData.segments.get("_0").fields);
  }

  private static final int NUM_DOCS = 10;

  @Test
  public void testReadOnlyCollection() throws Exception {
    String collectionName = getSaferTestName();

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2)
        .process(cluster.getSolrClient());
    CloudSolrClient solrClient = cluster.getSolrClient(collectionName);

    cluster.waitForActiveCollection(collectionName, 2, 4);

    // verify that indexing works
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);
    solrClient.commit();
    // verify the docs exist
    QueryResponse rsp = solrClient.query(params(CommonParams.Q, "*:*"));
    assertEquals("initial num docs", NUM_DOCS, rsp.getResults().getNumFound());

    // index more but don't commit
    docs.clear();
    for (int i = NUM_DOCS; i < NUM_DOCS * 2; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);

    Replica leader =
        ZkStateReader.from(solrClient).getLeaderRetry(collectionName, "shard1", DEFAULT_TIMEOUT);

    final AtomicReference<Long> coreStartTime =
        new AtomicReference<>(getCoreStatus(leader).startTime.getTime());

    // Check for value change
    CollectionAdminRequest.modifyCollection(
            collectionName, Collections.singletonMap(ZkStateReader.READ_ONLY, "true"))
        .process(solrClient);

    DocCollection coll = solrClient.getClusterState().getCollection(collectionName);
    assertNotNull(coll.toString(), coll.getProperties().get(ZkStateReader.READ_ONLY));
    assertEquals(
        coll.toString(), coll.getProperties().get(ZkStateReader.READ_ONLY).toString(), "true");

    // wait for the expected collection reload
    RetryUtil.retryUntil(
        "Timed out waiting for core to reload",
        30,
        1000,
        TimeUnit.MILLISECONDS,
        () -> {
          long restartTime = 0;
          try {
            restartTime = getCoreStatus(leader).startTime.getTime();
          } catch (Exception e) {
            log.warn("Exception getting core start time: ", e);
            return false;
          }
          return restartTime > coreStartTime.get();
        });

    coreStartTime.set(getCoreStatus(leader).startTime.getTime());

    // check for docs - reloading should have committed the new docs
    // this also verifies that searching works in read-only mode
    rsp = solrClient.query(params(CommonParams.Q, "*:*"));
    assertEquals(
        "num docs after turning on read-only", NUM_DOCS * 2, rsp.getResults().getNumFound());

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
    CollectionAdminRequest.modifyCollection(
            collectionName, Collections.singletonMap(ZkStateReader.READ_ONLY, ""))
        .process(cluster.getSolrClient());
    coll = solrClient.getClusterState().getCollection(collectionName);
    assertNull(coll.toString(), coll.getProperties().get(ZkStateReader.READ_ONLY));

    // wait for the expected collection reload
    RetryUtil.retryUntil(
        "Timed out waiting for core to reload",
        30,
        1000,
        TimeUnit.MILLISECONDS,
        () -> {
          long restartTime = 0;
          try {
            restartTime = getCoreStatus(leader).startTime.getTime();
          } catch (Exception e) {
            log.warn("Exception getting core start time: ", e);
            return false;
          }
          return restartTime > coreStartTime.get();
        });

    // check that updates are working now
    docs.clear();
    for (int i = NUM_DOCS * 2; i < NUM_DOCS * 3; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "string_s", String.valueOf(i)));
    }
    solrClient.add(docs);
    solrClient.commit();
    rsp = solrClient.query(params(CommonParams.Q, "*:*"));
    assertEquals(
        "num docs after turning off read-only", NUM_DOCS * 3, rsp.getResults().getNumFound());
  }

  @Test
  public void testRenameCollection() throws Exception {
    doTestRenameCollection(true);
    CollectionAdminRequest.deleteAlias("col1").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("col2").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("foo").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("simpleAlias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("catAlias").process(cluster.getSolrClient());
    CollectionAdminRequest.deleteAlias("compoundAlias").process(cluster.getSolrClient());
    cluster.getZkStateReader().aliasesManager.update();
    doTestRenameCollection(false);
  }

  private void doTestRenameCollection(boolean followAliases) throws Exception {
    String collectionName1 = "testRename1_" + followAliases;
    String collectionName2 = "testRename2_" + followAliases;
    CollectionAdminRequest.createCollection(collectionName1, "conf", 1, 1)
        .setAlias("col1")
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(collectionName2, "conf", 1, 1)
        .setAlias("col2")
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName1, 1, 1);
    cluster.waitForActiveCollection(collectionName2, 1, 1);

    waitForState(
        "Expected collection1 to be created with 1 shard and 1 replica",
        collectionName1,
        clusterShape(1, 1));
    waitForState(
        "Expected collection2 to be created with 1 shard and 1 replica",
        collectionName2,
        clusterShape(1, 1));

    CollectionAdminRequest.createAlias("compoundAlias", "col1,col2")
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias("simpleAlias", "col1").process(cluster.getSolrClient());
    CollectionAdminRequest.createCategoryRoutedAlias(
            "catAlias",
            "field1",
            100,
            CollectionAdminRequest.createCollection("_unused_", "conf", 1, 1))
        .process(cluster.getSolrClient());

    CollectionAdminRequest.Rename rename = CollectionAdminRequest.renameCollection("col1", "foo");
    rename.setFollowAliases(followAliases);
    ZkStateReader zkStateReader = cluster.getZkStateReader();
    Aliases aliases;
    if (!followAliases) {
      Exception e = assertThrows(Exception.class, () -> rename.process(cluster.getSolrClient()));
      assertTrue(e.toString(), e.toString().contains("source collection 'col1' not found"));
    } else {
      rename.process(cluster.getSolrClient());
      zkStateReader.aliasesManager.update();

      aliases = zkStateReader.getAliases();
      assertEquals(
          aliases.getCollectionAliasListMap().toString(),
          collectionName1,
          aliases.resolveSimpleAlias("foo"));
      assertEquals(
          aliases.getCollectionAliasListMap().toString(),
          collectionName1,
          aliases.resolveSimpleAlias("simpleAlias"));
      List<String> compoundAliases = aliases.resolveAliases("compoundAlias");
      assertEquals(compoundAliases.toString(), 2, compoundAliases.size());
      assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName1));
      assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName2));
    }

    CollectionAdminRequest.renameCollection(collectionName1, collectionName2)
        .process(cluster.getSolrClient());
    zkStateReader.aliasesManager.update();

    aliases = zkStateReader.getAliases();
    if (followAliases) {
      assertEquals(
          aliases.getCollectionAliasListMap().toString(),
          collectionName1,
          aliases.resolveSimpleAlias("foo"));
    }
    assertEquals(
        aliases.getCollectionAliasListMap().toString(),
        collectionName2,
        aliases.resolveSimpleAlias("simpleAlias"));
    assertEquals(
        aliases.getCollectionAliasListMap().toString(),
        collectionName1,
        aliases.resolveSimpleAlias(collectionName1));
    // we renamed col1 -> col2 so the compound alias contains only "col2,col2" which is reduced to
    // col2
    List<String> compoundAliases = aliases.resolveAliases("compoundAlias");
    assertEquals(compoundAliases.toString(), 1, compoundAliases.size());
    assertTrue(compoundAliases.toString(), compoundAliases.contains(collectionName2));

    CollectionAdminRequest.Rename catRename =
        CollectionAdminRequest.renameCollection("catAlias", "bar");
    catRename.setFollowAliases(followAliases);
    Exception e =
        assertThrows(
            "category-based alias renaming should fail",
            Exception.class,
            () -> catRename.process(cluster.getSolrClient()));
    if (followAliases) {
      assertTrue(e.toString(), e.toString().contains("is a routed alias"));
    } else {
      assertTrue(e.toString(), e.toString().contains("source collection 'catAlias' not found"));
    }

    CollectionAdminRequest.Rename rename2 = CollectionAdminRequest.renameCollection("col2", "foo");
    rename2.setFollowAliases(followAliases);
    e =
        assertThrows(
            "should fail because 'foo' already exists",
            Exception.class,
            () -> rename2.process(cluster.getSolrClient()));
    if (followAliases) {
      assertTrue(e.toString(), e.toString().contains("exists"));
    } else {
      assertTrue(e.toString(), e.toString().contains("source collection 'col2' not found"));
    }
  }

  @Test
  public void testDeleteAliasedCollection() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName1 = "aliasedCollection1";
    String collectionName2 = "aliasedCollection2";
    CollectionAdminRequest.createCollection(collectionName1, "conf", 1, 1).process(solrClient);
    CollectionAdminRequest.createCollection(collectionName2, "conf", 1, 1).process(solrClient);

    cluster.waitForActiveCollection(collectionName1, 1, 1);
    cluster.waitForActiveCollection(collectionName2, 1, 1);

    waitForState(
        "Expected collection1 to be created with 1 shard and 1 replica",
        collectionName1,
        clusterShape(1, 1));
    waitForState(
        "Expected collection2 to be created with 1 shard and 1 replica",
        collectionName2,
        clusterShape(1, 1));

    SolrInputDocument doc = new SolrInputDocument("id", "1");
    solrClient.add(collectionName1, doc);
    doc = new SolrInputDocument("id", "2");
    solrClient.add(collectionName2, doc);
    solrClient.commit(collectionName1);
    solrClient.commit(collectionName2);

    assertDoc(solrClient, collectionName1, "1");
    assertDoc(solrClient, collectionName2, "2");

    CollectionAdminRequest.createAlias(collectionName1, collectionName2).process(solrClient);

    RetryUtil.retryUntil(
        "didn't get the new aliases",
        10,
        1000,
        TimeUnit.MILLISECONDS,
        () -> {
          try {
            ZkStateReader.from(solrClient).aliasesManager.update();
            return ZkStateReader.from(solrClient)
                .getAliases()
                .resolveSimpleAlias(collectionName1)
                .equals(collectionName2);
          } catch (Exception e) {
            fail("exception caught refreshing aliases: " + e);
            return false;
          }
        });

    // both results should come from collection 2
    assertDoc(solrClient, collectionName1, "2"); // aliased
    assertDoc(solrClient, collectionName2, "2"); // direct

    // should be able to remove collection 1 when followAliases = false
    CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName1);
    delete.setFollowAliases(false);
    delete.process(solrClient);
    ClusterState state = solrClient.getClusterState();
    assertFalse(collectionNamesString(state), state.hasCollection(collectionName1));
    // search should still work, returning results from collection 2
    assertDoc(solrClient, collectionName1, "2"); // aliased
    assertDoc(solrClient, collectionName2, "2"); // direct

    // without aliases this collection doesn't exist anymore
    delete = CollectionAdminRequest.deleteCollection(collectionName1);
    delete.setFollowAliases(false);
    try {
      delete.process(solrClient);
      fail("delete of nonexistent collection 1 should have failed when followAliases=false");
    } catch (Exception e) {
      assertTrue(e.toString(), e.toString().contains(collectionName1));
    }

    // with followAliases=true collection 2 (and the alias) should both be removed
    delete.setFollowAliases(true);
    delete.process(solrClient);

    state = solrClient.getClusterState();
    // the collection is gone
    assertFalse(collectionNamesString(state), state.hasCollection(collectionName2));

    // and the alias is gone
    RetryUtil.retryUntil(
        "didn't get the new aliases",
        10,
        1000,
        TimeUnit.MILLISECONDS,
        () -> {
          try {
            ZkStateReader.from(solrClient).aliasesManager.update();
            return !ZkStateReader.from(solrClient).getAliases().hasAlias(collectionName1);
          } catch (Exception e) {
            fail("exception caught refreshing aliases: " + e);
            return false;
          }
        });
  }

  private static String collectionNamesString(ClusterState state) {
    return state.collectionStream().map(Object::toString).collect(Collectors.joining(","));
  }

  private void assertDoc(CloudSolrClient solrClient, String collection, String id)
      throws Exception {
    QueryResponse rsp = solrClient.query(collection, params(CommonParams.Q, "*:*"));
    assertEquals(rsp.toString(), 1, rsp.getResults().getNumFound());
    SolrDocument sdoc = rsp.getResults().get(0);
    assertEquals(sdoc.toString(), id, sdoc.getFieldValue("id"));
  }

  @Test
  public void testOverseerStatus() throws IOException, SolrServerException {
    CollectionAdminResponse response =
        new CollectionAdminRequest.OverseerStatus().process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    // When running with Distributed Collection API, no real data in Overseer status, but the
    // Collection API calls above shouldn't fail
    if (new CollectionAdminRequest.RequestApiDistributedProcessing()
        .process(cluster.getSolrClient())
        .getIsCollectionApiDistributed()) {
      return;
    }
    assertNotNull(
        "overseer_operations shouldn't be null", response.getResponse().get("overseer_operations"));
  }

  @Test
  public void testList() throws IOException, SolrServerException {
    CollectionAdminResponse response =
        new CollectionAdminRequest.List().process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertNotNull("collection list should not be null", response.getResponse().get("collections"));
  }

  @Test
  public void testAddAndDeleteReplicaProp() throws IOException, SolrServerException {

    final String collection = "replicaProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 4);

    final Replica replica = getCollectionState(collection).getLeader("shard1");
    CollectionAdminResponse response =
        CollectionAdminRequest.addReplicaProperty(
                collection, "shard1", replica.getName(), "preferredleader", "true")
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState(
        "Expecting property 'preferredleader' to appear on replica " + replica.getName(),
        collection,
        c -> "true".equals(c.getReplica(replica.getName()).getProperty("preferredleader")));

    response =
        CollectionAdminRequest.deleteReplicaProperty(
                collection, "shard1", replica.getName(), "property.preferredleader")
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState(
        "Expecting property 'preferredleader' to be removed from replica " + replica.getName(),
        collection,
        c -> c.getReplica(replica.getName()).getProperty("preferredleader") == null);
  }

  @Test
  public void testBalanceShardUnique() throws IOException, SolrServerException {

    final String collection = "balancedProperties";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 4);

    CollectionAdminResponse response =
        CollectionAdminRequest.balanceReplicaProperty(collection, "preferredLeader")
            .process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    waitForState(
        "Expecting 'preferredleader' property to be balanced across all shards",
        collection,
        c -> {
          for (Slice slice : c) {
            int count = 0;
            for (Replica replica : slice) {
              if ("true".equals(replica.getProperty("preferredleader"))) count += 1;
            }
            if (count != 1) return false;
          }
          return true;
        });
  }

  @Test
  public void testModifyCollectionAttribute() throws IOException, SolrServerException {
    final String collection = "testAddAndDeleteCollectionAttribute";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 1, 1);

    CollectionAdminRequest.modifyCollection(collection, null)
        .setAttribute("replicationFactor", 25)
        .process(cluster.getSolrClient());

    waitForState(
        "Expecting attribute 'replicationFactor' to be 25",
        collection,
        c -> 25 == c.getReplicationFactor());

    expectThrows(
        IllegalArgumentException.class,
        "An attempt to set unknown collection attribute should have failed",
        () ->
            CollectionAdminRequest.modifyCollection(collection, null)
                .setAttribute("non_existent_attr", 25)
                .process(cluster.getSolrClient()));

    expectThrows(
        IllegalArgumentException.class,
        "An attempt to set null value should have failed",
        () ->
            CollectionAdminRequest.modifyCollection(collection, null)
                .setAttribute("non_existent_attr", null)
                .process(cluster.getSolrClient()));

    expectThrows(
        IllegalArgumentException.class,
        "An attempt to unset unknown collection attribute should have failed",
        () ->
            CollectionAdminRequest.modifyCollection(collection, null)
                .unsetAttribute("non_existent_attr")
                .process(cluster.getSolrClient()));
  }

  @Test
  public void testCollectionCreationTime() throws SolrServerException, IOException {
    Instant beforeCreation = Instant.now();

    String collectionName = getSaferTestName();
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 1, 1);

    Instant afterCreation = Instant.now();

    CollectionAdminRequest.ColStatus req = CollectionAdminRequest.collectionStatus(collectionName);
    CollectionAdminResponse response = req.process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());

    NamedList<?> colStatus = (NamedList<?>) response.getResponse().get(collectionName);
    Long creationTimeMillis = (Long) colStatus._get("creationTimeMillis");
    assertNotNull("creationTimeMillis was not included in COLSTATUS response", creationTimeMillis);

    Instant creationTime = Instant.ofEpochMilli(creationTimeMillis);
    assertTrue(
        "COLSTATUS creationTimeMillis should be after the test started",
        creationTime.isAfter(beforeCreation));
    assertTrue(
        "COLSTATUS creationTimeMillis should not be after the collection creation was completed",
        creationTime.isBefore(afterCreation));
  }
}
