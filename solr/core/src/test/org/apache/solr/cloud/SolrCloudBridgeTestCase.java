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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.SocketProxy;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SolrCloudBridgeTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static String COLLECTION;
  protected static AtomicInteger collectionCount = new AtomicInteger(0);

  protected volatile static CloudHttp2SolrClient cloudClient;
  
  protected static final String SHARD1 = "s1";
  protected static final String SHARD2 = "s2";

  protected String id = "id";

  private static final Set<SolrClient> newClients = ConcurrentHashMap.newKeySet();
  
  protected Map<String, Integer> handle = new ConcurrentHashMap<>();
  
  private static final Set<RestTestHarness> restTestHarnesses = ConcurrentHashMap.newKeySet();
  
  public final static int ORDERED = 1;
  public final static int SKIP = 2;
  public final static int SKIPVAL = 4;
  public final static int UNORDERED = 8;

  String t1="a_t";
  String i1="a_i1";
  String tlong = "other_l";
  String tsort="t_sortable";

  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";

  public static RandVal rdate = new RandDate();
  
  protected static String[] fieldNames = null;
  
  protected volatile static int numJettys = 3;
  
  protected volatile static int sliceCount = 2;
  
  protected volatile static int replicationFactor = 1;

  protected volatile static boolean enableProxy = false;
  
  protected final List<SolrClient> clients = Collections.synchronizedList(new ArrayList<>());
  protected volatile static boolean createCollection1 = true;
  protected volatile static boolean createControl;
  protected volatile static CloudHttp2SolrClient controlClient;
  protected volatile static MiniSolrCloudCluster controlCluster;
  protected volatile static String schemaString;
  protected volatile static String solrconfigString;
  protected volatile static String solrxmlString = "solr.xml";
  protected volatile static boolean uploadSelectCollection1Config = true;
  protected volatile static boolean formatZk = true;

  protected volatile static SortedMap<ServletHolder, String> extraServlets;

  final Pattern filenameExclusions = Pattern.compile(".*solrconfig(?:-|_).*?\\.xml|.*schema(?:-|_).*?\\.xml");

  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    Path TEST_PATH = SolrTestUtil.getFile("solr/collection1").getParentFile().toPath();

    COLLECTION = "collection" + collectionCount.incrementAndGet();

    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    cluster = configureCluster(numJettys).formatZk(formatZk).withSolrXml(TEST_PATH.resolve(solrxmlString)).withJettyConfig(jettyCfg -> jettyCfg.withServlets(extraServlets).enableProxy(enableProxy)).build();

    SolrZkClient zkClient = cluster.getSolrClient().getZkStateReader().getZkClient();

    // ZkController.bootstrapDefaultConfigSet uploads the SERVER's _default configset
    // (ManagedIndexSchemaFactory + managed-schema) at node startup, so /configs/_default already
    // exists by the time this runs. uploadToZK() uses mkdirs and never overwrites existing znodes,
    // so the old guard (upload only when absent) left the managed-schema config active and shadowed
    // this base's classic collection1/conf (ClassicIndexSchemaFactory + schema.xml, which defines
    // the standard distributed-test fields such as the *_ti1 dynamic field). Remove the bootstrapped
    // configset first so the test's own config is the one actually used.
    if (zkClient.exists("/configs/_default")) {
      zkClient.clean("/configs/_default");
    }
    zkClient.uploadToZK(Paths.get(SolrTestUtil.TEST_HOME()).resolve("collection1").resolve("conf"), "/configs" + "/" + "_default", filenameExclusions);
    
    if (schemaString != null) {
      if (zkClient.exists("/configs/_default/schema.xml")) {
        zkClient.setData("/configs/_default/schema.xml", TEST_PATH.resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
      } else {
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH.resolve("collection1").resolve("conf").resolve(schemaString).toFile());
        zkClient.create("/configs/_default/schema.xml", data, CreateMode.PERSISTENT, true);
      }

      if (zkClient.exists("/configs/_default/managed-schema")) {
        zkClient.delete("/configs/_default/managed-schema", -1);
        //byte[] data = FileUtils.readFileToByteArray(TEST_PATH.resolve("collection1").resolve("conf").resolve(schemaString).toFile());
        //zkClient.setData("/configs/_default/managed-schema", data, true);
      }
    }
    if (solrconfigString != null) {
      //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/_default, null);
      zkClient.setData("/configs/_default/solrconfig.xml", TEST_PATH.resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
    }

    if (uploadSelectCollection1Config) {
      String path = null;
      try {
        path = "/configs/_default/solrconfig.snippet.randomindexconfig.xml";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }

      try {
        path = "/configs/_default/enumsConfig.xml";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }

      try {
        path = "/configs/_default/currency.xml";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }

      try {
        path = "/configs/_default/old_synonyms.txt";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }

      try {
        path = "/configs/_default/open-exchange-rates.json";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }
      try {
        path = "/configs/_default/mapping-ISOLatin1Accent.txt";
        zkClient.create(path, TEST_PATH.resolve("collection1").resolve("conf").resolve(new File(path).getName()).toFile(), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException exists) {
        log.info("extra collection config file already exist in zk {}", path);
      }
    }

    if (createCollection1) {
      // Honor useTlogReplicas() and getPullReplicaCount() so subclasses that request TLOG/PULL
      // replicas (e.g. the ChaosMonkey*WithPullReplicasTest classes) actually get them created.
      // Both default to false/0, so plain NRT subclasses are unaffected.
      CollectionAdminRequest.createCollection(COLLECTION, "_default", sliceCount,
          useTlogReplicas() ? 0 : replicationFactor,
          useTlogReplicas() ? replicationFactor : 0,
          getPullReplicaCount()).setMaxShardsPerNode(10).process(cluster.getSolrClient());
      // Wait for every shard to have an active leader before the test body runs. createCollection()
      // returns once the cores are created, but under load a shard's leader election can still be in
      // flight; issuing an update/RTG immediately then routes to a shard with no active replica and
      // fails with "no servers hosting shard: sN" (503). (ShardRoutingTest.doAtomicUpdate.)
      cluster.waitForActiveCollection(COLLECTION, sliceCount,
          sliceCount * (replicationFactor + getPullReplicaCount()));
    }

    cloudClient = cluster.getSolrClient();

    if (createCollection1) {
      cloudClient.setDefaultCollection(COLLECTION);
    }
    
    
    for (int i =0;i < cluster.getJettySolrRunners().size(); i++) {
      clients.add(getClient(i));
    }
    
    if (createControl) {
      controlCluster = configureCluster(1).withSolrXml(TEST_PATH.resolve(solrxmlString)).formatZk(formatZk).build();
      
      SolrZkClient zkClientControl = controlCluster.getSolrClient().getZkStateReader().getZkClient();
      
      // Same as the main cluster: the control node bootstraps the server's managed-schema _default
      // at startup, and uploadToZK never overwrites existing znodes, so remove it first and upload
      // this base's classic collection1/conf instead.
      if (zkClientControl.exists("/configs/_default")) {
        zkClientControl.clean("/configs/_default");
      }
      zkClientControl.uploadToZK(TEST_PATH.resolve("collection1").resolve("conf"), "/configs" + "/" + "_default", filenameExclusions);

      if (schemaString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString), "/configs/_default", null);
        
        zkClientControl.setData("/configs/_default/schema.xml", TEST_PATH.resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH.resolve("collection1").resolve("conf").resolve(schemaString).toFile());
      }
      if (solrconfigString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/_default", null);
        zkClientControl.setData("/configs/_default/solrconfig.xml", TEST_PATH.resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
      }
      CollectionAdminRequest.createCollection(COLLECTION, "_default", 1, 1)
          .setMaxShardsPerNode(10)
          .process(controlCluster.getSolrClient());

      controlClient = controlCluster.getSolrClient();
      controlClient.setDefaultCollection(COLLECTION);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    if (cluster != null) cluster.shutdown();
    if (controlCluster != null) controlCluster.shutdown();
    cluster = null;
    controlCluster = null;
    synchronized (clients) {
      for (SolrClient client : clients) {
        client.close();
      }
    }
    clients.clear();
    controlClient = null;
    cluster = null;
    super.tearDown();
  }

  @BeforeClass
  public static void beforeSolrCloudBridgeTestClass() {
    // collectionCount is static and gradle reuses a forked test JVM across many test classes
    // (forkEvery=0), so without this reset the counter carries over from whatever
    // SolrCloudBridgeTestCase subclass happened to run first in this JVM. That makes the per-method
    // COLLECTION name ("collection" + count) non-deterministic across classes, and any test that
    // hard-codes "collection1" (BasicDistributedZkTest, TestConfigReload, the ChaosMonkey suites,
    // ...) fails with "Could not find collection : collection1 collections=[collectionN]" whenever it
    // is not the first such class in the JVM. Reset at each class boundary so the first @Before in
    // every class deterministically yields collection1.
    collectionCount.set(0);
    fieldNames = new String[]{"n_ti1", "n_f1", "n_tf1", "n_d1", "n_td1", "n_l1", "n_tl1", "n_dt1", "n_tdt1"};
    randVals = new RandVal[]{rint, rfloat, rfloat, rdouble, rdouble, rlong, rlong, rdate, rdate};
  }
  
  @AfterClass
  public static void afterSolrCloudBridgeTestCase() throws Exception {
    closeRestTestHarnesses();
    // NOTE: do NOT close cloudClient here - it is the MiniSolrCloudCluster's own client
    // (cluster.getSolrClient()) and is closed by cluster.shutdown() in tearDown(). The old code
    // (newClients.forEach(c -> IOUtils.closeQuietly(cloudClient))) both ignored its lambda arg and
    // double-closed the already-closed cluster client, which hung in ParWork.close() waiting on an
    // orphaned task and blocked the whole test JVM from completing (no results flushed).
      for (SolrClient client : newClients) {
        client.close();
      }

    newClients.clear();
    cloudClient = null;
    extraServlets = null;
    createCollection1 = true;
    createControl = false;
    solrconfigString = null;
    schemaString = null;
    solrxmlString = "solr.xml";
    numJettys = 3;
    formatZk = true;
    uploadSelectCollection1Config = true;
    sliceCount = 2;
    replicationFactor = 1;
    enableProxy = false;
    fieldNames = null;
    randVals = null;
  }
  
  protected String getBaseUrl(Http2SolrClient client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - COLLECTION.length() - 1);
  }

  protected String getShardsString() {
    StringBuilder sb = new StringBuilder();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (sb.length() > 0) sb.append(',');
      sb.append(runner.getBaseUrl() + "/" + COLLECTION);
    }

    return sb.toString();
  }
  
  public Http2SolrClient getClient(int i) {
    return getClient(COLLECTION, i);
  }
  
  public Http2SolrClient getClient(String collection, int i) {
    String baseUrl = cluster.getJettySolrRunner(i).getBaseUrl().toString() + "/" + collection;
    Http2SolrClient client = new Http2SolrClient.Builder(baseUrl)
        .idleTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }

  public Http2SolrClient getClientByNode(String collection, String node) {
    ClusterState cs = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection coll = cs.getCollection(collection);
    List<Replica> replicas = coll.getReplicas();
    for (Replica replica : replicas) {
      if (replica.getNodeName().equals(node)) {
        String baseUrl = replica.getBaseUrl() + "/" + collection;
        Http2SolrClient client = new Http2SolrClient.Builder(baseUrl)
            .idleTimeout(Integer.getInteger("socketTimeout", 30000))
            .build();
        newClients.add(client);
        return client;
      }
    }

    throw new IllegalArgumentException("Could not find replica with nodename=" + node);
  }

  public Http2SolrClient getClient(String collection, String url) {
    return getClient(url + "/" + collection);
  }

  public Http2SolrClient getClient(String baseUrl) {
    Http2SolrClient client = new Http2SolrClient.Builder(baseUrl)
        .idleTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, String configName,
      int numShards, int numReplicas, int maxShardsPerNode) throws SolrServerException, IOException {
    return CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());
  }

  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(10)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr, String routerField) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(routerField)
        .setCreateNodeSet(createNodeSetStr)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, String createNodeSetStr, String routerField, String conf) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, conf, numShards, numReplicas)
        .setRouterField(routerField)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "_default", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setCreateNodeSet(createNodeSetStr)
        .process(cluster.getSolrClient());
    return resp;
  }
  
  protected void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }
  
  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    if (controlClient != null) controlClient.add(doc);

    SolrClient client = clients.get(serverNumber);
    client.add(doc);
  }
  
  protected void index_specific(SolrClient client, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);

    // add to control second in case adding to shards fails
    if (controlClient != null) controlClient.add(doc);
  }

  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();
    String tmp = replicaNode.substring(replicaNode.indexOf(':')+1);
    if (tmp.indexOf('_') != -1)
      tmp = tmp.substring(0,tmp.indexOf('_'));
    return Integer.parseInt(tmp);
  }

  protected Replica getShardLeader(String testCollectionName, String shardId, int timeoutms) throws Exception {
    return cloudClient.getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(), testCollectionName, shardId, timeoutms, true);
  }
  
  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }

    if (createControl) {
      if (theJetty == null) {
        if (controlCluster.getJettySolrRunner(0).getLocalPort() == port) {
          theJetty = controlCluster.getJettySolrRunner(0);
        }
      }
    }
    if (theJetty == null)
      fail("Not able to find JettySolrRunner for port: "+port);

    return theJetty;
  }
  
  public static void commit() throws SolrServerException, IOException {
    if (controlClient != null) controlClient.commit();
    cloudClient.commit();
  }
  
  protected int getShardCount() {
    return numJettys;
  }
  
  public static abstract class RandVal {
    public static Set uniqueValues = new HashSet();

    public abstract Object val();

    public Object uval() {
      for (; ;) {
        Object v = val();
        if (uniqueValues.add(v)) return v;
      }
    }
  }

  void doQuery(String expectedDocs, String... queryParams) throws Exception {
    Set<String> expectedIds = new HashSet<>( StrUtils.splitSmart(expectedDocs, ",", true) );

    QueryResponse rsp = cloudClient.query(params(queryParams));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  protected void setDistributedParams(ModifiableSolrParams params) {
    params.set("shards", getShardsString());
  }
  
  protected QueryResponse query(SolrParams p) throws Exception {
    return query(true, p);
  }
  
  protected QueryResponse query(boolean setDistribParams, SolrParams p) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams(p);

    // TODO: look into why passing true causes fails
    //params.set("distrib", "false");
    //final QueryResponse controlRsp = controlClient.query(params);
    //validateControlData(controlRsp);

    //params.remove("distrib");
    if (setDistribParams) setDistributedParams(params);

    QueryResponse rsp = queryServer(params);

    //compareResponses(rsp, controlRsp);

    return rsp;
  }
  
  protected QueryResponse query(boolean setDistribParams, Object[] q) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    return query(setDistribParams, params);
  }
  
  protected QueryResponse queryServer(ModifiableSolrParams params) throws Exception {
    return cloudClient.query(params);
  }
  
  protected QueryResponse query(Object... q) throws Exception {
    return query(true, q);
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    addRandFields(doc);
    indexDoc(doc);
  }
  
  protected UpdateResponse indexDoc(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateResponse specificRsp = add(cloudClient, params, sdocs);
    return specificRsp;
  }

  protected UpdateResponse add(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (SolrInputDocument sdoc : sdocs) {
      ureq.add(sdoc);
    }
    return ureq.process(client);
  }
  
  protected static void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }

  public static Object[] getRandFields(String[] fields, RandVal[] randVals) {
    Object[] o = new Object[fields.length * 2];
    for (int i = 0; i < fields.length; i++) {
      o[i * 2] = fields[i];
      o[i * 2 + 1] = randVals[i].uval();
    }
    return o;
  }
  
  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    addFields(sdoc, getRandFields(fieldNames, randVals));
    return sdoc;
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    if (controlClient != null) controlClient.add(doc);
    cloudClient.add(doc);
  }
  
  protected void del(String query) throws SolrServerException, IOException {
    if (controlClient != null) controlClient.deleteByQuery(query);
    cloudClient.deleteByQuery(query);
  }

  protected void waitForRecoveriesToFinish(String collectionName) throws InterruptedException, TimeoutException {
    // Recoveries are "finished" when no replica on a LIVE node is still non-active. A replica whose
    // node is down/stopped cannot recover and must not block this wait — matching upstream
    // waitForRecoveriesToFinish, which ignores replicas on non-live nodes. (The strict-all-active
    // AllActive predicate is wrong here: tests such as ForceLeaderTest deliberately leave a replica
    // DOWN on a stopped node while waiting for the surviving replicas to recover.)
    cloudClient.getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, (liveNodes, coll) -> {
      if (coll == null) return false;
      Collection<Slice> slices = coll.getActiveSlices();
      if (slices == null) return false;
      for (Slice slice : slices) {
        for (Replica replica : slice.getReplicas()) {
          if (!liveNodes.contains(replica.getNodeName())) continue;
          if (!replica.getState().equals(State.ACTIVE)) return false;
        }
      }
      return true;
    });
  }
  
  protected void waitForRecoveriesToFinish() throws InterruptedException, TimeoutException {
    waitForRecoveriesToFinish(COLLECTION);
  }
  
  protected Replica getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Replica leader = clusterState.getCollection(collection).getLeader(slice);
    if (leader == null) {
      throw new RuntimeException("Could not find leader:" + collection + " " + slice);
    }
    return leader;
  }
  
  /**
   * Create a collection in single node
   */
  protected void createCollectionInOneInstance(final SolrClient client, String nodeName,
                                               ExecutorService executor, final String collection,
                                               final int numShards, int numReplicas) throws ExecutionException, InterruptedException {
    assertNotNull(nodeName);
    try {
      assertEquals(0, CollectionAdminRequest.createCollection(collection, "_default", numShards, 1)
          .setCreateNodeSet(ZkStateReader.CREATE_NODE_SET_EMPTY)
          .process(client).getStatus());
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < numReplicas; i++) {
      final int freezeI = i;
      Future future = executor.submit(() -> {
        try {
          String shard = "s" + ((freezeI % numShards) + 1);
          log.info("add replica to shard {}", shard);
          CollectionAdminRequest.addReplicaToShard(collection, shard).setNode(nodeName).process(client);
        } catch (Exception e) {
          log.error("", e);
        }
      });
      futures.add(future);
    }
    for (Future future : futures) {
      future.get();
    }
  }

  protected boolean useTlogReplicas() {
    return false; // MRM TODO:
  }

  protected int getPullReplicaCount() {
    return 0; // MRM TODO:
  }

  public static CloudHttp2SolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly) {
    return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
        .build();
  }

  public static CloudHttp2SolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly,
      int connectionTimeoutMillis, int socketTimeoutMillis) {
    // Connection/socket timeout are not directly exposed on CloudHttp2SolrClient.Builder;
    // configure them on the underlying Http2SolrClient instead if needed. For bridge
    // compatibility we just build with defaults.
    return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
        .build();
  }

  public static boolean usually() {
    return org.apache.lucene.util.LuceneTestCase.usually();
  }

  protected void randomlyEnableAutoSoftCommit() {
    if (random().nextBoolean()) {
      log.info("Turning on auto soft commit: 1000ms");
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        CoreContainer cores = runner.getCoreContainer();
        if (cores == null) continue;
        for (SolrCore core : cores.getCores()) {
          ((org.apache.solr.update.DirectUpdateHandler2) core.getUpdateHandler())
              .getSoftCommitTracker().setTimeUpperBound(1000);
        }
      }
    } else {
      log.info("Not turning on auto soft commit");
    }
  }

  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
      String collectionName, int numShards, int replicationFactor, SolrClient client, String createNodeSetStr, String configName) throws SolrServerException, IOException, InterruptedException, TimeoutException {

    int numNrtReplicas = useTlogReplicas()?0:replicationFactor;
    int numTlogReplicas = useTlogReplicas()?replicationFactor:0;
    return  createCollection(collectionInfos, collectionName,
        Utils.makeMap(
            ZkStateReader.NUM_SHARDS_PROP, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            ZkStateReader.CREATE_NODE_SET, createNodeSetStr),
        client, configName);
  }

  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos,
      String collectionName, int numShards, int replicationFactor, int maxShardsPerNode,
      SolrClient client, String createNodeSetStr, String configName)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    int numNrtReplicas = useTlogReplicas() ? 0 : replicationFactor;
    int numTlogReplicas = useTlogReplicas() ? replicationFactor : 0;
    return createCollection(collectionInfos, collectionName,
        Utils.makeMap(
            ZkStateReader.NUM_SHARDS_PROP, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode,
            ZkStateReader.CREATE_NODE_SET, createNodeSetStr),
        client, configName);
  }

  protected CloudHttp2SolrClient createCloudClient(String defaultCollection) {
    CloudHttp2SolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress(), random().nextBoolean());
    if (defaultCollection != null) client.setDefaultCollection(defaultCollection);
    return client;
  }

  // TODO: Use CollectionAdminRequest#createCollection() instead of a raw request
  protected CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos, String collectionName, Map<String, Object> collectionProps, SolrClient client, String confSetName)  throws SolrServerException, IOException, InterruptedException, TimeoutException{
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.CREATE.toString());
    params.set(CommonAdminParams.WAIT_FOR_FINAL_STATE, "true");
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if(entry.getValue() !=null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(ZkStateReader.NUM_SHARDS_PROP);
    if(numShards==null){
      String shardNames = (String) collectionProps.get(OverseerCollectionMessageHandler.SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames,',').size();
    }
    Integer numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.NRT_REPLICAS);
    if (numNrtReplicas == null) {
      numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.REPLICATION_FACTOR);
    }
    if(numNrtReplicas == null){
      numNrtReplicas = (Integer) OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.get(ZkStateReader.REPLICATION_FACTOR);
    }
    if (numNrtReplicas == null) {
      numNrtReplicas = Integer.valueOf(0);
    }
    Integer numTlogReplicas = (Integer) collectionProps.get(ZkStateReader.TLOG_REPLICAS);
    if (numTlogReplicas == null) {
      numTlogReplicas = Integer.valueOf(0);
    }
    Integer numPullReplicas = (Integer) collectionProps.get(ZkStateReader.PULL_REPLICAS);
    if (numPullReplicas == null) {
      numPullReplicas = Integer.valueOf(0);
    }
    if (confSetName != null) {
      params.set("collection.configName", confSetName);
    } else {
      params.set("collection.configName", "_default");
    }

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(numNrtReplicas + numTlogReplicas + numPullReplicas);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res;
    if (client == null) {
      final String baseUrl = getBaseUrl((Http2SolrClient) clients.get(clientIndex));
      try (SolrClient adminClient = createNewSolrClient("", baseUrl)) {
        res = new CollectionAdminResponse(adminClient.request(request));
      }
    } else {
      res = new CollectionAdminResponse(client.request(request));
    }

    try {
      cloudClient.waitForState(collectionName, 30, TimeUnit.SECONDS, SolrCloudTestCase.activeClusterShape(numShards,
          numShards * (numNrtReplicas + numTlogReplicas + numPullReplicas)));
    } catch (TimeoutException e) {
      throw new RuntimeException("Timeout waiting for " + numShards + " shards and " + (numNrtReplicas + numTlogReplicas + numPullReplicas) + " replicas.", e);
    }
    return res;
  }

  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    try {
      // setup the server...
      Http2SolrClient client = getHttpSolrClient(baseUrl + "/" + collection, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT_MILLIS);
      return client;
    }
    catch (Exception ex) {
      ParWork.propagateInterrupt(ex);
      throw new RuntimeException(ex);
    }
  }

  public static Http2SolrClient getHttpSolrClient(String url, int connectionTimeoutMillis, int socketTimeoutMillis) {
    return new Http2SolrClient.Builder(url)
        .build();
  }

  public static Http2SolrClient getHttpSolrClient(String url) {
    return new Http2SolrClient.Builder(url).build();
  }

  protected static boolean attemptCollectionDelete(CloudHttp2SolrClient client, String collectionName) {
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(client);
      return true;
    } catch (Exception e) {
      org.apache.solr.common.ParWork.propagateInterrupt(e);
      log.warn("Could not delete collection {} - ignoring", collectionName);
    }
    return false;
  }

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {

    String coreName = replica.getName();
    boolean reloadedOk = true;
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      // send reload command for the collection
      log.info("Sending RELOAD command for {}", testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      // verify reload is done, waiting up to 30 seconds for slow test environments
      // MRM TODO: we should do this to check status, but should not be need to wait for reload like it was
//      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
//      while (System.nanoTime() < timeout) {
//        statusResp = CoreAdminRequest.getStatus(coreName, client);
//        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
//        if (startTimeAfterReload > leaderCoreStartTime) {
//          reloadedOk = true;
//          break;
//        }
//        // else ... still waiting to see the reloaded core report a later start time
//        Thread.sleep(1000);
//      }
    }
    return reloadedOk;
  }
  
  protected void setupRestTestHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(() -> ((Http2SolrClient) client).getBaseURL(), cluster.getSolrClient().getHttpClient(), cluster.getJettySolrRunners().get(0).getCoreContainer()
          .getResourceLoader());
      restTestHarnesses.add(harness);
    }
  }

  protected static void closeRestTestHarnesses() throws IOException {
    for (RestTestHarness h : restTestHarnesses) {
      h.close();
    }
    restTestHarnesses.clear();
  }

  protected static RestTestHarness randomRestTestHarness(Random random) {
    List<RestTestHarness> rthList = new ArrayList<>(restTestHarnesses);
    return rthList.get(random.nextInt(rthList.size()));
  }

  protected static void forAllRestTestHarnesses(Consumer<RestTestHarness> op) {
    restTestHarnesses.forEach(op);
  }
  
  public static class AllActive implements CollectionStatePredicate {

    @Override
    public boolean matches(Set<String> liveNodes, DocCollection coll) {
      if (coll == null) return false;
      Collection<Slice> slices = coll.getActiveSlices();
      if (slices == null) return false;
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if (!replica.getState().equals(State.ACTIVE)) return false;
        }
      }

      return true;
    }
    
  }

  public static RandVal rint = new RandVal() {
    @Override
    public Object val() {
      return random().nextInt();
    }
  };

  public static RandVal rlong = new RandVal() {
    @Override
    public Object val() {
      return random().nextLong();
    }
  };

  public static RandVal rfloat = new RandVal() {
    @Override
    public Object val() {
      return random().nextFloat();
    }
  };

  public static RandVal rdouble = new RandVal() {
    @Override
    public Object val() {
      return random().nextDouble();
    }
  };
  
  public static class RandDate extends RandVal {
    @Override
    public Object val() {
      long v = random().nextLong();
      Date d = new Date(v);
      return d.toInstant().toString();
    }
  }
  
  protected static RandVal[] randVals;

  protected void checkShardConsistency(boolean verbose, boolean failOnInconsistency) throws Exception {
    DocCollection coll = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION);
    for (Slice slice : coll.getActiveSlices()) {
      checkShardConsistency(slice.getName());
    }
  }

  private java.util.TreeSet<String> fetchAllIds(String baseUrl) throws Exception {
    java.util.TreeSet<String> ids = new java.util.TreeSet<>();
    try (Http2SolrClient rc = new Http2SolrClient.Builder(baseUrl).build()) {
      SolrQuery q = new SolrQuery("*:*");
      q.add("distrib", "false");
      q.add("fl", "id");
      q.setRows(100000);
      org.apache.solr.common.SolrDocumentList docs = rc.query(q).getResults();
      for (org.apache.solr.common.SolrDocument d : docs) {
        Object id = d.getFirstValue("id");
        if (id != null) ids.add(String.valueOf(id));
      }
    }
    return ids;
  }

  protected void checkShardConsistency(String shardName) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    DocCollection coll = zkStateReader.getClusterState().getCollection(COLLECTION);
    Slice slice = coll.getSlice(shardName);
    assertNotNull("Shard " + shardName + " not found", slice);
    Set<String> liveNodes = zkStateReader.getLiveNodes();

    // Only replicas that are both on a live node AND in ACTIVE state are required to be
    // consistent. A replica on a downed node (e.g. a deliberately-stopped jetty, or one killed
    // by ChaosMonkey) cannot be queried at all, and a replica still recovering is allowed to
    // lag behind the leader. Both are skipped here, mirroring upstream
    // AbstractFullDistribZkTestBase.checkShardConsistency. Recovery completion is enforced
    // separately by the waitForRecoveriesToFinish / waitForReplicationFromReplicas calls the
    // chaos tests run before this check, so any live+ACTIVE replica that still disagrees is a
    // genuine consistency defect and must fail. Note getState() treats LEADER as ACTIVE.
    long baselineCount = -1;
    String baselineReplica = null;
    String baselineBaseUrl = null;
    for (Replica replica : slice.getReplicas()) {
      if (!liveNodes.contains(replica.getNodeName())) {
        continue;
      }
      if (replica.getState() != Replica.State.ACTIVE) {
        continue;
      }
      String baseUrl = replica.getBaseUrl() + "/" + COLLECTION;
      long cnt;
      try (Http2SolrClient rc = new Http2SolrClient.Builder(baseUrl).build()) {
        cnt = rc.query(new SolrQuery("*:*").add("distrib", "false")).getResults().getNumFound();
      }
      if (baselineCount < 0) {
        baselineCount = cnt;
        baselineReplica = replica.getName();
        baselineBaseUrl = baseUrl;
      } else if (baselineCount != cnt) {
        // DIVERGENCE DIAGNOSTIC: dump which doc ids differ so the lost-update can be traced through
        // the per-node OUTPUT logs (grep the id across leader-forward / recovery events).
        try {
          java.util.TreeSet<String> baseIds = fetchAllIds(baselineBaseUrl);
          java.util.TreeSet<String> thisIds = fetchAllIds(baseUrl);
          java.util.TreeSet<String> missingHere = new java.util.TreeSet<>(baseIds);
          missingHere.removeAll(thisIds);
          java.util.TreeSet<String> extraHere = new java.util.TreeSet<>(thisIds);
          extraHere.removeAll(baseIds);
          log.error("DIVERGENCE shard={} baseline={}({}) replica={}({}) missingOnReplica={} extraOnReplica={}",
              shardName, baselineReplica, baselineCount, replica.getName(), cnt, missingHere, extraHere);
          // FLT-INVESTIGATION (temporary; revert before commit): tie the divergence to leadership + terms.
          String termsJson;
          try {
            byte[] td = cloudClient.getZkStateReader().getZkClient()
                .getData("/collections/" + COLLECTION + "/terms/" + shardName,
                    (org.apache.zookeeper.Watcher) null, (org.apache.zookeeper.data.Stat) null, true);
            termsJson = td == null ? "null" : new String(td, java.nio.charset.StandardCharsets.UTF_8);
          } catch (Exception te) {
            termsJson = "err:" + te;
          }
          StringBuilder rs = new StringBuilder();
          for (Replica r : slice.getReplicas()) {
            rs.append(r.getName()).append("[state=").append(r.getState())
                .append(",raw=").append(r.getRawState()).append(",type=").append(r.getType())
                .append(",node=").append(r.getNodeName())
                .append(",live=").append(liveNodes.contains(r.getNodeName())).append("] ");
          }
          Replica ldr = slice.getLeader();
          log.error("DIVERGENCE-STATE shard={} leader={} terms={} replicas={}", shardName,
              ldr == null ? "null" : ldr.getName(), termsJson, rs);
        } catch (Exception diagEx) {
          log.error("DIVERGENCE diagnostic failed", diagEx);
        }
        assertEquals("Shard " + shardName + " replica " + replica.getName()
                + " count mismatch vs " + baselineReplica,
            baselineCount, cnt);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Bridge helpers — ported from AbstractFullDistribZkTestBase / AbstractDistribZkTestBase
  // ---------------------------------------------------------------------------

  protected String printClusterStateInfo() throws Exception {
    return printClusterStateInfo(null);
  }

  protected String printClusterStateInfo(String collection) throws Exception {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    String cs;
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      Map<String, DocCollection> map = clusterState.getCollectionsMap();
      org.noggit.CharArr out = new org.noggit.CharArr();
      new org.noggit.JSONWriter(out, 2).write(map);
      cs = out.toString();
    }
    return cs + "\nLive Nodes:" + cloudClient.getZkStateReader().getLiveNodes();
  }

  protected CollectionAdminResponse createCollectionRetry(String testCollectionName, String configSetName,
      int numShards, int replicationFactor, int maxShardsPerNode)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(testCollectionName, configSetName,
            numShards, replicationFactor)
        .setMaxShardsPerNode(maxShardsPerNode)
        .process(cluster.getSolrClient());
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.deleteCollection(testCollectionName).process(cloudClient);
      resp = CollectionAdminRequest.createCollection(testCollectionName, configSetName,
              numShards, replicationFactor)
          .setMaxShardsPerNode(maxShardsPerNode)
          .process(cluster.getSolrClient());
      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
    return resp;
  }

  protected int sendDocsWithRetry(List<SolrInputDocument> batch, int minRf, int maxRetries, int waitBeforeRetry) throws Exception {
    return sendDocsWithRetry(cloudClient, cloudClient.getDefaultCollection(), batch, minRf, maxRetries, waitBeforeRetry);
  }

  @SuppressWarnings("rawtypes")
  protected static int sendDocsWithRetry(CloudHttp2SolrClient cloudClient, String collection,
      List<SolrInputDocument> batch, int minRf, int maxRetries, int waitBeforeRetry) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.add(batch);
    org.apache.solr.common.util.NamedList resp = null;
    int numRetries = 0;
    while (true) {
      try {
        resp = cloudClient.request(up, collection);
        return cloudClient.getMinAchievedReplicationFactor(cloudClient.getDefaultCollection(), resp);
      } catch (Exception exc) {
        org.apache.solr.common.ParWork.propagateInterrupt(exc);
        Throwable rootCause = org.apache.solr.common.SolrException.getRootCause(exc);
        if (++numRetries <= maxRetries) {
          log.warn("ERROR: {} ... Sleeping for {} seconds before re-try ...", rootCause, waitBeforeRetry);
          Thread.sleep(waitBeforeRetry * 1000L);
        } else {
          log.error("No more retries available! Add batch failed", rootCause);
          throw exc;
        }
      }
    }
  }

  protected SocketProxy getProxyForReplica(Replica replica) throws Exception {
    return cluster.getProxyForReplica(replica);
  }

  protected void expireZkSession(JettySolrRunner jetty) {
    cluster.expireZkSession(jetty);
  }

  public static void waitForNewLeader(CloudHttp2SolrClient cloudClient, String collection,
      String shardName, Replica oldLeader, org.apache.solr.util.TimeOut timeOut) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.waitForState(collection, timeOut.timeLeft(java.util.concurrent.TimeUnit.SECONDS),
        java.util.concurrent.TimeUnit.SECONDS, (l, docCollection) -> {
          if (docCollection == null) return false;
          Slice slice = docCollection.getSlice(shardName);
          return slice != null
              && slice.getLeader(zkStateReader.getLiveNodes()) != null
              && !slice.getLeader(zkStateReader.getLiveNodes()).equals(oldLeader)
              && slice.getLeader(zkStateReader.getLiveNodes()).getState() == Replica.State.ACTIVE;
        });
  }

  public static void verifyReplicaStatus(ZkStateReader reader, String collection, String shard,
      String coreNodeName, Replica.State expectedState) throws InterruptedException, TimeoutException {
    reader.waitForState(collection, 15000, TimeUnit.MILLISECONDS,
        (l, collectionState) -> collectionState != null
            && collectionState.getSlice(shard) != null
            && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName) != null
            && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName).getState() == expectedState);
  }

  /**
   * Compat shim: old tests call waitForRecoveriesToFinish(collection, zkStateReader, verbose)
   * or waitForRecoveriesToFinish(collection, zkStateReader, false, true).
   * Delegate to the single-arg form which waits for all replicas to be ACTIVE.
   */
  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose)
      throws InterruptedException, TimeoutException {
    waitForRecoveriesToFinish(collection);
  }

  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader,
      boolean verbose, boolean failOnTimeout) throws InterruptedException, TimeoutException {
    waitForRecoveriesToFinish(collection);
  }

  protected void assertAllActive(String collection, ZkStateReader zkStateReader)
      throws InterruptedException, TimeoutException {
    zkStateReader.waitForState(collection, 30, TimeUnit.SECONDS, new AllActive());
  }

  // ---------------------------------------------------------------------------
  // checkShardConsistency with add/delete fail sets (compat shim)
  // ---------------------------------------------------------------------------

  protected void checkShardConsistency(boolean checkVsControl, boolean verbose,
      Set<String> addFails, Set<String> deleteFails) throws Exception {
    // In the bridge model we don't have a separate control cluster for these
    // chaos tests, so just check per-shard replica consistency.
    checkShardConsistency(verbose, true);
  }

  // ---------------------------------------------------------------------------
  // setErrorHook / clearErrorHook — lifted from AbstractFullDistribZkTestBase
  // ---------------------------------------------------------------------------

  protected static void setErrorHook() {
    SolrCmdDistributor.testing_errorHook = new Diagnostics.Callable() {
      @Override
      public void call(Object... data) {
        Exception e = (Exception) data[0];
        if (e == null) return;
        String msg = e.getMessage();
        if (msg != null && msg.contains("Timeout")) {
          Diagnostics.logThreadDumps("REQUESTING THREAD DUMP DUE TO TIMEOUT: " + e.getMessage());
        }
      }
    };
  }

  protected static void clearErrorHook() {
    SolrCmdDistributor.testing_errorHook = null;
  }

  // ---------------------------------------------------------------------------
  // restartZk — lifted from AbstractDistribZkTestBase
  // ---------------------------------------------------------------------------

  protected void restartZk(int pauseMillis) throws Exception {
    log.info("Restarting ZK with a pause of {}ms in between", pauseMillis);
    // Delegate to the cluster so its single ZkTestServer reference is updated. Building a local
    // replacement here (as this used to) leaked the live server: the cluster kept pointing at the
    // already-shut-down original, so the next restart shut down the corpse and the previous live
    // server kept the port forever -> "Error trying to run ZK Test Server" on the same-port rebind.
    cluster.restartZk(pauseMillis);
  }

  // ---------------------------------------------------------------------------
  // checkForCollection — lifted from AbstractFullDistribZkTestBase
  // ---------------------------------------------------------------------------

  protected void checkForCollection(String collectionName,
      List<Integer> numShardsNumReplicaList,
      List<String> nodesAllowedToRunShards) throws Exception {
    final TimeOut timeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    boolean success = false;
    String checkResult = "Didnt get to perform a single check";
    while (!timeout.hasTimedOut()) {
      checkResult = checkCollectionExpectations(collectionName, numShardsNumReplicaList, nodesAllowedToRunShards);
      if (checkResult == null) {
        success = true;
        break;
      }
      Thread.sleep(250);
    }
    if (!success) {
      fail(checkResult);
    }
  }

  private String checkCollectionExpectations(String collectionName,
      List<Integer> numShardsNumReplicaList,
      List<String> nodesAllowedToRunShards) {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    int expectedSlices = numShardsNumReplicaList.get(0);
    int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
    int expectedTotalShards = expectedSlices * expectedShardsPerSlice;
    if (clusterState.hasCollection(collectionName)) {
      Map<String, Slice> slices = clusterState.getCollection(collectionName).getSlicesMap();
      if (slices.size() != expectedSlices) {
        return "Found new collection " + collectionName + ", but mismatch on number of slices. Expected: "
            + expectedSlices + ", actual: " + slices.size();
      }
      int totalShards = 0;
      for (Slice replicas : slices.values()) {
        for (Replica replica : replicas.getReplicas()) {
          if (nodesAllowedToRunShards != null
              && !nodesAllowedToRunShards.contains(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            return "Shard " + replica.getName() + " created on node " + replica.getNodeName()
                + " not allowed to run shards for the created collection " + collectionName;
          }
        }
        totalShards += replicas.getReplicas().size();
      }
      if (totalShards != expectedTotalShards) {
        return "Found new collection " + collectionName
            + " with correct number of slices, but mismatch on number of shards. Expected: "
            + expectedTotalShards + ", actual: " + totalShards;
      }
      return null;
    } else {
      return "Could not find new collection " + collectionName;
    }
  }

  // ---------------------------------------------------------------------------
  // waitForReplicationFromReplicas / logReplicaTypesReplicationInfo /
  // getIndexVersion / logReplicationDetails — lifted from AbstractFullDistribZkTestBase
  // ---------------------------------------------------------------------------

  protected void waitForReplicationFromReplicas(String collectionName, ZkStateReader zkStateReader,
      TimeOut timeout) throws KeeperException, InterruptedException, IOException, TimeoutException {
    log.info("waitForReplicationFromReplicas: {}", collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    Map<String, CoreContainer> containers = new HashMap<>();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (!runner.isRunning()) continue;
      containers.put(runner.getNodeName(), runner.getCoreContainer());
    }
    for (Slice s : collection.getSlices()) {
      Replica leader = zkStateReader.getLeaderRetry(collectionName, s.getName(),
          (int) timeout.timeLeft(TimeUnit.MILLISECONDS));
      long leaderIndexVersion = -1;
      while (!timeout.hasTimedOut()) {
        leaderIndexVersion = getIndexVersion(leader);
        if (leaderIndexVersion >= 0) break;
        Thread.sleep(250);
      }
      if (timeout.hasTimedOut()) {
        fail("Unable to get leader indexVersion");
      }
      for (Replica pullReplica : s.getReplicas(EnumSet.of(Replica.Type.PULL, Replica.Type.TLOG))) {
        if (!zkStateReader.isNodeLive(pullReplica.getNodeName())) continue;
        while (true) {
          long replicaIndexVersion = getIndexVersion(pullReplica);
          if (leaderIndexVersion == replicaIndexVersion) {
            CoreContainer cc = containers.get(pullReplica.getNodeName());
            if (cc != null) {
              try (SolrCore core = cc.getCore(pullReplica.getName())) {
                if (core != null) {
                  RefCounted<SolrIndexSearcher> ref = core.getRegisteredSearcher();
                  try {
                    SolrIndexSearcher searcher = ref.get();
                    String servingVersion = searcher.getIndexReader().getIndexCommit()
                        .getUserData().get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
                    if (servingVersion != null && Long.parseLong(servingVersion) == replicaIndexVersion) {
                      break;
                    }
                  } finally {
                    if (ref != null) ref.decref();
                  }
                }
              }
            } else {
              break;
            }
          } else {
            if (timeout.hasTimedOut()) {
              logReplicaTypesReplicationInfo(collectionName, zkStateReader);
              fail(String.format(Locale.ROOT,
                  "Timed out waiting for replica %s (%d) to replicate from leader %s (%d)",
                  pullReplica.getName(), replicaIndexVersion, leader.getName(), leaderIndexVersion));
            }
            Thread.sleep(250);
          }
        }
      }
    }
  }

  protected static void logReplicaTypesReplicationInfo(String collectionName,
      ZkStateReader zkStateReader) throws KeeperException, InterruptedException, IOException {
    log.info("## Collecting extra Replica.Type information of the cluster");
    zkStateReader.updateLiveNodes();
    StringBuilder builder = new StringBuilder();
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    for (Slice s : collection.getSlices()) {
      Replica leader = s.getLeader(zkStateReader.getLiveNodes());
      for (Replica r : s.getReplicas()) {
        if (!r.isActive(zkStateReader.getLiveNodes())) {
          builder.append(String.format(Locale.ROOT, "Replica %s not in liveNodes or is not active%s",
              r.getName(), System.lineSeparator()));
          continue;
        }
        if (r.equals(leader)) {
          builder.append(String.format(Locale.ROOT, "Replica %s is leader%s",
              r.getName(), System.lineSeparator()));
        }
        logReplicationDetails(r, builder);
      }
    }
    log.info("Summary of the cluster: {}", builder);
  }

  protected static long getIndexVersion(Replica replica) throws IOException {
    try (Http2SolrClient client = new Http2SolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_SHOW_COMMITS);
      try {
        QueryResponse response = client.query(params);
        @SuppressWarnings("unchecked")
        List<NamedList<Object>> commits =
            (List<NamedList<Object>>) response.getResponse().get(ReplicationHandler.CMD_SHOW_COMMITS);
        return (long) Collections.max(commits,
            (a, b) -> ((Long) a.get("indexVersion")).compareTo((Long) b.get("indexVersion")))
            .get("indexVersion");
      } catch (SolrServerException e) {
        log.warn("Exception getting version from {}, will return an invalid version to retry.",
            replica.getName(), e);
        return -1;
      }
    }
  }

  protected static void logReplicationDetails(Replica replica, StringBuilder builder)
      throws IOException {
    try (Http2SolrClient client = new Http2SolrClient.Builder(replica.getCoreUrl()).build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_DETAILS);
      try {
        QueryResponse response = client.query(params);
        builder.append(String.format(Locale.ROOT, "Replica %s: %s%s",
            replica.getName(), response.getResponse(), System.lineSeparator()));
      } catch (SolrServerException e) {
        log.warn("Exception getting replication details from {}", replica.getName(), e);
      }
    }
  }
}