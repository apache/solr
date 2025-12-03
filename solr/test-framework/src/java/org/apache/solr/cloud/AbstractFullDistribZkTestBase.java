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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.solr.common.cloud.ZkStateReader.HTTPS;
import static org.apache.solr.common.cloud.ZkStateReader.URL_SCHEME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.cli.ConfigSetUploadTool;
import org.apache.solr.cli.DefaultToolRuntime;
import org.apache.solr.cli.SolrCLI;
import org.apache.solr.cli.ToolRuntime;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.apache.HttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.cloud.api.collections.CollectionHandlingUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.core.MockDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SocketProxy;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: we should still test this works as a custom update chain as well as what we test now - the
 * default update chain
 */
public abstract class AbstractFullDistribZkTestBase extends BaseDistributedSearchTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String REMOVE_VERSION_FIELD = "remove.version.field";
  private static final String ENABLE_UPDATE_LOG = "solr.index.updatelog.enabled";
  private static final String ZK_HOST = "zkHost";
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";
  protected static final String DEFAULT_COLLECTION = "collection1";
  protected volatile ZkTestServer zkServer;
  private final AtomicInteger homeCount = new AtomicInteger();

  @BeforeClass
  public static void beforeThisClass() throws Exception {
    // Only For Manual Testing: this will force an fs based dir factory
    // useFactory(null);
  }

  @BeforeClass
  public static void beforeFullSolrCloudTest() {}

  @Before
  public void beforeTest() {
    cloudInit = false;
  }

  public static final String SHARD1 = "shard1";
  public static final String SHARD2 = "shard2";

  protected boolean printLayoutOnTearDown = false;

  String t1 = "a_t";
  String i1 = "a_i1";
  String tlong = "other_tl1";

  String oddField = "oddField_s";
  String missingField = "ignore_exception__missing_but_valid_field_t";
  protected int sliceCount;

  protected volatile CloudSolrClient controlClientCloud; // cloud version of the control client
  protected volatile CloudSolrClient cloudClient;
  protected final List<SolrClient> coreClients = Collections.synchronizedList(new ArrayList<>());

  protected final List<CloudJettyRunner> cloudJettys =
      Collections.synchronizedList(new ArrayList<>());
  protected final Map<String, List<CloudJettyRunner>> shardToJetty = new ConcurrentHashMap<>();
  private AtomicInteger jettyIntCntr = new AtomicInteger(0);
  protected volatile ChaosMonkey chaosMonkey;

  protected Map<String, CloudJettyRunner> shardToLeaderJetty = new ConcurrentHashMap<>();
  protected Map<String, CloudSolrClient> solrClientByCollection = new ConcurrentHashMap<>();
  private static volatile boolean cloudInit;
  protected volatile boolean useJettyDataDir = true;

  private final List<RestTestHarness> restTestHarnesses =
      Collections.synchronizedList(new ArrayList<>());

  public static class CloudJettyRunner {
    public JettySolrRunner jetty;
    public String nodeName;
    public String coreNodeName;

    /** Core or Collection URL */
    public String url;

    public CloudSolrServerClient client;
    public Replica info;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof CloudJettyRunner other)) return false;
      return Objects.equals(url, other.url);
    }

    @Override
    public String toString() {
      return "CloudJettyRunner [url=" + url + "]";
    }
  }

  public static class CloudSolrServerClient {
    SolrClient solrClient;
    String nodeName;
    int port;

    public CloudSolrServerClient() {}

    public CloudSolrServerClient(SolrClient client) {
      this.solrClient = client;
    }

    public SolrClient getSolrClient() {
      return solrClient;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((solrClient == null) ? 0 : solrClient.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof CloudSolrServerClient other)) return false;
      return Objects.equals(solrClient, other.solrClient);
    }
  }

  protected static void setErrorHook() {
    SolrCmdDistributor.testing_errorHook =
        new Diagnostics.Callable() {
          @Override
          public void call(Object... data) {
            Exception e = (Exception) data[0];
            if (e == null) return;
            String msg = e.getMessage();
            if (msg != null && msg.contains("Timeout")) {
              Diagnostics.logThreadDumps(
                  "REQUESTING THREAD DUMP DUE TO TIMEOUT: " + e.getMessage());
            }
          }
        };
  }

  protected static void clearErrorHook() {
    SolrCmdDistributor.testing_errorHook = null;
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();

    // Setup from AbstractFullDistribZkTestBase
    Path zkDir = testDir.resolve("zookeeper/server1/data");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    System.setProperty(ZK_HOST, zkServer.getZkAddress());
    System.setProperty(ENABLE_UPDATE_LOG, "true");
    System.setProperty(REMOVE_VERSION_FIELD, "true");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");
    System.setProperty(
        MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE, "true");

    String schema = getCloudSchemaFile();
    if (schema == null) schema = "schema.xml";
    zkServer.buildZooKeeper(getCloudSolrConfig(), schema);

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    // ignoreException(".*");

    cloudInit = false;

    if (sliceCount > 0) {
      System.setProperty("numShards", Integer.toString(sliceCount));
    } else {
      System.clearProperty("numShards");
    }

    if (isSSLMode()) {
      System.clearProperty(URL_SCHEME);
      try (ZkStateReader zkStateReader =
          new ZkStateReader(
              zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT)) {
        try {
          zkStateReader
              .getZkClient()
              .create(
                  ZkStateReader.CLUSTER_PROPS,
                  Utils.toJSON(Collections.singletonMap(URL_SCHEME, HTTPS)),
                  CreateMode.PERSISTENT,
                  true);
        } catch (KeeperException.NodeExistsException e) {
          ZkNodeProps props =
              ZkNodeProps.load(
                  zkStateReader
                      .getZkClient()
                      .getData(ZkStateReader.CLUSTER_PROPS, null, null, true));
          zkStateReader
              .getZkClient()
              .setData(
                  ZkStateReader.CLUSTER_PROPS, Utils.toJSON(props.plus(URL_SCHEME, HTTPS)), true);
        }
      }
    }
    if (useTlogReplicas()) {
      log.info("Will use {} replicas unless explicitly asked otherwise", Replica.Type.TLOG);
    } else {
      log.info("Will use {} replicas unless explicitly asked otherwise", Replica.Type.NRT);
    }
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  protected String getCloudSchemaFile() {
    return getSchemaFile();
  }

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.update.delay", "0");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    System.clearProperty("solrcloud.update.delay");
  }

  public AbstractFullDistribZkTestBase() {
    if (TEST_NIGHTLY) {
      sliceCount = 2;
      fixShardCount(4);
    } else {
      sliceCount = 1;
      fixShardCount(2);
    }

    // TODO: for now, turn off stress because it uses regular clients, and we
    // need the cloud client because we kill servers
    stress = 0;

    useExplicitNodeNames = random().nextBoolean();
  }

  protected String getDataDir(String dataDir) throws IOException {
    return dataDir;
  }

  protected void initCloud() throws Exception {
    assert (cloudInit == false);
    cloudInit = true;
    cloudClient = createCloudClient(DEFAULT_COLLECTION);
    cloudClient.connect();

    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);

    chaosMonkey =
        new ChaosMonkey(
            zkServer, zkStateReader, DEFAULT_COLLECTION, shardToJetty, shardToLeaderJetty);
  }

  protected boolean useTlogReplicas() {
    return false;
  }

  protected CloudSolrClient createCloudClient(String defaultCollection) {
    return getCloudSolrClient(
        zkServer.getZkAddress(), defaultCollection, random().nextBoolean(), 30000, 120000);
  }

  @Override
  protected void createServers(int numServers) throws Exception {
    Path controlJettyDir = createTempDir("control");
    setupJettySolrHome(controlJettyDir);
    controlJetty =
        createJetty(
            controlJettyDir, useJettyDataDir ? getDataDir(testDir + "/control/data") : null);
    controlJetty.start();
    try (CloudSolrClient client = createCloudClient("control_collection")) {
      assertEquals(
          0,
          CollectionAdminRequest.createCollection("control_collection", "conf1", 1, 1)
              .setCreateNodeSet(controlJetty.getNodeName())
              .process(client)
              .getStatus());
      waitForActiveReplicaCount(client, "control_collection", 1);
    }

    controlClient =
        new HttpSolrClient.Builder(controlJetty.getBaseUrl().toString())
            .withDefaultCollection("control_collection")
            .build();
    if (sliceCount <= 0) {
      // for now, just create the cloud client for the control if we don't
      // create the normal cloud client.
      // this can change if more tests need it.
      controlClientCloud = createCloudClient("control_collection");
      controlClientCloud.connect();
      // NOTE: we are skipping creation of the chaos monkey by returning here
      cloudClient = controlClientCloud; // temporary - some code needs/uses
      // cloudClient
      return;
    }

    initCloud();

    createJettys(numServers);
  }

  public static void waitForCollection(ZkStateReader reader, String collection, int slices)
      throws Exception {
    log.info("waitForCollection ({}): slices={}", collection, slices);
    // wait until shards have started registering...
    int cnt = 30;
    while (!reader.getClusterState().hasCollection(collection)) {
      if (cnt == 0) {
        throw new RuntimeException(
            "timeout waiting for collection in cluster state: collection=" + collection);
      }
      cnt--;
      Thread.sleep(500);
    }
    cnt = 30;

    while (reader.getClusterState().getCollection(collection).getSlices().size() < slices) {
      if (cnt == 0) {
        throw new RuntimeException(
            "timeout waiting for collection shards to come up: collection="
                + collection
                + ", slices.expected="
                + slices
                + " slices.actual= "
                + reader.getClusterState().getCollection(collection).getSlices().size()
                + " slices : "
                + reader.getClusterState().getCollection(collection).getSlices());
      }
      cnt--;
      Thread.sleep(500);
    }
  }

  protected List<JettySolrRunner> createJettys(int numJettys) throws Exception {
    List<JettySolrRunner> jettys = Collections.synchronizedList(new ArrayList<>());
    List<SolrClient> clients = Collections.synchronizedList(new ArrayList<>());
    List<CollectionAdminRequest<CollectionAdminResponse>> createReplicaRequests =
        Collections.synchronizedList(new ArrayList<>());
    List<CollectionAdminRequest<CollectionAdminResponse>> createPullReplicaRequests =
        Collections.synchronizedList(new ArrayList<>());
    StringBuilder sb = new StringBuilder();

    // HACK: Don't be fooled by the replication factor of '1'...
    //
    // This CREATE command asks for a repFactor of 1, but uses an empty nodeSet.
    // This allows this method to create a collection with numShards == sliceCount,
    // but no actual cores ... yet.  The actual replicas are added later (once the actual
    // jetty instances are started)
    assertEquals(
        0,
        CollectionAdminRequest.createCollection(
                DEFAULT_COLLECTION, "conf1", sliceCount, 1) // not real rep factor!
            .setCreateNodeSet("") // empty node set prevents creation of cores
            .process(cloudClient)
            .getStatus());

    // expect sliceCount active shards, but no active replicas
    ZkStateReader.from(cloudClient)
        .waitForState(
            DEFAULT_COLLECTION,
            30,
            TimeUnit.SECONDS,
            SolrCloudTestCase.activeClusterShape(sliceCount, 0));

    ExecutorService customThreadPool =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));

    int numOtherReplicas = numJettys - getPullReplicaCount() * sliceCount;

    if (log.isInfoEnabled()) {
      log.info(
          "Creating jetty instances pullReplicaCount={} numOtherReplicas={}",
          getPullReplicaCount(),
          numOtherReplicas);
    }

    int addedReplicas = 0;
    for (int i = 1; i <= numJettys; i++) {
      if (sb.length() > 0) sb.append(',');
      int cnt = this.jettyIntCntr.incrementAndGet();

      Path jettyDir = createTempDir("shard-" + i);

      Files.createDirectories(jettyDir);
      setupJettySolrHome(jettyDir);
      int currentI = i;
      if (numOtherReplicas > 0) {
        numOtherReplicas--;
        if (useTlogReplicas()) {
          if (log.isInfoEnabled()) {
            log.info(
                "create jetty {} in directory {} of type {} in shard {}",
                i,
                jettyDir,
                Replica.Type.TLOG,
                ((currentI % sliceCount) + 1)); // nowarn
          }
          customThreadPool.execute(
              () -> {
                try {
                  JettySolrRunner j =
                      createJetty(
                          jettyDir,
                          useJettyDataDir ? getDataDir(testDir + "/jetty" + cnt) : null,
                          null,
                          "solrconfig.xml",
                          null,
                          Replica.Type.TLOG);
                  j.start();
                  jettys.add(j);
                  waitForLiveNode(j);

                  createReplicaRequests.add(
                      CollectionAdminRequest.addReplicaToShard(
                              DEFAULT_COLLECTION, "shard" + ((currentI % sliceCount) + 1))
                          .setNode(j.getNodeName())
                          .setType(Replica.Type.TLOG));

                  coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
                  SolrClient client = createNewSolrClient(j.getLocalPort());
                  clients.add(client);

                } catch (Exception e) {
                  log.error("error creating jetty", e);
                  throw new RuntimeException(e);
                }
              });

          addedReplicas++;
        } else {
          if (log.isInfoEnabled()) {
            log.info(
                "create jetty {} in directory {} of type {} for shard{}",
                i,
                jettyDir,
                Replica.Type.NRT,
                ((currentI % sliceCount) + 1)); // nowarn
          }

          customThreadPool.execute(
              () -> {
                try {
                  JettySolrRunner j =
                      createJetty(
                          jettyDir,
                          useJettyDataDir ? getDataDir(testDir + "/jetty" + cnt) : null,
                          null,
                          "solrconfig.xml",
                          null,
                          null);
                  j.start();
                  jettys.add(j);
                  waitForLiveNode(j);
                  createReplicaRequests.add(
                      CollectionAdminRequest.addReplicaToShard(
                              DEFAULT_COLLECTION, "shard" + ((currentI % sliceCount) + 1))
                          .setNode(j.getNodeName())
                          .setType(Replica.Type.NRT));
                  coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
                  SolrClient client = createNewSolrClient(j.getLocalPort());
                  clients.add(client);
                } catch (Exception e) {
                  log.error("error creating jetty", e);
                  throw new RuntimeException(e);
                }
              });

          addedReplicas++;
        }
      } else {
        log.info(
            "create jetty {} in directory {} of type {} for shard{}",
            i,
            jettyDir,
            Replica.Type.PULL,
            ((currentI % sliceCount) + 1)); // nowarn
        customThreadPool.execute(
            () -> {
              try {
                JettySolrRunner j =
                    createJetty(
                        jettyDir,
                        useJettyDataDir ? getDataDir(testDir + "/jetty" + cnt) : null,
                        null,
                        "solrconfig.xml",
                        null,
                        Replica.Type.PULL);
                j.start();
                jettys.add(j);
                waitForLiveNode(j);
                createPullReplicaRequests.add(
                    CollectionAdminRequest.addReplicaToShard(
                            DEFAULT_COLLECTION, "shard" + ((currentI % sliceCount) + 1))
                        .setNode(j.getNodeName())
                        .setType(Replica.Type.PULL));
                coreClients.add(createNewSolrClient(coreName, j.getLocalPort()));
                SolrClient client = createNewSolrClient(j.getLocalPort());
                clients.add(client);
              } catch (Exception e) {
                log.error("error creating jetty", e);
                throw new RuntimeException(e);
              }
            });
        addedReplicas++;
      }
    }

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);

    customThreadPool =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            new SolrNamedThreadFactory("createReplicaRequests"));

    for (CollectionAdminRequest<CollectionAdminResponse> r : createReplicaRequests) {
      customThreadPool.execute(
          () -> {
            CollectionAdminResponse response;
            try {
              response = r.process(cloudClient);
            } catch (SolrServerException | IOException e) {
              throw new RuntimeException(e);
            }

            assertTrue(response.isSuccess());
            String coreName = response.getCollectionCoresStatus().keySet().iterator().next();
          });
    }

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);

    customThreadPool =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            new SolrNamedThreadFactory("createPullReplicaRequests"));
    for (CollectionAdminRequest<CollectionAdminResponse> r : createPullReplicaRequests) {
      customThreadPool.execute(
          () -> {
            CollectionAdminResponse response;
            try {
              response = r.process(cloudClient);
            } catch (SolrServerException | IOException e) {
              throw new RuntimeException(e);
            }

            assertTrue(response.isSuccess());
            String coreName = response.getCollectionCoresStatus().keySet().iterator().next();
          });
    }

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);

    waitForActiveReplicaCount(cloudClient, DEFAULT_COLLECTION, addedReplicas);

    this.jettys.addAll(jettys);
    this.clients.addAll(clients);

    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    // make sure we have a leader for each shard
    for (int i = 1; i <= sliceCount; i++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + i, 10000);
    }

    if (sliceCount > 0) {
      updateMappingsFromZk(this.jettys, this.clients);
    }

    // build the shard string
    for (int i = 1; i <= numJettys / 2; i++) {
      JettySolrRunner j = this.jettys.get(i);
      JettySolrRunner j2 = this.jettys.get(i + (numJettys / 2 - 1));
      if (sb.length() > 0) sb.append(',');
      sb.append(buildUrl(j.getLocalPort()));
      sb.append("|").append(buildUrl(j2.getLocalPort()));
    }
    shards = sb.toString();

    return jettys;
  }

  protected void waitForLiveNode(JettySolrRunner j) throws InterruptedException, TimeoutException {
    if (log.isInfoEnabled()) {
      log.info("waitForLiveNode: {}", j.getNodeName());
    }
    ZkStateReader.from(cloudClient)
        .waitForLiveNodes(
            30, TimeUnit.SECONDS, SolrCloudTestCase.containsLiveNode(j.getNodeName()));
  }

  protected void waitForActiveReplicaCount(
      CloudSolrClient client, String collection, int expectedNumReplicas)
      throws TimeoutException, NotInClusterStateException {
    log.info(
        "Waiting to see {} active replicas in collection: {}", expectedNumReplicas, collection);
    AtomicInteger nReplicas = new AtomicInteger();
    try {
      ZkStateReader.from(client)
          .waitForState(
              collection,
              30,
              TimeUnit.SECONDS,
              (liveNodes, collectionState) -> {
                if (collectionState == null) {
                  return false;
                }
                int activeReplicas = 0;
                for (Slice slice : collectionState) {
                  for (Replica replica : slice) {
                    if (replica.isActive(liveNodes)) {
                      activeReplicas++;
                    }
                  }
                }
                nReplicas.set(activeReplicas);
                return (activeReplicas == expectedNumReplicas);
              });
    } catch (TimeoutException | InterruptedException e) {
      try {
        printLayout();
      } catch (Exception e1) {
        throw new RuntimeException(e1);
      }
      throw new NotInClusterStateException(
          ErrorCode.SERVER_ERROR,
          "Number of replicas in the state does not match what we set:"
              + nReplicas
              + " vs "
              + expectedNumReplicas);
    }
  }

  protected int getPullReplicaCount() {
    return 0;
  }

  /**
   * Total number of replicas for all shards as indicated by the cluster state, regardless of
   * status.
   *
   * @deprecated This method is virtually useless as it does not consider the status of either the
   *     shard or replica, nor wether the node hosting each replica is alive.
   */
  @Deprecated
  protected int getTotalReplicas(DocCollection c, String collection) {
    if (c == null) return 0; // support for when collection hasn't been created yet
    int cnt = 0;
    for (Slice slices : c.getSlices()) {
      cnt += slices.getReplicas().size();
    }
    return cnt;
  }

  public JettySolrRunner createJetty(
      String dataDir, String ulogDir, String shardList, String solrConfigOverride)
      throws Exception {

    JettyConfig jettyconfig =
        JettyConfig.builder()
            .stopAtShutdown(false)
            .withServlets(getExtraServlets())
            .withFilters(getExtraRequestFilters())
            .build();

    Properties props = new Properties();
    props.setProperty("solr.data.dir", getDataDir(dataDir));
    props.setProperty("shards", shardList);
    props.setProperty("solr.ulog.dir", ulogDir);
    props.setProperty("solrconfig", solrConfigOverride);

    JettySolrRunner jetty = new JettySolrRunner(getSolrHome().toString(), props, jettyconfig);

    jetty.start();

    return jetty;
  }

  @Override
  public final JettySolrRunner createJetty(
      Path solrHome,
      String dataDir,
      String shardList,
      String solrConfigOverride,
      String schemaOverride)
      throws Exception {
    return createJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride, null);
  }

  public JettySolrRunner createJetty(
      Path solrHome,
      String dataDir,
      String shardList,
      String solrConfigOverride,
      String schemaOverride,
      Replica.Type replicaType)
      throws Exception {
    // randomly test a relative solr.home path
    if (random().nextBoolean()) {
      solrHome = getRelativeSolrHomePath(solrHome);
    }

    JettyConfig jettyconfig =
        JettyConfig.builder()
            .stopAtShutdown(false)
            .withServlets(getExtraServlets())
            .withFilters(getExtraRequestFilters())
            .build();

    Properties props = new Properties();
    if (solrConfigOverride != null) props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null) props.setProperty("schema", schemaOverride);
    if (shardList != null) props.setProperty("shards", shardList);
    if (dataDir != null) props.setProperty("solr.data.dir", getDataDir(dataDir));
    if (replicaType != null) {
      props.setProperty("replicaType", replicaType.toString());
    } else if (random().nextBoolean()) {
      props.setProperty("replicaType", Replica.Type.NRT.toString());
    }
    props.setProperty("coreRootDirectory", solrHome.resolve("cores").toString());

    JettySolrRunner jetty = new JettySolrRunner(solrHome.toString(), props, jettyconfig);

    return jetty;
  }

  /**
   * Creates a JettySolrRunner with a socket proxy sitting in front of the Jetty server, which gives
   * us the ability to simulate network partitions without having to fuss with IPTables.
   */
  public JettySolrRunner createProxiedJetty(
      Path solrHome,
      String dataDir,
      String shardList,
      String solrConfigOverride,
      String schemaOverride,
      Replica.Type replicaType)
      throws Exception {

    JettyConfig jettyconfig =
        JettyConfig.builder()
            .stopAtShutdown(false)
            .withServlets(getExtraServlets())
            .withFilters(getExtraRequestFilters())
            .build();

    Properties props = new Properties();
    if (solrConfigOverride != null) props.setProperty("solrconfig", solrConfigOverride);
    if (schemaOverride != null) props.setProperty("schema", schemaOverride);
    if (shardList != null) props.setProperty("shards", shardList);
    if (dataDir != null) props.setProperty("solr.data.dir", getDataDir(dataDir));
    if (replicaType != null) {
      props.setProperty("replicaType", replicaType.toString());
    } else if (random().nextBoolean()) {
      props.setProperty("replicaType", Replica.Type.NRT.toString());
    }
    props.setProperty("coreRootDirectory", solrHome.resolve("cores").toString());

    JettySolrRunner jetty = new JettySolrRunner(solrHome.toString(), props, jettyconfig, true);

    return jetty;
  }

  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();
    String tmp = replicaNode.substring(replicaNode.indexOf(':') + 1);
    if (tmp.indexOf('_') != -1) tmp = tmp.substring(0, tmp.indexOf('_'));
    return Integer.parseInt(tmp);
  }

  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : jettys) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }

    if (theJetty == null) {
      if (controlJetty.getLocalPort() == port) {
        theJetty = controlJetty;
      }
    }

    if (theJetty == null) fail("Not able to find JettySolrRunner for port: " + port);

    return theJetty;
  }

  protected SocketProxy getProxyForReplica(Replica replica) throws Exception {
    String replicaBaseUrl = replica.getBaseUrl();
    assertNotNull(replicaBaseUrl);

    List<JettySolrRunner> runners = new ArrayList<>(jettys);
    runners.add(controlJetty);

    for (JettySolrRunner j : runners) {
      if (replicaBaseUrl
          .replaceAll("/$", "")
          .equals(j.getProxyBaseUrl().toExternalForm().replaceAll("/$", ""))) {
        return j.getProxy();
      }
    }

    printLayout();

    fail("No proxy found for " + replicaBaseUrl + "!");
    return null;
  }

  private Path getRelativeSolrHomePath(Path solrHome) {
    final Path curDirPath = Path.of("").toAbsolutePath();

    if (!solrHome.getRoot().equals(curDirPath.getRoot())) {
      // root of current directory and solrHome are not the same, therefore cannot relativize
      return solrHome;
    }

    final Path root = solrHome.getRoot();

    // relativize current directory to root: /tmp/foo -> /tmp/foo/../..
    final Path relativizedCurDir = curDirPath.resolve(curDirPath.relativize(root));

    // exclude the root from solrHome: /tmp/foo/solrHome -> tmp/foo/solrHome
    final Path solrHomeRelativeToRoot = root.relativize(solrHome);

    // create the relative solrHome: /tmp/foo/../../tmp/foo/solrHome
    return relativizedCurDir.resolve(solrHomeRelativeToRoot).toAbsolutePath();
  }

  protected void updateMappingsFromZk(List<JettySolrRunner> jettys, List<SolrClient> clients)
      throws Exception {
    updateMappingsFromZk(jettys, clients, false);
  }

  protected void updateMappingsFromZk(
      List<JettySolrRunner> jettys, List<SolrClient> clients, boolean allowOverSharding)
      throws Exception {
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    zkStateReader.forceUpdateCollection(DEFAULT_COLLECTION);
    cloudJettys.clear();
    shardToJetty.clear();

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(DEFAULT_COLLECTION);

    List<CloudSolrServerClient> theClients = new ArrayList<>();
    for (SolrClient client : clients) {
      // find info for this client in zk
      nextClient:
      // we find out state by simply matching ports...
      for (Slice slice : coll.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          int port = new URI(((HttpSolrClient) client).getBaseURL()).getPort();

          if (replica.getBaseUrl().contains(":" + port)) {
            CloudSolrServerClient csc = new CloudSolrServerClient();
            csc.solrClient = client;
            csc.port = port;
            csc.nodeName = replica.getNodeName();

            theClients.add(csc);

            break nextClient;
          }
        }
      }
    }

    for (JettySolrRunner jetty : jettys) {
      int port = jetty.getLocalPort();
      if (port == -1) {
        throw new RuntimeException("Cannot find the port for jetty");
      }

      nextJetty:
      for (Slice slice : coll.getSlices()) {
        Set<Entry<String, Replica>> entries = slice.getReplicasMap().entrySet();
        for (Entry<String, Replica> entry : entries) {
          Replica replica = entry.getValue();
          if (replica.getBaseUrl().contains(":" + port)) {
            List<CloudJettyRunner> list = shardToJetty.get(slice.getName());
            if (list == null) {
              list = new ArrayList<>();
              shardToJetty.put(slice.getName(), list);
            }
            boolean isLeader = Objects.equals(slice.getLeader(), replica);
            CloudJettyRunner cjr = new CloudJettyRunner();
            cjr.jetty = jetty;
            cjr.info = replica;
            cjr.nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            cjr.coreNodeName = entry.getKey();
            // TODO: no trailing slash on end desired, so replica.getCoreUrl is not applicable here
            cjr.url = replica.getBaseUrl() + "/" + replica.getStr(ZkStateReader.CORE_NAME_PROP);
            cjr.client = findClientByPort(port, theClients);
            list.add(cjr);
            if (isLeader) {
              shardToLeaderJetty.put(slice.getName(), cjr);
            }
            cloudJettys.add(cjr);
            break nextJetty;
          }
        }
      }
    }

    // # of jetties may not match replicas in shard here, because we don't map
    // jetties that are not running - every shard should have at least one
    // running jetty though
    for (Slice slice : coll.getSlices()) {
      // check that things look right
      List<CloudJettyRunner> jetties = shardToJetty.get(slice.getName());
      if (!allowOverSharding) {
        assertNotNull(
            "Test setup problem: We found no jetties for shard: "
                + slice.getName()
                + " just:"
                + shardToJetty.keySet(),
            jetties);

        assertEquals("slice:" + slice.getName(), slice.getReplicas().size(), jetties.size());
      }
    }
  }

  private CloudSolrServerClient findClientByPort(int port, List<CloudSolrServerClient> theClients) {
    for (CloudSolrServerClient client : theClients) {
      if (client.port == port) {
        return client;
      }
    }
    throw new IllegalArgumentException("Client with the given port does not exist:" + port);
  }

  @Override
  protected void setDistributedParams(ModifiableSolrParams params) {

    if (r.nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < sliceCount; i++) {
        if (i > 0) sb.append(',');
        sb.append("shard").append(i + 1);
      }
      params.set("shards", sb.toString());
    }
  }

  protected int sendDocsWithRetry(
      String collectionName,
      List<SolrInputDocument> batch,
      int minRf,
      int maxRetries,
      int waitBeforeRetry)
      throws Exception {
    return sendDocsWithRetry(
        cloudClient, collectionName, batch, minRf, maxRetries, waitBeforeRetry);
  }

  @SuppressWarnings("rawtypes")
  protected static int sendDocsWithRetry(
      CloudSolrClient cloudClient,
      String collection,
      List<SolrInputDocument> batch,
      int minRf,
      int maxRetries,
      int waitBeforeRetry)
      throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.add(batch);
    NamedList resp = null;
    int numRetries = 0;
    while (true) {
      try {
        resp = cloudClient.request(up, collection);
        return cloudClient.getMinAchievedReplicationFactor(
            cloudClient.getDefaultCollection(), resp);
      } catch (Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        if (++numRetries <= maxRetries) {
          log.warn(
              "ERROR: {} ... Sleeping for {} seconds before re-try ...",
              rootCause,
              waitBeforeRetry);
          Thread.sleep(waitBeforeRetry * 1000L);
        } else {
          log.error("No more retries available! Add batch failed due to: {}", rootCause);
          throw exc;
        }
      }
    }
  }

  @Override
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {

    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setParam("CONTROL", "TRUE");
    req.process(controlClient);

    // if we wanted to randomly pick a client - but sometimes they may be
    // down...

    // boolean pick = random.nextBoolean();
    //
    // int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) %
    // sliceCount;
    //
    // if (pick && sliceCount > 1) {
    // which = which + ((shardCount / sliceCount) *
    // random.nextInt(sliceCount-1));
    // }
    //
    // HttpSolrServer client = (HttpSolrServer)
    // clients.get(which);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam(UpdateParams.UPDATE_CHAIN, DISTRIB_UPDATE_CHAIN);
    ureq.process(cloudClient);
  }

  @Override
  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    SolrClient client = clients.get(serverNumber);

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);
  }

  protected void index_specific(SolrClient client, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);

    // add to control second in case adding to shards fails
    controlClient.add(doc);
  }

  protected Replica getLeaderFromZk(String collection, String slice) {
    getCommonCloudSolrClient();
    ClusterState clusterState = cloudClient.getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection != null && docCollection.getLeader(slice) != null) {
      return docCollection.getLeader(slice);
    }
    throw new RuntimeException("Could not find leader:" + collection + " " + slice);
  }

  @Override
  protected void del(String q) throws Exception {
    controlClient.deleteByQuery(q);
    cloudClient.deleteByQuery(q);

    //     for (SolrServer client : clients) {
    //       UpdateRequest ureq = new UpdateRequest();
    //       // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    //       ureq.deleteByQuery(q).process(client);
    //     }
  } // serial commit...

  protected void waitForRecoveriesToFinish(boolean verbose) throws Exception {
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose);
  }

  protected void waitForRecoveriesToFinish(String collection, boolean verbose) throws Exception {
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    waitForRecoveriesToFinish(collection, zkStateReader, verbose);
  }

  protected void waitForRecoveriesToFinish(boolean verbose, long timeoutSeconds) throws Exception {
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, verbose, true, timeoutSeconds);
  }

  protected void waitForRecoveriesToFinish(
      String collection, ZkStateReader zkStateReader, boolean verbose) throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, true);
  }

  protected void waitForRecoveriesToFinish(
      String collection, ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, failOnTimeout, 330, SECONDS);
  }

  public static void waitForRecoveriesToFinish(
      String collection,
      ZkStateReader zkStateReader,
      boolean verbose,
      boolean failOnTimeout,
      long timeoutSeconds)
      throws Exception {
    waitForRecoveriesToFinish(
        collection, zkStateReader, verbose, failOnTimeout, timeoutSeconds, SECONDS);
  }

  public static void waitForRecoveriesToFinish(
      String collection,
      ZkStateReader zkStateReader,
      boolean verbose,
      boolean failOnTimeout,
      long timeout,
      TimeUnit unit)
      throws Exception {
    log.info(
        "Wait for recoveries to finish - collection:{} failOnTimeout:{} timeout:{}{}",
        collection,
        failOnTimeout,
        timeout,
        unit);
    try {
      zkStateReader.waitForState(
          collection,
          timeout,
          unit,
          (liveNodes, docCollection) -> {
            if (docCollection == null) return false;
            boolean sawLiveRecovering = false;

            Map<String, Slice> slices = docCollection.getSlicesMap();
            assertNotNull("Could not find collection:" + collection, slices);
            for (Map.Entry<String, Slice> entry : slices.entrySet()) {
              Slice slice = entry.getValue();
              if (slice.getState()
                  == Slice.State
                      .CONSTRUCTION) { // similar to replica recovering; pretend its the same
                // thing
                if (verbose) System.out.println("Found a slice in construction state; will wait.");
                sawLiveRecovering = true;
              }
              Map<String, Replica> shards = slice.getReplicasMap();
              for (Map.Entry<String, Replica> shard : shards.entrySet()) {
                if (verbose)
                  System.out.println(
                      "replica:"
                          + shard.getValue().getName()
                          + " rstate:"
                          + shard.getValue().getStr(ZkStateReader.STATE_PROP)
                          + " live:"
                          + liveNodes.contains(shard.getValue().getNodeName()));
                final Replica.State state = shard.getValue().getState();
                if ((state == Replica.State.RECOVERING
                        || state == Replica.State.DOWN
                        || state == Replica.State.RECOVERY_FAILED)
                    && liveNodes.contains(shard.getValue().getStr(ZkStateReader.NODE_NAME_PROP))) {
                  return false;
                }
              }
            }
            if (!sawLiveRecovering) {
              if (verbose) System.out.println("no one is recoverying");
              return true;
            } else {
              return false;
            }
          });
    } catch (TimeoutException | InterruptedException e) {
      Diagnostics.logThreadDumps("Gave up waiting for recovery to finish.  THREAD DUMP:");
      zkStateReader.getZkClient().printLayoutToStream(System.out);
      fail("There are still nodes recovering - waited for " + timeout + unit);
    }

    log.info("Recoveries finished - collection:{}", collection);
  }

  public static void waitForCollectionToDisappear(
      String collection, ZkStateReader zkStateReader, boolean failOnTimeout, int timeoutSeconds)
      throws Exception {
    log.info(
        "Wait for collection to disappear - collection: {} failOnTimeout:{} timeout (sec):{}",
        collection,
        failOnTimeout,
        timeoutSeconds);

    zkStateReader.waitForState(
        collection, timeoutSeconds, TimeUnit.SECONDS, (docCollection) -> docCollection == null);
    log.info("Collection has disappeared - collection:{}", collection);
  }

  static void waitForNewLeader(CloudSolrClient cloudClient, String shardName, Replica oldLeader)
      throws Exception {
    log.info("Will wait for a node to become leader for 15 secs");
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);

    long startNs = System.nanoTime();
    try {
      zkStateReader.waitForState(
          "collection1",
          15,
          TimeUnit.SECONDS,
          (docCollection) -> {
            if (docCollection == null) return false;

            Slice slice = docCollection.getSlice(shardName);
            if (slice != null
                && slice.getLeader() != null
                && !slice.getLeader().equals(oldLeader)
                && slice.getLeader().getState() == Replica.State.ACTIVE) {
              if (log.isInfoEnabled()) {
                log.info(
                    "Old leader {}, new leader {}. New leader got elected in {} ms",
                    oldLeader,
                    slice.getLeader(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
              }
              return true;
            }
            return false;
          });
    } catch (TimeoutException e) {
      // If we failed to get a new leader, print some diagnotics before the test fails
      Diagnostics.logThreadDumps("Could not find new leader in specified timeout");
      zkStateReader.getZkClient().printLayoutToStream(System.out);
      fail("Could not find new leader even after waiting for 15s");
    }
  }

  public static void verifyReplicaStatus(
      ZkStateReader reader,
      String collection,
      String shard,
      String coreNodeName,
      Replica.State expectedState)
      throws InterruptedException, TimeoutException {
    log.info("verifyReplicaStatus ({}) shard={} coreNodeName={}", collection, shard, coreNodeName);
    reader.waitForState(
        collection,
        15000,
        TimeUnit.MILLISECONDS,
        (collectionState) ->
            collectionState != null
                && collectionState.getSlice(shard) != null
                && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName) != null
                && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName).getState()
                    == expectedState);
  }

  protected static void assertAllActive(String collection, ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException {

    zkStateReader.forceUpdateCollection(collection);
    ClusterState clusterState = zkStateReader.getClusterState();
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlices() == null) {
      throw new IllegalArgumentException("Cannot find collection:" + collection);
    }

    Map<String, Slice> slices = docCollection.getSlicesMap();
    for (Map.Entry<String, Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      if (slice.getState() != Slice.State.ACTIVE) {
        fail(
            "Not all shards are ACTIVE - found a shard "
                + slice.getName()
                + " that is: "
                + slice.getState());
      }
      Map<String, Replica> shards = slice.getReplicasMap();
      for (Map.Entry<String, Replica> shard : shards.entrySet()) {
        Replica replica = shard.getValue();
        if (replica.getState() != Replica.State.ACTIVE) {
          fail(
              "Not all replicas are ACTIVE - found a replica "
                  + replica.getName()
                  + " that is: "
                  + replica.getState());
        }
      }
    }
  }

  protected void checkQueries() throws Exception {

    handle.put("_version_", SKIPVAL);

    query("q", "*:*", "sort", "n_tl1 desc");

    handle.put("response", UNORDERED); // get?ids=a,b,c requests are unordered
    String ids = "987654";
    for (int i = 0; i < 20; i++) {
      query("qt", "/get", "id", Integer.toString(i));
      query("qt", "/get", "ids", Integer.toString(i));
      ids = ids + ',' + Integer.toString(i);
      query("qt", "/get", "ids", ids);
    }
    handle.remove("response");

    // random value sort
    for (String f : fieldNames) {
      query("q", "*:*", "sort", f + " desc");
      query("q", "*:*", "sort", f + " asc");
    }

    // these queries should be exactly ordered and scores should exactly match
    query("q", "*:*", "sort", i1 + " desc");
    query("q", "*:*", "sort", i1 + " asc");
    query("q", "*:*", "sort", i1 + " desc", "fl", "*,score");
    query("q", "*:*", "sort", "n_tl1 asc", "fl", "score"); // test legacy
    // behavior -
    // "score"=="*,score"
    query("q", "*:*", "sort", "n_tl1 desc");
    handle.put("maxScore", SKIPVAL);
    query("q", "{!func}" + i1); // does not expect maxScore. So if it comes
    // ,ignore it.
    // JavaBinCodec.writeSolrDocumentList()
    // is agnostic of request params.
    handle.remove("maxScore");
    query("q", "{!func}" + i1, "fl", "*,score"); // even scores should match
    // exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q", "quick");
    query("q", "all", "fl", "id", "start", "0");
    query("q", "all", "fl", "foofoofoo", "start", "0"); // no fields in returned
    // docs
    query("q", "all", "fl", "id", "start", "100");

    handle.put("score", SKIPVAL);
    query("q", "quick", "fl", "*,score");
    query("q", "all", "fl", "*,score", "start", "1");
    query("q", "all", "fl", "*,score", "start", "100");

    query("q", "now their fox sat had put", "fl", "*,score", "hl", "true", "hl.fl", t1);

    query("q", "now their fox sat had put", "fl", "foofoofoo", "hl", "true", "hl.fl", t1);

    query("q", "matchesnothing", "fl", "*,score");

    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1);
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.field",
        t1,
        "facet.limit",
        -1,
        "facet.sort",
        "count");
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.field",
        t1,
        "facet.limit",
        -1,
        "facet.sort",
        "count",
        "facet.mincount",
        2);
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.field",
        t1,
        "facet.limit",
        -1,
        "facet.sort",
        "index");
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.field",
        t1,
        "facet.limit",
        -1,
        "facet.sort",
        "index",
        "facet.mincount",
        2);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1, "facet.limit", 1);
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.query",
        "quick",
        "facet.query",
        "all",
        "facet.query",
        "*:*");
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1, "facet.offset", 1);
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", t1, "facet.mincount", 2);

    // test faceting multiple things at once
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.query",
        "quick",
        "facet.query",
        "all",
        "facet.query",
        "*:*",
        "facet.field",
        t1);

    // test filter tagging, facet exclusion, and naming (multi-select facet
    // support)
    query(
        "q",
        "*:*",
        "rows",
        100,
        "facet",
        "true",
        "facet.query",
        "{!key=myquick}quick",
        "facet.query",
        "{!key=myall ex=a}all",
        "facet.query",
        "*:*",
        "facet.field",
        "{!key=mykey ex=a}" + t1,
        "facet.field",
        "{!key=other ex=b}" + t1,
        "facet.field",
        "{!key=again ex=a,b}" + t1,
        "facet.field",
        t1,
        "fq",
        "{!tag=a}id:[1 TO 7]",
        "fq",
        "{!tag=b}id:[3 TO 9]");
    query(
        "q",
        "*:*",
        "facet",
        "true",
        "facet.field",
        "{!ex=t1}SubjectTerms_mfacet",
        "fq",
        "{!tag=t1}SubjectTerms_mfacet:(test 1)",
        "facet.limit",
        "10",
        "facet.mincount",
        "1");

    // test field that is valid in schema but missing in all shards
    query(
        "q", "*:*", "rows", 100, "facet", "true", "facet.field", missingField, "facet.mincount", 2);
    // test field that is valid in schema and missing in some shards
    query("q", "*:*", "rows", 100, "facet", "true", "facet.field", oddField, "facet.mincount", 2);

    query("q", "*:*", "sort", i1 + " desc", "stats", "true", "stats.field", i1);

    // Try to get better coverage for refinement queries by turning off over
    // requesting.
    // This makes it much more likely that we may not get the top facet values
    // and hence
    // we turn of that checking.
    handle.put("facet_fields", SKIPVAL);
    query(
        "q",
        "*:*",
        "rows",
        0,
        "facet",
        "true",
        "facet.field",
        t1,
        "facet.limit",
        5,
        "facet.shard.limit",
        5);
    // check a complex key name
    query(
        "q",
        "*:*",
        "rows",
        0,
        "facet",
        "true",
        "facet.field",
        "{!key='a b/c \\' \\} foo'}" + t1,
        "facet.limit",
        5,
        "facet.shard.limit",
        5);
    handle.remove("facet_fields");

    query("q", "*:*", "sort", "n_tl1 desc");

    // index the same document to two shards and make sure things
    // don't blow up.
    // assumes first n clients are first n shards
    if (clients.size() >= 2) {
      index(id, 100, i1, 107, t1, "oh no, a duplicate!");
      for (int i = 0; i < getShardCount(); i++) {
        index_specific(i, id, 100, i1, 107, t1, "oh no, a duplicate!");
      }
      commit();
      query("q", "duplicate", "hl", "true", "hl.fl", t1);
      query("q", "fox duplicate horses", "hl", "true", "hl.fl", t1);
      query("q", "*:*", "rows", 100);
    }
  }

  protected void indexAbunchOfDocs() throws Exception {
    indexr(id, 2, i1, 50, t1, "to come to the aid of their country.");
    indexr(id, 3, i1, 2, t1, "how now brown cow");
    indexr(id, 4, i1, -100, t1, "the quick fox jumped over the lazy dog");
    indexr(id, 5, i1, 500, t1, "the quick fox jumped way over the lazy dog");
    indexr(id, 6, i1, -600, t1, "humpty dumpy sat on a wall");
    indexr(id, 7, i1, 123, t1, "humpty dumpy had a great fall");
    indexr(id, 8, i1, 876, t1, "all the kings horses and all the kings men");
    indexr(id, 9, i1, 7, t1, "couldn't put humpty together again");
    indexr(id, 10, i1, 4321, t1, "this too shall pass");
    indexr(id, 11, i1, -987, t1, "An eye for eye only ends up making the whole world blind.");
    indexr(id, 12, i1, 379, t1, "Great works are performed, not by strength, but by perseverance.");
    indexr(id, 13, i1, 232, t1, "no eggs on wall, lesson learned", oddField, "odd man out");

    indexr(
        id,
        14,
        "SubjectTerms_mfacet",
        new String[] {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[] {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[] {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i = 0; i < 100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    for (int i = 100; i < 150; i++) {
      indexr(id, i);
    }
  }

  /**
   * Executes a query against each live and active replica of the specified shard and aserts that
   * the results are identical.
   *
   * @see #queryAndCompare
   */
  public QueryResponse queryAndCompareReplicas(SolrParams params, String shard) throws Exception {

    ArrayList<SolrClient> shardClients = new ArrayList<>(7);

    updateMappingsFromZk(jettys, clients);
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    assertNotNull("no jetties found for shard: " + shard, solrJetties);

    for (CloudJettyRunner cjetty : solrJetties) {
      Replica replica = cjetty.info;
      String nodeName = replica.getNodeName();
      boolean active = replica.getState() == Replica.State.ACTIVE;
      boolean live = zkStateReader.getClusterState().liveNodesContain(nodeName);
      if (active && live) {
        shardClients.add(cjetty.client.solrClient);
      }
    }
    return queryAndCompare(params, shardClients);
  }

  /**
   * For each Shard, executes a query against each live and active replica of that shard and asserts
   * that the results are identical for each replica of the same shard. Because results are not
   * compared between replicas of different shards, this method should be safe for comparing the
   * results of any query, even if it contains "distrib=false", because the replicas should all be
   * identical.
   *
   * @see AbstractFullDistribZkTestBase#queryAndCompareReplicas(SolrParams, String)
   */
  public void queryAndCompareShards(SolrParams params) throws Exception {

    updateMappingsFromZk(jettys, clients);
    List<String> shards = new ArrayList<>(shardToJetty.keySet());
    for (String shard : shards) {
      queryAndCompareReplicas(params, shard);
    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a consistent number of
   * documents.
   */
  protected void checkShardConsistency(String shard) throws Exception {
    checkShardConsistency(shard, false, false);
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a consistent number of
   * documents. If expectFailure==false, the exact differences found will be logged since this would
   * be an unexpected failure. verbose causes extra debugging into to be displayed, even if
   * everything is consistent.
   */
  protected String checkShardConsistency(String shard, boolean expectFailure, boolean verbose)
      throws Exception {

    List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);
    if (solrJetties == null) {
      throw new RuntimeException("shard not found:" + shard + " keys:" + shardToJetty.keySet());
    }
    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    log.debug("check const of {}", shard);
    int cnt = 0;
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    assertEquals(
        "The client count does not match up with the shard count for slice:" + shard,
        zkStateReader
            .getClusterState()
            .getCollection(DEFAULT_COLLECTION)
            .getSlice(shard)
            .getReplicasMap()
            .size(),
        solrJetties.size());

    CloudJettyRunner lastJetty = null;
    for (CloudJettyRunner cjetty : solrJetties) {
      Replica replica = cjetty.info;
      log.debug("client{}", cnt);
      log.debug("REPLICA:{}", replica);
      cnt++;

      try {
        SolrParams query =
            params(
                "q",
                "*:*",
                "rows",
                "0",
                "distrib",
                "false",
                "tests",
                "checkShardConsistency"); // "tests" is just a tag that won't do anything except be
        // echoed in logs
        num = cjetty.client.solrClient.query(query).getResults().getNumFound();
      } catch (SolrException | SolrServerException e) {
        log.debug("error contacting client: {}", e);
        continue;
      }

      boolean live = false;
      String nodeName = replica.getNodeName();
      if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
        live = true;
      }
      log.debug(" live:{}", live);
      log.debug(" num:{}", num);

      boolean active = replica.getState() == Replica.State.ACTIVE;
      if (active && live) {
        if (lastNum > -1 && lastNum != num && failMessage == null) {
          failMessage =
              shard
                  + " is not consistent.  Got "
                  + lastNum
                  + " from "
                  + lastJetty.url
                  + " (previous client)"
                  + " and got "
                  + num
                  + " from "
                  + cjetty.url;

          if (!expectFailure || verbose) {
            System.err.println("######" + failMessage);
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            query.set("fl", "id,_version_");
            query.set("rows", "100000");
            query.set("sort", "id asc");
            query.set("tests", "checkShardConsistency/showDiff");

            SolrDocumentList lst1 = lastJetty.client.solrClient.query(query).getResults();
            SolrDocumentList lst2 = cjetty.client.solrClient.query(query).getResults();

            CloudInspectUtil.showDiff(lst1, lst2, lastJetty.url, cjetty.url);
          }
        }
        lastNum = num;
        lastJetty = cjetty;
      }
    }
    return failMessage;
  }

  public void showCounts() {
    Set<String> theShards = shardToJetty.keySet();

    for (String shard : theShards) {
      List<CloudJettyRunner> solrJetties = shardToJetty.get(shard);

      for (CloudJettyRunner cjetty : solrJetties) {
        Replica info = cjetty.info;
        log.debug("REPLICA:{}", info);

        try {
          SolrParams query =
              params(
                  "q",
                  "*:*",
                  "rows",
                  "0",
                  "distrib",
                  "false",
                  "tests",
                  "checkShardConsistency"); // "tests" is just a
          // tag that won't do
          // anything except be
          // echoed in logs
          long num = cjetty.client.solrClient.query(query).getResults().getNumFound();
          log.debug("DOCS:{}", num);
        } catch (SolrServerException | SolrException | IOException e) {
          System.err.println("error contacting client: " + e.getMessage() + "\n");
          continue;
        }
        boolean live = false;
        String nodeName = info.getNodeName();
        ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
        if (zkStateReader.getClusterState().liveNodesContain(nodeName)) {
          live = true;
        }
        System.err.println(" live:" + live);
      }
    }
  }

  protected void randomlyEnableAutoSoftCommit() {
    if (r.nextBoolean()) {
      enableAutoSoftCommit(1000);
    } else {
      log.info("Not turning on auto soft commit");
    }
  }

  protected void enableAutoSoftCommit(int time) {
    log.info("Turning on auto soft commit: {}", time);
    for (List<CloudJettyRunner> jettyList : shardToJetty.values()) {
      for (CloudJettyRunner jetty : jettyList) {
        CoreContainer cores = jetty.jetty.getCoreContainer();
        for (SolrCore core : cores.getCores()) {
          ((DirectUpdateHandler2) core.getUpdateHandler())
              .getSoftCommitTracker()
              .setTimeUpperBound(time);
        }
      }
    }
  }

  /* Checks both shard replcia consistency and against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency() throws Exception {
    checkShardConsistency(true, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(boolean checkVsControl, boolean verbose) throws Exception {
    checkShardConsistency(checkVsControl, verbose, null, null);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(
      boolean checkVsControl, boolean verbose, Set<String> addFails, Set<String> deleteFails)
      throws Exception {

    updateMappingsFromZk(jettys, coreClients, true);

    Set<String> theShards = shardToJetty.keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      fail(failMessage);
    }

    if (!checkVsControl) return;

    SolrParams q =
        params(
            "q",
            "*:*",
            "rows",
            "0",
            "tests",
            "checkShardConsistency(vsControl)"); // add a tag to aid in debugging via logs

    SolrDocumentList controlDocList = controlClient.query(q).getResults();
    long controlDocs = controlDocList.getNumFound();

    SolrDocumentList cloudDocList = cloudClient.query(q).getResults();
    long cloudClientDocs = cloudDocList.getNumFound();

    // now check that the right # are on each shard
    theShards = shardToJetty.keySet();
    long cnt = 0;
    for (String s : theShards) {
      int times = shardToJetty.get(s).size();
      for (int i = 0; i < times; i++) {
        try {
          CloudJettyRunner cjetty = shardToJetty.get(s).get(i);
          Replica replica = cjetty.info;
          SolrClient client = cjetty.client.solrClient;
          boolean active = replica.getState() == Replica.State.ACTIVE;
          if (active) {
            SolrQuery query = new SolrQuery("*:*");
            query.set("distrib", false);
            long results = client.query(query).getResults().getNumFound();
            if (verbose) System.err.println(replica.getCoreUrl() + " : " + results);
            if (verbose) System.err.println("shard:" + replica.getShard());
            cnt += results;
            break;
          }
        } catch (Exception e) {
          // if we have a problem, try the next one
          if (i == times - 1) {
            throw e;
          }
        }
      }
    }

    if (controlDocs != cnt || cloudClientDocs != controlDocs) {
      String msg =
          "document count mismatch.  control="
              + controlDocs
              + " sum(shards)="
              + cnt
              + " cloudClient="
              + cloudClientDocs;
      log.error(msg);

      boolean shouldFail =
          CloudInspectUtil.compareResults(controlClient, cloudClient, addFails, deleteFails);
      if (shouldFail) {
        fail(msg);
      }
    }
  }

  protected SolrClient getClient(String nodeName) {
    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      if (client.nodeName.equals(nodeName)) {
        return client.solrClient;
      }
    }
    return null;
  }

  protected void assertDocCounts(boolean verbose) throws Exception {
    // TODO: as we create the clients, we should build a map from shard to
    // node/client
    // and node/client to shard?
    if (verbose)
      System.err.println(
          "control docs:"
              + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
              + "\n\n");
    long controlCount = controlClient.query(new SolrQuery("*:*")).getResults().getNumFound();

    // do some really inefficient mapping...
    Map<String, Slice> slices = null;
    ClusterState clusterState;
    try (ZkStateReader zk =
        new ZkStateReader(
            zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT)) {
      zk.createClusterStateWatchersAndUpdate();
      clusterState = zk.getClusterState();
      final DocCollection docCollection = clusterState.getCollectionOrNull(DEFAULT_COLLECTION);
      slices = (docCollection != null) ? docCollection.getSlicesMap() : null;
    }

    if (slices == null) {
      throw new RuntimeException(
          "Could not find collection "
              + DEFAULT_COLLECTION
              + " in "
              + clusterState.getCollectionNames());
    }

    for (CloudJettyRunner cjetty : cloudJettys) {
      CloudSolrServerClient client = cjetty.client;
      for (Map.Entry<String, Slice> slice : slices.entrySet()) {
        Map<String, Replica> theShards = slice.getValue().getReplicasMap();
        for (Map.Entry<String, Replica> shard : theShards.entrySet()) {
          String shardName =
              new URI(((HttpSolrClient) client.solrClient).getBaseURL()).getPort() + "_solr_";
          if (verbose && shard.getKey().endsWith(shardName)) {
            System.err.println("shard:" + slice.getKey());
            System.err.println(shard.getValue());
          }
        }
      }
      ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
      long count = 0;
      final Replica.State currentState = cjetty.info.getState();
      if (currentState == Replica.State.ACTIVE
          && zkStateReader.getClusterState().liveNodesContain(cjetty.info.getNodeName())) {
        SolrQuery query = new SolrQuery("*:*");
        query.set("distrib", false);
        count = client.solrClient.query(query).getResults().getNumFound();
      }

      if (verbose) System.err.println("client docs:" + count + "\n\n");
    }
    if (verbose)
      System.err.println(
          "control docs:"
              + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound()
              + "\n\n");
    SolrQuery query = new SolrQuery("*:*");
    assertEquals(
        "Doc Counts do not add up",
        controlCount,
        cloudClient.query(query).getResults().getNumFound());
  }

  @Override
  protected QueryResponse queryRandomShard(ModifiableSolrParams params)
      throws SolrServerException, IOException {

    if (r.nextBoolean()) params.set("collection", DEFAULT_COLLECTION);

    return cloudClient.query(params);
  }

  abstract static class StoppableThread extends Thread {
    public StoppableThread(String name) {
      super(name);
    }

    public abstract void safeStop();
  }

  public void waitForThingsToLevelOut() throws Exception {
    // Arbitrary, but if we're waiting for longer than 10 minutes, then fail the test anyway
    waitForThingsToLevelOut(10, TimeUnit.MINUTES);
  }

  public void waitForThingsToLevelOut(int timeout, TimeUnit unit) throws Exception {
    log.info("Wait for recoveries to finish - wait {}{} for each attempt", timeout, unit);
    int cnt = 0;
    boolean retry;
    do {
      waitForRecoveriesToFinish(VERBOSE, unit.toSeconds(timeout));

      try {
        commit();
      } catch (Exception e) {
        // we don't care if this commit fails on some nodes
        log.info("Commit failed while waiting for recoveries", e);
      }

      updateMappingsFromZk(jettys, clients);

      Set<String> theShards = shardToJetty.keySet();
      retry = false;
      for (String shard : theShards) {
        String failMessage = checkShardConsistency(shard, true, false);
        if (failMessage != null) {
          log.info("shard inconsistency - will retry ...");
          retry = true;
        }
      }

      if (cnt++ > 30) {
        throw new TimeoutException("Cluster state still in flux after 30 retry intervals.");
      }
      Thread.sleep(2000);
    } while (retry);
  }

  public void waitForNoShardInconsistency() throws Exception {
    log.info("Wait for no shard inconsistency");
    int cnt = 0;
    boolean retry = false;
    do {
      try {
        commit();
      } catch (Throwable t) {
        log.error("we don't care if this commit fails on some nodes", t);
      }

      updateMappingsFromZk(jettys, clients);

      Set<String> theShards = shardToJetty.keySet();
      String failMessage = null;
      for (String shard : theShards) {
        try {
          failMessage = checkShardConsistency(shard, true, true);
        } catch (Exception e) {
          // we might hit a node we just stopped
          failMessage = "hit exception:" + e.getMessage();
        }
      }

      if (failMessage != null) {
        log.info("shard inconsistency - waiting ...");
        retry = true;
      } else {
        retry = false;
      }
      cnt++;
      if (cnt > 40) break;
      Thread.sleep(2000);
    } while (retry);
  }

  void doQuery(String expectedDocs, String... queryParams) throws Exception {
    Set<String> expectedIds = new HashSet<>(StrUtils.splitSmart(expectedDocs, ",", true));

    QueryResponse rsp = cloudClient.query(params(queryParams));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  @Override
  public void distribTearDown() throws Exception {
    try {
      if (VERBOSE || printLayoutOnTearDown) {
        printLayout();
      }

      closeRestTestHarnesses(); // TODO: close here or later?

    } finally {
      resetExceptionIgnores();

      try {
        zkServer.shutdown();
      } catch (Exception e) {
        throw new RuntimeException("Exception shutting down Zk Test Server.", e);
      } finally {
        try {
          super.distribTearDown();
        } finally {
          System.clearProperty(ZK_HOST);
          System.clearProperty("collection");
          System.clearProperty(ENABLE_UPDATE_LOG);
          System.clearProperty(REMOVE_VERSION_FIELD);
          System.clearProperty("solr.directoryFactory");
          System.clearProperty("solr.test.sys.prop1");
          System.clearProperty("solr.test.sys.prop2");
          System.clearProperty(ZOOKEEPER_FORCE_SYNC);
          System.clearProperty(
              MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE);
          System.clearProperty("zkHost");
          System.clearProperty("numShards");
        }
      }
    }
  }

  protected void printLayout() throws Exception {
    SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
    zkClient.printLayoutToStream(System.out);
    zkClient.close();
  }

  protected void restartZk(int pauseMillis) throws Exception {
    log.info("Restarting ZK with a pause of {}ms in between", pauseMillis);
    zkServer.shutdown();
    // disconnect enough to test stalling, if things stall, then clientSoTimeout will be hit
    Thread.sleep(pauseMillis);
    zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
    zkServer.run(false);
  }

  // Copy a configset up from some path on the local  machine to ZK.
  // Example usage:
  //
  // copyConfigUp(TEST_PATH().resolve("configsets"), "cloud-minimal", "configset-name", zk_address);

  public static void copyConfigUp(
      Path configSetDir, String srcConfigSet, String dstConfigName, String zkAddr)
      throws Exception {

    Path fullConfDir = configSetDir.resolve(srcConfigSet);
    String[] args =
        new String[] {
          "--conf-name", dstConfigName,
          "--conf-dir", fullConfDir.toAbsolutePath().toString(),
          "-z", zkAddr
        };

    ToolRuntime runtime = new DefaultToolRuntime();
    ConfigSetUploadTool tool = new ConfigSetUploadTool(runtime);

    int res = tool.runTool(SolrCLI.processCommandLineArgs(tool, args));
    assertEquals("Tool should have returned 0 for success, returned: " + res, res, 0);
  }

  @Override
  protected void destroyServers() throws Exception {
    ExecutorService customThreadPool =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));

    customThreadPool.execute(() -> IOUtils.closeQuietly(commonCloudSolrClient));

    customThreadPool.execute(() -> IOUtils.closeQuietly(controlClient));

    for (SolrClient client : coreClients) {
      customThreadPool.execute(() -> IOUtils.closeQuietly(client));
    }

    for (SolrClient client : solrClientByCollection.values()) {
      customThreadPool.execute(() -> IOUtils.closeQuietly(client));
    }

    customThreadPool.execute(() -> IOUtils.closeQuietly(controlClientCloud));

    customThreadPool.execute(() -> IOUtils.closeQuietly(cloudClient));

    ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);

    coreClients.clear();
    solrClientByCollection.clear();

    super.destroyServers();
  }

  @Override
  protected void commit() throws Exception {
    controlClient.commit();
    cloudClient.commit();
  }

  protected CollectionAdminResponse createCollection(
      String collectionName, String configSetName, int numShards, int replicationFactor)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    return createCollection(
        null, collectionName, configSetName, numShards, replicationFactor, null, null);
  }

  protected CollectionAdminResponse createCollection(
      Map<String, List<Integer>> collectionInfos,
      String collectionName,
      Map<String, Object> collectionProps,
      SolrClient client)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    return createCollection(collectionInfos, collectionName, collectionProps, client, "conf1");
  }

  // TODO: Use CollectionAdminRequest#createCollection() instead of a raw request
  protected CollectionAdminResponse createCollection(
      Map<String, List<Integer>> collectionInfos,
      String collectionName,
      Map<String, Object> collectionProps,
      SolrClient client,
      String confSetName)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if (entry.getValue() != null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(CollectionHandlingUtils.NUM_SLICES);
    if (numShards == null) {
      String shardNames = (String) collectionProps.get(CollectionHandlingUtils.SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames, ',').size();
    }
    Integer numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.NRT_REPLICAS);
    if (numNrtReplicas == null) {
      numNrtReplicas = (Integer) collectionProps.get(ZkStateReader.REPLICATION_FACTOR);
    }
    if (numNrtReplicas == null) {
      numNrtReplicas =
          (Integer)
              CollectionHandlingUtils.COLLECTION_PROPS_AND_DEFAULTS.get(
                  ZkStateReader.REPLICATION_FACTOR);
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
      params.set("collection.configName", "conf1");
    }

    int clientIndex = random().nextInt(2);
    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(numNrtReplicas + numTlogReplicas + numPullReplicas);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    @SuppressWarnings({"rawtypes"})
    SolrRequest request =
        new GenericSolrRequest(METHOD.GET, "/admin/collections", SolrRequestType.ADMIN, params);

    CollectionAdminResponse res = new CollectionAdminResponse();
    if (client == null) {
      final String baseUrl = getBaseUrl(jettys.get(clientIndex));
      try (SolrClient adminClient = createNewSolrClient("", baseUrl)) {
        res.setResponse(adminClient.request(request));
      }
    } else {
      res.setResponse(client.request(request));
    }

    try {
      ZkStateReader.from(cloudClient)
          .waitForState(
              collectionName,
              30,
              TimeUnit.SECONDS,
              SolrCloudTestCase.activeClusterShape(
                  numShards, numShards * (numNrtReplicas + numTlogReplicas + numPullReplicas)));
    } catch (TimeoutException e) {
      throw new RuntimeException(
          "Timeout waiting for "
              + numShards
              + " shards and "
              + (numNrtReplicas + numTlogReplicas + numPullReplicas)
              + " replicas.",
          e);
    }
    return res;
  }

  protected CollectionAdminResponse createCollection(
      Map<String, List<Integer>> collectionInfos,
      String collectionName,
      String configSetName,
      int numShards,
      int replicationFactor,
      SolrClient client,
      String createNodeSetStr)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {

    int numNrtReplicas = useTlogReplicas() ? 0 : replicationFactor;
    int numTlogReplicas = useTlogReplicas() ? replicationFactor : 0;
    return createCollection(
        collectionInfos,
        collectionName,
        Utils.makeMap(
            CollectionHandlingUtils.NUM_SLICES, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            CollectionHandlingUtils.CREATE_NODE_SET, createNodeSetStr),
        client,
        configSetName);
  }

  protected CollectionAdminResponse createCollection(
      Map<String, List<Integer>> collectionInfos,
      String collectionName,
      int numShards,
      int replicationFactor,
      SolrClient client,
      String createNodeSetStr,
      String configName)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {

    int numNrtReplicas = useTlogReplicas() ? 0 : replicationFactor;
    int numTlogReplicas = useTlogReplicas() ? replicationFactor : 0;
    return createCollection(
        collectionInfos,
        collectionName,
        Utils.makeMap(
            CollectionHandlingUtils.NUM_SLICES, numShards,
            ZkStateReader.NRT_REPLICAS, numNrtReplicas,
            ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
            ZkStateReader.PULL_REPLICAS, getPullReplicaCount(),
            CollectionHandlingUtils.CREATE_NODE_SET, createNodeSetStr),
        client,
        configName);
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient. Tests that do
   * not wish to have any randomized behavior should use the {@link
   * org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */
  public static CloudSolrClient getCloudSolrClient(
      String zkHost,
      String defaultCollection,
      boolean shardLeadersOnly,
      int connectionTimeoutMillis,
      int socketTimeoutMillis) {
    RandomizingCloudSolrClientBuilder builder =
        new RandomizingCloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty());
    if (shardLeadersOnly) {
      builder.sendUpdatesOnlyToShardLeaders();
    } else {
      builder.sendUpdatesToAllReplicasInShard();
    }
    if (defaultCollection != null) {
      builder.withDefaultCollection(defaultCollection);
    }
    return builder
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }

  @Override
  protected SolrClient createNewSolrClient(int port) {
    return createNewSolrClient(DEFAULT_COLLECTION, port);
  }

  protected SolrClient createNewSolrClient(String coreName, int port) {
    String baseUrl = buildUrl(port);
    return new HttpSolrClient.Builder(baseUrl)
        .withDefaultCollection(coreName)
        .withConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
        .withSocketTimeout(60000, TimeUnit.MILLISECONDS)
        .build();
  }

  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    return getHttpSolrClient(baseUrl, collection);
  }

  protected String getBaseUrl(JettySolrRunner jetty) {
    return jetty.getBaseUrl().toString();
  }

  public static SolrInputDocument getDoc(Object... fields) throws Exception {
    return sdoc(fields);
  }

  protected void checkForCollection(String collectionName, List<Integer> numShardsNumReplicaList)
      throws Exception {
    // check for an expectedSlices new collection - we poll the state
    ZkStateReader reader = ZkStateReader.from(cloudClient);

    AtomicReference<String> message = new AtomicReference<>();
    try {
      reader.waitForState(
          collectionName,
          120,
          TimeUnit.SECONDS,
          c -> {
            int expectedSlices = numShardsNumReplicaList.get(0);
            // The Math.min thing is here, because we expect replication-factor to be reduced to if
            // there are not enough live nodes to spread all shards of a collection over different
            // nodes.
            int expectedShardsPerSlice = numShardsNumReplicaList.get(1);
            int expectedTotalShards = expectedSlices * expectedShardsPerSlice;

            if (c != null) {
              Collection<Slice> slices = c.getSlices();
              // did we find expectedSlices slices/shards?
              if (slices.size() != expectedSlices) {
                message.set(
                    "Found new collection "
                        + collectionName
                        + ", but mismatch on number of slices. Expected: "
                        + expectedSlices
                        + ", actual: "
                        + slices.size());
                return false;
              }
              int totalShards = 0;
              for (Slice slice : slices) {
                totalShards += slice.getReplicas().size();
              }
              if (totalShards != expectedTotalShards) {
                message.set(
                    "Found new collection "
                        + collectionName
                        + " with correct number of slices, but mismatch on number of shards. Expected: "
                        + expectedTotalShards
                        + ", actual: "
                        + totalShards);
                return false;
              }
              return true;
            } else {
              message.set("Could not find new collection " + collectionName);
              return false;
            }
          });
    } catch (TimeoutException e) {
      fail(message.get());
    }
  }

  private CloudSolrClient commonCloudSolrClient;

  protected CloudSolrClient getCommonCloudSolrClient() {
    synchronized (this) {
      if (commonCloudSolrClient == null) {
        commonCloudSolrClient =
            getCloudSolrClient(
                zkServer.getZkAddress(), DEFAULT_COLLECTION, random().nextBoolean(), 5000, 120000);
        commonCloudSolrClient.connect();
        if (log.isInfoEnabled()) {
          log.info(
              "Created commonCloudSolrClient with updatesToLeaders={} and parallelUpdates={}",
              commonCloudSolrClient.isUpdatesToLeaders(),
              commonCloudSolrClient.isParallelUpdates());
        }
      }
    }
    return commonCloudSolrClient;
  }

  protected CloudSolrClient getSolrClient(String collectionName) {
    return solrClientByCollection.computeIfAbsent(
        collectionName,
        k -> {
          CloudSolrClient solrClient =
              getCloudSolrClient(
                  zkServer.getZkAddress(), collectionName, random().nextBoolean(), 5000, 120000);

          solrClient.connect();
          if (log.isInfoEnabled()) {
            log.info(
                "Created solrClient for collection {} with updatesToLeaders={} and parallelUpdates={}",
                collectionName,
                solrClient.isUpdatesToLeaders(),
                solrClient.isParallelUpdates());
          }
          return solrClient;
        });
  }

  /**
   * Returns the base URL of a live Solr node hosting the specified collection
   *
   * <p>Note that the returned URL does not contain the collection name itself.
   *
   * @param clusterState used to identify which live nodes host the collection
   * @param collection the name of the collection to search for
   */
  public static String getBaseUrlFromZk(ClusterState clusterState, String collection) {
    Map<String, Slice> slices = clusterState.getCollection(collection).getSlicesMap();

    if (slices == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Could not find collection:" + collection);
    }

    for (Map.Entry<String, Slice> entry : slices.entrySet()) {
      Slice slice = entry.getValue();
      Map<String, Replica> shards = slice.getReplicasMap();
      Set<Map.Entry<String, Replica>> shardEntries = shards.entrySet();
      for (Map.Entry<String, Replica> shardEntry : shardEntries) {
        final ZkNodeProps node = shardEntry.getValue();
        final String nodeName = node.getStr(ZkStateReader.NODE_NAME_PROP);
        if (clusterState.liveNodesContain(nodeName)) {
          return node.getStr(ZkStateReader.BASE_URL_PROP);
        }
      }
    }

    throw new RuntimeException("Could not find a live node for collection:" + collection);
  }

  public static void waitForNon403or404or503(SolrClient collectionClient, String baseUrl)
      throws Exception {
    SolrException exp = null;
    final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (!timeout.hasTimedOut()) {
      boolean missing = false;

      try {
        collectionClient.query(new SolrQuery("*:*"));
      } catch (SolrException e) {
        if (!(e.code() == 403 || e.code() == 503 || e.code() == 404)) {
          throw e;
        }
        exp = e;
        missing = true;
      }
      if (!missing) {
        return;
      }
      Thread.sleep(50);
    }

    fail("Could not find the new collection - " + exp.code() + " : " + baseUrl);
  }

  // for bypassing getPullReplicaCount() - idk why setting pulla replica # should be determined at
  // class level and
  //                                       can cause weird behavior
  protected static Map<String, Object> createReplicaProps(
      int numNrtReplicas, int numTlogReplicas, int numPullReplicas, int numShards) {
    return Map.of(
        ZkStateReader.NRT_REPLICAS, numNrtReplicas,
        ZkStateReader.TLOG_REPLICAS, numTlogReplicas,
        ZkStateReader.PULL_REPLICAS, numPullReplicas,
        CollectionHandlingUtils.NUM_SLICES, numShards);
  }

  protected void createCollection(
      String collName, CloudSolrClient client, int replicationFactor, int numShards)
      throws Exception {
    int numNrtReplicas = useTlogReplicas() ? 0 : replicationFactor;
    int numTlogReplicas = useTlogReplicas() ? replicationFactor : 0;
    Map<String, Object> props =
        createReplicaProps(numNrtReplicas, numTlogReplicas, getPullReplicaCount(), numShards);
    Map<String, List<Integer>> collectionInfos = new HashMap<>();
    createCollection(collectionInfos, collName, props, client);
  }

  protected void createCollectionRetry(
      String testCollectionName, String configSetName, int numShards, int replicationFactor)
      throws SolrServerException, IOException, InterruptedException, TimeoutException {
    CollectionAdminResponse resp =
        createCollection(testCollectionName, configSetName, numShards, replicationFactor);
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.Delete req =
          CollectionAdminRequest.deleteCollection(testCollectionName);
      req.process(cloudClient);

      resp = createCollection(testCollectionName, configSetName, numShards, replicationFactor);

      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
  }

  protected Replica getShardLeader(String testCollectionName, String shardId, int timeoutSecs)
      throws Exception {
    Replica leader = null;
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      Replica tmp = null;
      try {
        tmp = ZkStateReader.from(cloudClient).getLeaderRetry(testCollectionName, shardId);
      } catch (Exception exc) {
      }
      if (tmp != null && "active".equals(tmp.getStr(ZkStateReader.STATE_PROP))) {
        leader = tmp;
        break;
      }
      Thread.sleep(1000);
    }
    assertNotNull(
        "Could not find active leader for "
            + shardId
            + " of "
            + testCollectionName
            + " after "
            + timeoutSecs
            + " secs; clusterState: "
            + printClusterStateInfo(testCollectionName),
        leader);

    return leader;
  }

  protected List<Replica> ensureAllReplicasAreActive(
      String testCollectionName, String shardId, int shards, int rf, int maxWaitSecs)
      throws Exception {
    final RTimer timer = new RTimer();

    Map<String, Replica> notLeaders = new HashMap<>();

    ZkStateReader zkr = ZkStateReader.from(cloudClient);
    zkr.forceUpdateCollection(testCollectionName); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getCollection(testCollectionName).getActiveSlices();
    assertEquals(slices.size(), shards);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    Replica leader;
    ZkShardTerms zkShardTerms =
        new ZkShardTerms(
            testCollectionName, shardId, ZkStateReader.from(cloudClient).getZkClient());
    while (waitMs < maxWaitMs && !allReplicasUp) {
      cs = cloudClient.getClusterState();
      assertNotNull(cs);
      final DocCollection docCollection = cs.getCollectionOrNull(testCollectionName);
      assertNotNull("No collection found for " + testCollectionName, docCollection);
      Slice shard = docCollection.getSlice(shardId);
      assertNotNull("No Slice for " + shardId, shard);
      allReplicasUp = true; // assume true
      Collection<Replica> replicas = shard.getReplicas();
      assertEquals(
          "Did not find correct number of replicas. Expected:" + rf + " Found:" + replicas.size(),
          replicas.size(),
          rf);

      leader = shard.getLeader();
      assertNotNull(leader);
      if (log.isInfoEnabled()) {
        log.info(
            "Found {}  replicas and leader on {} for {} in {}",
            replicas.size(),
            leader.getNodeName(),
            shardId,
            testCollectionName);
      }

      // ensure all replicas are "active" and identify the non-leader replica
      for (Replica replica : replicas) {
        if (!zkShardTerms.canBecomeLeader(replica.getName())
            || replica.getState() != Replica.State.ACTIVE) {
          if (log.isInfoEnabled()) {
            log.info("Replica {} is currently {}", replica.getName(), replica.getState());
          }
          allReplicasUp = false;
        }

        if (!leader.equals(replica)) notLeaders.put(replica.getName(), replica);
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(500L);
        } catch (Exception ignoreMe) {
        }
        waitMs += 500L;
      }
    } // end while

    zkShardTerms.close();
    if (!allReplicasUp)
      fail(
          "Didn't see all replicas for shard "
              + shardId
              + " in "
              + testCollectionName
              + " come up within "
              + maxWaitMs
              + " ms! ClusterState: "
              + printClusterStateInfo());

    if (notLeaders.isEmpty())
      fail(
          "Didn't isolate any replicas that are not the leader! ClusterState: "
              + printClusterStateInfo());

    if (log.isInfoEnabled()) {
      log.info("Took {} ms to see all replicas become active.", timer.getTime());
    }

    List<Replica> replicas = new ArrayList<>(notLeaders.values());
    return replicas;
  }

  protected String printClusterStateInfo() throws Exception {
    return printClusterStateInfo(null);
  }

  protected String printClusterStateInfo(String collection) throws Exception {
    ZkStateReader.from(cloudClient).forceUpdateCollection(collection);
    String cs;
    ClusterState clusterState = cloudClient.getClusterState();
    if (collection != null) {
      cs = clusterState.getCollection(collection).toString();
    } else {
      cs = ClusterStateUtil.toDebugAllStatesString(clusterState);
    }
    return cs;
  }

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    String coreName = replica.getCoreName();
    boolean reloadedOk = false;
    try (SolrClient client = getHttpSolrClient(replica.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      Thread.sleep(1000);

      // send reload command for the collection
      log.info("Sending RELOAD command for {}", testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      var request =
          new GenericSolrRequest(METHOD.GET, "/admin/collections", SolrRequestType.ADMIN, params);
      request.setPath("/admin/collections");
      client.request(request);
      Thread.sleep(2000); // reload can take a short while

      // verify reload is done, waiting up to 30 seconds for slow test environments
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        statusResp = CoreAdminRequest.getStatus(coreName, client);
        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
        if (startTimeAfterReload > leaderCoreStartTime) {
          reloadedOk = true;
          break;
        }
        // else ... still waiting to see the reloaded core report a later start time
        Thread.sleep(1000);
      }
    }
    return reloadedOk;
  }

  protected void logReplicaTypesReplicationInfo(String collectionName, ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException, IOException {
    log.info("## Collecting extra Replica.Type information of the cluster");
    zkStateReader.updateLiveNodes();
    StringBuilder builder = new StringBuilder();
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    for (Slice s : collection.getSlices()) {
      Replica leader = s.getLeader();
      for (Replica r : s.getReplicas()) {
        if (!r.isActive(zkStateReader.getClusterState().getLiveNodes())) {
          builder.append(
              String.format(
                  Locale.ROOT,
                  "Replica %s not in liveNodes or is not active%s",
                  r.getName(),
                  System.lineSeparator()));
          continue;
        }
        if (r.equals(leader)) {
          builder.append(
              String.format(
                  Locale.ROOT, "Replica %s is leader%s", r.getName(), System.lineSeparator()));
        }
        logReplicationDetails(r, builder);
      }
    }
    log.info("Summary of the cluster: {}", builder);
  }

  protected void waitForReplicationFromReplicas(
      String collectionName, ZkStateReader zkStateReader, TimeOut timeout)
      throws KeeperException, InterruptedException, IOException {
    log.info("waitForReplicationFromReplicas: {}", collectionName);
    zkStateReader.forceUpdateCollection(collectionName);
    DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
    Map<String, CoreContainer> containers = new HashMap<>();
    for (JettySolrRunner runner : jettys) {
      if (!runner.isRunning()) {
        continue;
      }
      containers.put(runner.getNodeName(), runner.getCoreContainer());
    }
    for (Slice s : collection.getSlices()) {
      Replica leader =
          zkStateReader.getLeaderRetry(
              collectionName, s.getName(), (int) timeout.timeLeft(TimeUnit.MILLISECONDS));
      long leaderIndexVersion = -1;
      while (!timeout.hasTimedOut()) {
        leaderIndexVersion = getIndexVersion(leader);
        if (leaderIndexVersion >= 0) {
          break;
        }
        Thread.sleep(1000);
      }
      if (timeout.hasTimedOut()) {
        fail("Unable to get leader indexVersion");
      }
      for (Replica pullReplica : s.getReplicas(EnumSet.of(Replica.Type.PULL, Replica.Type.TLOG))) {
        if (!zkStateReader.getClusterState().liveNodesContain(pullReplica.getNodeName())) {
          continue;
        }
        while (true) {
          long replicaIndexVersion = getIndexVersion(pullReplica);
          if (leaderIndexVersion == replicaIndexVersion) {
            if (log.isInfoEnabled()) {
              log.info(
                  "Leader replica's version ({}) in sync with replica({}): {} == {}",
                  leader.getName(),
                  pullReplica.getName(),
                  leaderIndexVersion,
                  replicaIndexVersion);
            }

            // Make sure the host is serving the correct version
            try (SolrCore core =
                containers.get(pullReplica.getNodeName()).getCore(pullReplica.getCoreName())) {
              RefCounted<SolrIndexSearcher> ref = core.getRegisteredSearcher();
              try {
                SolrIndexSearcher searcher = ref.get();
                String servingVersion =
                    searcher
                        .getIndexReader()
                        .getIndexCommit()
                        .getUserData()
                        .get(SolrIndexWriter.COMMIT_TIME_MSEC_KEY);
                if (servingVersion != null
                    && Long.parseLong(servingVersion) == replicaIndexVersion) {
                  break;
                } else {
                  if (log.isInfoEnabled()) {
                    log.info(
                        "Replica {} has the correct version replicated, but the searcher is not ready yet. Replicated version: {}, Serving version: {}",
                        pullReplica.getName(),
                        replicaIndexVersion,
                        servingVersion);
                  }
                }
              } finally {
                if (ref != null) ref.decref();
              }
            }
          } else {
            if (leaderIndexVersion > replicaIndexVersion) {
              if (log.isInfoEnabled()) {
                log.info(
                    "{} version is {} and leader's is {}, will wait for replication",
                    pullReplica.getName(),
                    replicaIndexVersion,
                    leaderIndexVersion);
              }
            } else {
              if (log.isInfoEnabled()) {
                log.info(
                    "Leader replica's version ({}) is lower than pull replica({}): {} < {}",
                    leader.getName(),
                    pullReplica.getName(),
                    leaderIndexVersion,
                    replicaIndexVersion);
              }
            }
          }
          if (timeout.hasTimedOut()) {
            logReplicaTypesReplicationInfo(collectionName, zkStateReader);
            fail(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for replica %s (%d) to replicate from leader %s (%d)",
                    pullReplica.getName(),
                    replicaIndexVersion,
                    leader.getName(),
                    leaderIndexVersion));
          }
          Thread.sleep(1000);
        }
      }
    }
  }

  protected void waitForAllWarmingSearchers() throws InterruptedException {
    log.info("waitForAllWarmingSearchers");
    for (JettySolrRunner jetty : jettys) {
      if (!jetty.isRunning()) {
        continue;
      }
      for (SolrCore core : jetty.getCoreContainer().getCores()) {
        waitForWarming(core);
      }
    }
  }

  protected long getIndexVersion(Replica replica) throws IOException {
    try (SolrClient client =
        new HttpSolrClient.Builder(replica.getBaseUrl())
            .withDefaultCollection(replica.getCoreName())
            .build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_SHOW_COMMITS);
      try {
        QueryResponse response = client.query(params);
        @SuppressWarnings("unchecked")
        List<NamedList<Object>> commits =
            (List<NamedList<Object>>)
                response.getResponse().get(ReplicationHandler.CMD_SHOW_COMMITS);
        Collections.max(
            commits,
            (a, b) -> ((Long) a.get("indexVersion")).compareTo((Long) b.get("indexVersion")));
        return (long)
            Collections.max(
                    commits,
                    (a, b) ->
                        ((Long) a.get("indexVersion")).compareTo((Long) b.get("indexVersion")))
                .get("indexVersion");
      } catch (SolrServerException e) {
        log.warn(
            "Exception getting version from {}, will return an invalid version to retry.",
            replica.getName(),
            e);
        return -1;
      }
    }
  }

  /**
   * Logs a WARN if collection can't be deleted, but does not fail or throw an exception
   *
   * @return true if success, else false
   */
  protected static boolean attemptCollectionDelete(CloudSolrClient client, String collectionName) {
    // try to clean up
    try {
      CollectionAdminRequest.deleteCollection(collectionName).process(client);
      return true;
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} - ignoring", collectionName);
    }
    return false;
  }

  protected void logReplicationDetails(Replica replica, StringBuilder builder) throws IOException {
    try (SolrClient client =
        new HttpSolrClient.Builder(replica.getBaseUrl())
            .withDefaultCollection(replica.getCoreName())
            .build()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("qt", "/replication");
      params.set(ReplicationHandler.COMMAND, ReplicationHandler.CMD_DETAILS);
      try {
        QueryResponse response = client.query(params);
        builder.append(
            String.format(
                Locale.ROOT,
                "%s: %s%s",
                replica.getName(),
                response.getResponse(),
                System.lineSeparator()));
      } catch (SolrServerException e) {
        log.warn("Unable to ger replication details for replica {}", replica.getName(), e);
      }
    }
  }

  public static RequestStatusState getRequestStateAfterCompletion(
      String requestId, int waitForSeconds, SolrClient client)
      throws IOException, SolrServerException {
    RequestStatusState state = null;
    final TimeOut timeout = new TimeOut(waitForSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);

    while (!timeout.hasTimedOut()) {
      state = getRequestState(requestId, client);
      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        return state;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted whie waiting for request completion. Last state seen: " + state, e);
      }
    }

    return state;
  }

  static RequestStatusState getRequestState(int requestId, SolrClient client)
      throws IOException, SolrServerException {
    return getRequestState(String.valueOf(requestId), client);
  }

  static RequestStatusState getRequestState(String requestId, SolrClient client)
      throws IOException, SolrServerException {
    CollectionAdminResponse response = getStatusResponse(requestId, client);

    @SuppressWarnings({"rawtypes"})
    NamedList innerResponse = (NamedList) response.getResponse().get("status");
    return RequestStatusState.fromKey((String) innerResponse.get("state"));
  }

  static CollectionAdminResponse getStatusResponse(String requestId, SolrClient client)
      throws SolrServerException, IOException {
    return CollectionAdminRequest.requestStatus(requestId).process(client);
  }

  protected void setupRestTestHarnesses() {
    for (final JettySolrRunner jetty : jettys) {
      RestTestHarness harness =
          new RestTestHarness(
              () -> jetty.getBaseUrl().toString() + "/" + DEFAULT_TEST_COLLECTION_NAME);
      restTestHarnesses.add(harness);
    }
  }

  protected void closeRestTestHarnesses() throws IOException {
    for (RestTestHarness h : restTestHarnesses) {
      h.close();
    }
  }

  protected RestTestHarness randomRestTestHarness() {
    return restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
  }

  protected RestTestHarness randomRestTestHarness(Random random) {
    return restTestHarnesses.get(random.nextInt(restTestHarnesses.size()));
  }

  protected void forAllRestTestHarnesses(Consumer<RestTestHarness> op) {
    restTestHarnesses.forEach(op);
  }
}
