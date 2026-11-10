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

package org.apache.solr.client.solrj.impl;

import static org.apache.solr.client.solrj.impl.BaseHttpClusterStateProvider.SYS_PROP_CACHE_TIMEOUT_SECONDS;
import static org.apache.solr.common.util.URLUtil.getNodeNameForBaseUrl;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.ErrorListenerPathable;
import org.apache.curator.framework.api.SyncBuilder;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpHeader;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStateProviderTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SOLR_SSL_ENABLED = "solr.ssl.enabled";
  private static final long SYNC_TEST_TIMEOUT_SECONDS = 30;

  static class UserAgentChangingJdkClient extends HttpJdkSolrClient {

    final String userAgent;

    protected UserAgentChangingJdkClient(Builder builder, String userAgent) {
      super(null, builder);
      this.userAgent = userAgent;
    }

    @Override
    protected PreparedRequest prepareRequest(
        String overrideBaseUrl, SolrRequest<?> solrRequest, String collection)
        throws SolrServerException, IOException {
      var pr = super.prepareRequest(overrideBaseUrl, solrRequest, collection);
      pr.reqb.header("User-Agent", userAgent);
      return pr;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig(
            "conf",
            getFile("solrj")
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .configure();
    cluster.waitForAllNodes(30);
    System.setProperty(SYS_PROP_CACHE_TIMEOUT_SECONDS, "1");
  }

  @After
  public void cleanup() throws Exception {
    while (cluster.getJettySolrRunners().size() < 2) {
      cluster.startJettySolrRunner();
    }
  }

  @ParametersFactory
  public static Iterable<String[]> parameters() throws NoSuchMethodException {
    return List.of(
        new String[] {"http2ClusterStateProvider"}, new String[] {"zkClientClusterStateProvider"});
  }

  static class ClosingHttpClusterStateProvider extends HttpClusterStateProvider<HttpSolrClient> {
    public ClosingHttpClusterStateProvider(List<String> solrUrls, HttpSolrClient httpClient)
        throws Exception {
      super(solrUrls, httpClient);
    }

    @Override
    public void close() throws IOException {
      super.close();
      try {
        httpClient.close();
      } catch (IOException e) {
        log.error("error closing the client.", e);
      }
    }
  }

  private static HttpClusterStateProvider<?> http2ClusterStateProvider(String userAgent) {
    try {
      var useJdkProvider = random().nextBoolean();
      HttpSolrClient client;

      if (userAgent != null) {
        if (useJdkProvider) {
          client =
              new UserAgentChangingJdkClient(
                  new HttpJdkSolrClient.Builder()
                      .withSSLContext(MockTrustManager.ALL_TRUSTING_SSL_CONTEXT),
                  userAgent);
        } else {
          var http2SolrClient = new HttpJettySolrClient.Builder().build();
          http2SolrClient
              .getHttpClient()
              .setUserAgentField(new HttpField(HttpHeader.USER_AGENT, userAgent));
          client = http2SolrClient;
        }
      } else {
        client =
            useJdkProvider
                ? new HttpJdkSolrClient.Builder()
                    .withSSLContext(MockTrustManager.ALL_TRUSTING_SSL_CONTEXT)
                    .build()
                : new HttpJettySolrClient.Builder().build();
      }
      var clientClassName = client.getClass().getName();
      log.info("Using Http client implementation: {}", clientClassName);

      var csp =
          new ClosingHttpClusterStateProvider(
              List.of(
                  cluster.getJettySolrRunner(0).getBaseUrl().toString(),
                  cluster.getJettySolrRunner(1).getBaseUrl().toString()),
              client);
      return csp;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static HttpClusterStateProvider<?> http2ClusterStateProvider() {
    return http2ClusterStateProvider(null);
  }

  private static ClusterStateProvider zkClientClusterStateProvider() {
    return new ZkClientClusterStateProvider(cluster.getZkStateReader());
  }

  private final Supplier<ClusterStateProvider> cspSupplier;

  public ClusterStateProviderTest(String method) throws Exception {
    this.cspSupplier =
        () -> {
          try {
            return (ClusterStateProvider) getClass().getDeclaredMethod(method).invoke(getClass());
          } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
          }
        };

    cluster.deleteAllCollections();
  }

  private ClusterStateProvider createClusterStateProvider() {
    return cspSupplier.get();
  }

  @Test
  public void testGetClusterState() throws Exception {

    createCollection("testGetClusterState");
    createCollection("testGetClusterState2");

    try (ClusterStateProvider provider = createClusterStateProvider()) {

      ClusterState clusterState = provider.getClusterState();

      DocCollection docCollection = clusterState.getCollection("testGetClusterState");
      assertEquals(
          getCreationTimeFromClusterStatus("testGetClusterState"), docCollection.getCreationTime());

      docCollection = clusterState.getCollection("testGetClusterState2");
      assertEquals(
          getCreationTimeFromClusterStatus("testGetClusterState2"),
          docCollection.getCreationTime());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSyncWaitsForCallbackAndPropagatesFailure() throws Exception {
    assumeWorkingMockito();
    SolrZkClient zkClient = spy(cluster.getZkClient());
    CuratorFramework curator = mock(CuratorFramework.class);
    SyncBuilder builder = mock(SyncBuilder.class);
    ErrorListenerPathable<Void> operation = mock(ErrorListenerPathable.class);
    AtomicReference<BackgroundCallback> callback = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    doReturn(curator).when(zkClient).getCuratorFramework();
    when(curator.sync()).thenReturn(builder);
    when(builder.inBackground(any(BackgroundCallback.class)))
        .thenAnswer(
            invocation -> {
              callback.set(invocation.getArgument(0));
              return operation;
            });
    when(operation.forPath(anyString()))
        .thenAnswer(
            invocation -> {
              started.countDown();
              return null;
            });

    ExecutorService executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(1, new SolrNamedThreadFactory("zk-sync-test"));
    try {
      Future<?> result =
          executor.submit(
              () -> {
                zkClient.sync(ZkStateReader.COLLECTIONS_ZKNODE);
                return null;
              });
      assertTrue(started.await(SYNC_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      expectThrows(TimeoutException.class, () -> result.get(200, TimeUnit.MILLISECONDS));
      CuratorEvent event = mock(CuratorEvent.class);
      when(event.getResultCode()).thenReturn(KeeperException.Code.CONNECTIONLOSS.intValue());
      callback.get().processResult(curator, event);
      ExecutionException error =
          expectThrows(
              ExecutionException.class,
              () -> result.get(SYNC_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(error.getCause() instanceof KeeperException.ConnectionLossException);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(SYNC_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testProviderRefreshesCollectionMissingFromLocalState() throws Exception {
    ZkTestServer server = new ZkTestServer(createTempDir("stale-state"));
    server.run();
    try (SolrZkClient zkClient =
            new SolrZkClient.Builder().withUrl(server.getZkAddress()).build();
        ZkStateReader reader = new ZkStateReader(zkClient);
        ZkClientClusterStateProvider provider = new ZkClientClusterStateProvider(reader)) {
      ZkController.createClusterZkNodes(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), -1, new ZLibCompressor());
      synchronized (reader.getUpdateLock()) {
        assertNull(reader.getClusterState().getCollectionRef("new_collection"));
        zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/new_collection", true);
        DocCollection state =
            DocCollection.create(
                "new_collection",
                new HashMap<>(),
                Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME),
                DocRouter.DEFAULT,
                0,
                Instant.now(),
                PerReplicaStatesOps.getZkClientPrsSupplier(
                    zkClient, DocCollection.getCollectionPath("new_collection")));
        writer.enqueueUpdate(
            reader.getClusterState(), List.of(new ZkWriteCommand("new_collection", state)), null);
        writer.writePendingUpdates();

        // The watcher cannot publish the new collection while this lock is held.
        assertNull(reader.getClusterState().getCollectionRef("new_collection"));
        ClusterState.CollectionRef refreshed = provider.getState("new_collection");
        assertNotNull(refreshed);
        assertEquals("new_collection", refreshed.get().getName());
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testProviderUnknownCollection() throws Exception {
    assumeWorkingMockito();
    ZkStateReader reader = mock(ZkStateReader.class);
    SolrZkClient zkClient = mock(SolrZkClient.class);
    when(reader.getClusterState()).thenReturn(mock(ClusterState.class));
    when(reader.getAliases()).thenReturn(Aliases.EMPTY);
    when(reader.getZkClient()).thenReturn(zkClient);
    try (ZkClientClusterStateProvider provider = new ZkClientClusterStateProvider(reader)) {
      assertNull(provider.getState("unknown"));
      InOrder order = inOrder(zkClient, reader);
      order.verify(zkClient).sync(ZkStateReader.COLLECTIONS_ZKNODE);
      order.verify(reader).forceUpdateCollection("unknown");
    }
  }

  @Test
  public void testProviderPropagatesCollectionReadFailures() throws Exception {
    assumeWorkingMockito();
    SolrZkClient zkClient = spy(cluster.getZkClient());
    try (ZkStateReader reader = new ZkStateReader(zkClient);
        ZkClientClusterStateProvider provider = new ZkClientClusterStateProvider(reader)) {
      reader.createClusterStateWatchersAndUpdate();
      KeeperException failure = new KeeperException.ConnectionLossException();
      doThrow(failure)
          .when(zkClient)
          .getData(eq(DocCollection.getCollectionPath("missing")), isNull(), any(Stat.class));
      SolrException readerError =
          expectThrows(SolrException.class, () -> reader.forceUpdateCollection("missing"));
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, readerError.code());
      assertSame(failure, readerError.getCause());

      SolrException error = expectThrows(SolrException.class, () -> provider.getState("missing"));
      assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, error.code());
      assertSame(failure, error.getCause());

      InterruptedException interrupted = new InterruptedException("interrupted");
      doThrow(interrupted)
          .when(zkClient)
          .getData(eq(DocCollection.getCollectionPath("interrupted")), isNull(), any(Stat.class));
      try {
        error = expectThrows(SolrException.class, () -> provider.getState("interrupted"));
        assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, error.code());
        assertSame(interrupted, error.getCause());
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }
    }
  }

  @Test
  public void testGetState() throws Exception {

    createCollection("testGetState");

    try (ClusterStateProvider provider = createClusterStateProvider()) {

      ClusterState.CollectionRef collectionRef = provider.getState("testGetState");
      DocCollection docCollection = collectionRef.get();
      assertNotNull(docCollection);
      assertEquals(
          getCreationTimeFromClusterStatus("testGetState"), docCollection.getCreationTime());
    }
  }

  private void createCollection(String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.Create request =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 0, 1, 0);
    request.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);
  }

  @SuppressWarnings("unchecked")
  private Instant getCreationTimeFromClusterStatus(String collectionName)
      throws SolrServerException, IOException {
    CollectionAdminRequest.ClusterStatus request = CollectionAdminRequest.getClusterStatus();
    request.setCollectionName(collectionName);
    CollectionAdminResponse clusterStatusResponse = request.process(cluster.getSolrClient());
    NamedList<Object> response = clusterStatusResponse.getResponse();

    NamedList<Object> cluster = (NamedList<Object>) response.get("cluster");
    Map<String, Object> collections = (Map<String, Object>) cluster.get("collections");
    Map<String, Object> collection = (Map<String, Object>) collections.get(collectionName);
    return Instant.ofEpochMilli((long) collection.get("creationTimeMillis"));
  }

  @Test
  public void testClusterStateProvider() throws SolrServerException, IOException {
    // we'll test equivalency of the two cluster state providers

    CollectionAdminRequest.setClusterProperty("ext.foo", "bar").process(cluster.getSolrClient());

    createCollection("col1");
    createCollection("col2");

    try (var cspZk = zkClientClusterStateProvider();
        var cspHttp = http2ClusterStateProvider()) {
      assertThat(cspZk.getClusterProperties(), Matchers.hasEntry("ext.foo", "bar"));
      assertThat(
          cspZk.getClusterProperties().entrySet(),
          containsInAnyOrder(cspHttp.getClusterProperties().entrySet().toArray()));

      assertThat(cspHttp.getCollection("col1"), equalTo(cspZk.getCollection("col1")));

      final var clusterStateZk = cspZk.getClusterState();
      final var clusterStateHttp = cspHttp.getClusterState();
      assertThat(
          clusterStateHttp.getLiveNodes(),
          containsInAnyOrder(clusterStateHttp.getLiveNodes().toArray()));
      assertEquals(2, clusterStateZk.size());
      assertEquals(clusterStateZk.size(), clusterStateHttp.size());
      assertThat(
          clusterStateHttp.collectionStream().toList(),
          containsInAnyOrder(clusterStateHttp.collectionStream().toArray()));

      assertThat(
          clusterStateZk.getCollection("col2"), equalTo(clusterStateHttp.getCollection("col2")));
    }
  }

  @Test
  public void testClusterStateProviderOldVersion() throws SolrServerException, IOException {
    CollectionAdminRequest.setClusterProperty("ext.foo", "bar").process(cluster.getSolrClient());
    createCollection("col1");
    createCollection("col2");

    try (var cspZk = zkClientClusterStateProvider();
        // SolrJ < version 9.9.0 for non streamed response
        var cspHttp =
            http2ClusterStateProvider(
                "Solr[" + MethodHandles.lookup().lookupClass().getName() + "] " + "9.8.0")) {

      assertThat(cspHttp.getCollection("col1"), equalTo(cspZk.getCollection("col1")));

      final var clusterStateZk = cspZk.getClusterState();
      final var clusterStateHttp = cspHttp.getClusterState();
      assertThat(
          clusterStateHttp.getLiveNodes(),
          containsInAnyOrder(clusterStateHttp.getLiveNodes().toArray()));
      assertEquals(2, clusterStateZk.size());
      assertEquals(clusterStateZk.size(), clusterStateHttp.size());
      assertThat(
          clusterStateHttp.collectionStream().toList(),
          containsInAnyOrder(clusterStateHttp.collectionStream().toArray()));

      assertThat(
          clusterStateZk.getCollection("col2"), equalTo(clusterStateHttp.getCollection("col2")));
    }

    try (var cspZk = zkClientClusterStateProvider();
        // Even older SolrJ versions for non streamed response
        var cspHttp =
            http2ClusterStateProvider(
                "Solr[" + MethodHandles.lookup().lookupClass().getName() + "] " + "2.0")) {

      assertThat(cspHttp.getCollection("col1"), equalTo(cspZk.getCollection("col1")));

      final var clusterStateZk = cspZk.getClusterState();
      final var clusterStateHttp = cspHttp.getClusterState();
      assertThat(
          clusterStateHttp.getLiveNodes(),
          containsInAnyOrder(clusterStateHttp.getLiveNodes().toArray()));
      assertEquals(2, clusterStateZk.size());
      assertEquals(clusterStateZk.size(), clusterStateHttp.size());
      assertThat(
          clusterStateHttp.collectionStream().toList(),
          containsInAnyOrder(clusterStateHttp.collectionStream().toArray()));

      assertThat(
          clusterStateZk.getCollection("col2"), equalTo(clusterStateHttp.getCollection("col2")));
    }
  }

  @Test
  public void testClusterStateProviderEmptySolrVersion() throws SolrServerException, IOException {
    CollectionAdminRequest.setClusterProperty("ext.foo", "bar").process(cluster.getSolrClient());
    createCollection("col1");
    createCollection("col2");

    try (var cspZk = zkClientClusterStateProvider();
        var cspHttp = http2ClusterStateProvider("")) {

      assertThat(cspHttp.getCollection("col1"), equalTo(cspZk.getCollection("col1")));

      final var clusterStateZk = cspZk.getClusterState();
      final var clusterStateHttp = cspHttp.getClusterState();
      assertThat(
          clusterStateHttp.getLiveNodes(),
          containsInAnyOrder(clusterStateHttp.getLiveNodes().toArray()));
      assertEquals(2, clusterStateZk.size());
      assertEquals(clusterStateZk.size(), clusterStateHttp.size());
      assertThat(
          clusterStateHttp.collectionStream().toList(),
          containsInAnyOrder(clusterStateHttp.collectionStream().toArray()));

      assertThat(
          clusterStateZk.getCollection("col2"), equalTo(clusterStateHttp.getCollection("col2")));
    }
  }

  @Test
  public void testClusterStateProviderDownedInitialLiveNodes() throws Exception {
    try (var cspHttp = http2ClusterStateProvider()) {
      var jettyNode1 = cluster.getJettySolrRunner(0);
      var jettyNode2 = cluster.getJettySolrRunner(1);

      String nodeName1 = getNodeNameForBaseUrl(jettyNode1.getBaseUrl().toString());
      String nodeName2 = getNodeNameForBaseUrl(jettyNode2.getBaseUrl().toString());

      Set<String> actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(2, actualLiveNodes.size());
      assertEquals(Set.of(nodeName1, nodeName2), actualLiveNodes);

      cluster.stopJettySolrRunner(jettyNode1);
      waitForCSPCacheTimeout();

      actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualLiveNodes.size());
      assertEquals(Set.of(nodeName2), actualLiveNodes);

      cluster.startJettySolrRunner(jettyNode1, true);
      cluster.stopJettySolrRunner(jettyNode2);
      waitForCSPCacheTimeout();

      // Should still be reachable because backup nodes
      actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualLiveNodes.size());
      assertEquals(Set.of(nodeName1), actualLiveNodes);
    }
  }

  @Test
  public void testClusterStateProviderLiveNodesWithNewNode() throws Exception {
    try (var cspHttp = http2ClusterStateProvider()) {
      var jettyNode1 = cluster.getJettySolrRunner(0);
      var jettyNode2 = cluster.getJettySolrRunner(1);
      var jettyNode3 = cluster.startJettySolrRunner();

      String nodeName1 = getNodeNameForBaseUrl(jettyNode1.getBaseUrl().toString());
      String nodeName2 = getNodeNameForBaseUrl(jettyNode2.getBaseUrl().toString());
      String nodeName3 = getNodeNameForBaseUrl(jettyNode3.getBaseUrl().toString());
      waitForCSPCacheTimeout();

      Set<String> actualKnownNodes = cspHttp.getLiveNodes();
      assertEquals(3, actualKnownNodes.size());
      assertEquals(Set.of(nodeName1, nodeName2, nodeName3), actualKnownNodes);

      // Stop all backup nodes
      cluster.stopJettySolrRunner(jettyNode1);
      cluster.stopJettySolrRunner(jettyNode2);
      waitForCSPCacheTimeout();

      long startTimeNs = System.nanoTime();
      actualKnownNodes = cspHttp.getLiveNodes();
      long liveNodeFetchTimeMs =
          TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);
      assertEquals(1, actualKnownNodes.size());
      assertEquals(Set.of(nodeName3), actualKnownNodes);
      // This should already be cached, because it is being updated in the background
      assertThat(
          "Cached getLiveNodes() should take no more than 2 milliseconds",
          liveNodeFetchTimeMs,
          lessThanOrEqualTo(2L));

      // Bring back a backup node and take down the new node
      cluster.startJettySolrRunner(jettyNode2, true);
      cluster.stopJettySolrRunner(jettyNode3);
      waitForCSPCacheTimeout();

      actualKnownNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualKnownNodes.size());
      assertEquals(Set.of(nodeName2), actualKnownNodes);
    }
  }

  @Test
  public void testHttpClusterStateProviderUrlScheme() throws Exception {
    final List<String> solrUrls =
        List.of("https://localhost:8983/solr", "https://localhost:8984/solr");
    try (var httpClient = new HttpJettySolrClient.Builder().build();
        ClusterStateProvider clusterStateProvider =
            new HttpClusterStateProvider<>(solrUrls, httpClient)) {
      assertEquals("https", clusterStateProvider.getUrlScheme());
    }
  }

  @Test
  public void testZkClusterStateProviderUrlScheme() throws Exception {
    final Path zkDir = createTempDir("zkData");

    final ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient client =
          new SolrZkClient.Builder().withUrl(server.getZkAddress()).build()) {
        ZkController.createClusterZkNodes(client);

        testUrlSchemeWithSystemProperties(client);

        assumeFalse(
            "Skip schema default and cluster property checks when SSL is enabled", isSSLMode());
        testUrlSchemeWithClusterProperties(client);
        testUrlSchemeDefault(client);
      }
    } finally {
      server.shutdown();
    }
  }

  /**
   * Used by {@link #testZkClusterStateProviderUrlScheme}. Only valid if {@link #isSSLMode} is false
   */
  private void testUrlSchemeDefault(SolrZkClient client) throws Exception {
    assertFalse("These checks are not supported in SSL mode", isSSLMode());
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      assertEquals("http", clusterStateProvider.getUrlScheme());
    }
  }

  /** Used by {@link #testZkClusterStateProviderUrlScheme} */
  private void testUrlSchemeWithSystemProperties(SolrZkClient client) throws Exception {
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      String expectedUrlScheme = isSSLMode() ? "https" : "http";
      assertEquals(expectedUrlScheme, clusterStateProvider.getUrlScheme());
    }
  }

  /**
   * Used by {@link #testZkClusterStateProviderUrlScheme}. Only valid if {@link #isSSLMode} is false
   */
  private void testUrlSchemeWithClusterProperties(SolrZkClient client) throws Exception {
    assertFalse("These checks are not supported in SSL mode", isSSLMode());
    ClusterProperties cp = new ClusterProperties(client);
    cp.setClusterProperty("urlScheme", "ftp");
    try (var zkStateReader = new ZkStateReader(client);
        var clusterStateProvider = new ZkClientClusterStateProvider(zkStateReader)) {
      zkStateReader.createClusterStateWatchersAndUpdate();
      assertEquals("ftp", clusterStateProvider.getUrlScheme());
    }
  }

  private void waitForCSPCacheTimeout() throws InterruptedException {
    Thread.sleep(2000);
  }
}
