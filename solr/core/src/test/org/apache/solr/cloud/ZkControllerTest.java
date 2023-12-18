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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class ZkControllerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 10000;

  @BeforeClass
  public static void beforeClass() {}

  @AfterClass
  public static void afterClass() {}

  public void testNodeNameUrlConversion() throws Exception {

    // nodeName from parts
    assertEquals("localhost:8888_solr", ZkController.generateNodeName("localhost", "8888", "solr"));
    assertEquals(
        "localhost:8888_solr", ZkController.generateNodeName("localhost", "8888", "/solr"));
    assertEquals(
        "localhost:8888_solr", ZkController.generateNodeName("localhost", "8888", "/solr/"));
    // root context
    assertEquals("localhost:8888_", ZkController.generateNodeName("localhost", "8888", ""));
    assertEquals("localhost:8888_", ZkController.generateNodeName("localhost", "8888", "/"));
    // subdir
    assertEquals(
        "foo-bar:77_solr%2Fsub_dir",
        ZkController.generateNodeName("foo-bar", "77", "solr/sub_dir"));
    assertEquals(
        "foo-bar:77_solr%2Fsub_dir",
        ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir"));
    assertEquals(
        "foo-bar:77_solr%2Fsub_dir",
        ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir/"));

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      try (SolrZkClient client =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {

        ZkController.createClusterZkNodes(client);

        try (ZkStateReader zkStateReader = new ZkStateReader(client)) {
          zkStateReader.createClusterStateWatchersAndUpdate();

          // getBaseUrlForNodeName
          assertEquals(
              "http://zzz.xxx:1234/solr", zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));
          assertEquals(
              "http://zzz_xxx:1234/solr", zkStateReader.getBaseUrlForNodeName("zzz_xxx:1234_solr"));
          assertEquals("http://xxx:99", zkStateReader.getBaseUrlForNodeName("xxx:99_"));
          assertEquals(
              "http://foo-bar.baz.org:9999/some_dir",
              zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_some_dir"));
          assertEquals(
              "http://foo-bar.baz.org:9999/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_solr%2Fsub_dir"));

          // generateNodeName + getBaseUrlForNodeName
          assertEquals(
              "http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo", "9876", "solr")));
          assertEquals(
              "http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo", "9876", "/solr")));
          assertEquals(
              "http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo", "9876", "/solr/")));
          assertEquals(
              "http://foo.bar.com:9876/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo.bar.com", "9876", "solr/sub_dir")));
          assertEquals(
              "http://foo.bar.com:9876/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo.bar.com", "9876", "/solr/sub_dir/")));
          assertEquals(
              "http://foo-bar:9876",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo-bar", "9876", "")));
          assertEquals(
              "http://foo-bar:9876",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo-bar", "9876", "/")));
          assertEquals(
              "http://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo-bar.com", "80", "some_dir")));
          assertEquals(
              "http://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));
        }

        ClusterProperties cp = new ClusterProperties(client);
        cp.setClusterProperty("urlScheme", "https");

        // Verify the URL Scheme is taken into account

        try (ZkStateReader zkStateReader = new ZkStateReader(client)) {

          zkStateReader.createClusterStateWatchersAndUpdate();

          assertEquals(
              "https://zzz.xxx:1234/solr",
              zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));

          assertEquals(
              "https://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName(
                  ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));
        }
      }
    } finally {
      server.shutdown();
    }
  }

  public void testGetHostName() throws Exception {
    Path zkDir = createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      CoreContainer cc = getCoreContainer();
      ZkController zkController = null;

      try {
        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
        zkController =
            new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig, () -> null);
      } catch (IllegalArgumentException e) {
        fail("ZkController did not normalize host name correctly");
      } finally {
        if (zkController != null) zkController.close();
        if (cc != null) {
          cc.shutdown();
        }
      }
    } finally {
      server.shutdown();
    }
  }

  @LogLevel(value = "org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.overseer=DEBUG")
  public void testPublishAndWaitForDownStates() throws Exception {

    /*
    This test asserts that if zkController.publishAndWaitForDownStates uses only core name to check if all local
    cores are down then the method will return immediately but if it uses coreNodeName (as it does after SOLR-6665 then
    the method will time out).
    We set up the cluster state in such a way that two replicas with same core name exist on non-existent nodes
    and core container also has a dummy core that has the same core name. The publishAndWaitForDownStates before SOLR-6665
    would only check the core names and therefore return immediately but after SOLR-6665 it should time out.
     */

    assumeWorkingMockito();
    final String collectionName = "testPublishAndWaitForDownStates";
    Path zkDir = createTempDir(collectionName);

    String nodeName = "127.0.0.1:8983_solr";

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AtomicReference<ZkController> zkControllerRef = new AtomicReference<>();
      CoreContainer cc =
          new MockCoreContainer() {
            @Override
            public List<CoreDescriptor> getCoreDescriptors() {
              CoreDescriptor descriptor =
                  new CoreDescriptor(
                      collectionName,
                      TEST_PATH(),
                      Collections.emptyMap(),
                      new Properties(),
                      zkControllerRef.get());
              // non-existent coreNodeName, this will cause zkController.publishAndWaitForDownStates
              // to wait indefinitely when using coreNodeName but usage of core name alone will
              // return immediately
              descriptor.getCloudDescriptor().setCoreNodeName("core_node0");
              return Collections.singletonList(descriptor);
            }
          };
      ZkController zkController = null;

      try {
        CloudConfig cloudConfig =
            new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
        zkController =
            new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig, () -> null);
        zkControllerRef.set(zkController);

        zkController
            .getZkClient()
            .makePath(
                DocCollection.getCollectionPathRoot(collectionName),
                new byte[0],
                CreateMode.PERSISTENT,
                true);

        ZkNodeProps m =
            new ZkNodeProps(
                Overseer.QUEUE_OPERATION,
                CollectionParams.CollectionAction.CREATE.toLower(),
                ZkStateReader.NODE_NAME_PROP,
                nodeName,
                ZkStateReader.NUM_SHARDS_PROP,
                "1",
                "name",
                collectionName);
        if (zkController.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
          zkController
              .getDistributedClusterStateUpdater()
              .doSingleStateUpdate(
                  DistributedClusterStateUpdater.MutatingCommand.ClusterCreateCollection,
                  m,
                  zkController.getSolrCloudManager(),
                  zkController.getZkStateReader());
        } else {
          zkController.getOverseerJobQueue().offer(Utils.toJSON(m));
        }

        MapWriter propMap =
            ew ->
                ew.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower())
                    .put(COLLECTION_PROP, collectionName)
                    .put(SHARD_ID_PROP, "shard1")
                    .put(ZkStateReader.NODE_NAME_PROP, "non_existent_host:1_")
                    .put(ZkStateReader.CORE_NAME_PROP, collectionName)
                    .put(ZkStateReader.STATE_PROP, "active");

        if (zkController.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
          zkController
              .getDistributedClusterStateUpdater()
              .doSingleStateUpdate(
                  DistributedClusterStateUpdater.MutatingCommand.SliceAddReplica,
                  new ZkNodeProps(propMap),
                  zkController.getSolrCloudManager(),
                  zkController.getZkStateReader());
        } else {
          zkController.getOverseerJobQueue().offer(propMap);
        }

        propMap =
            ew ->
                ew.put(Overseer.QUEUE_OPERATION, ADDREPLICA.toLower())
                    .put(COLLECTION_PROP, collectionName)
                    .put(SHARD_ID_PROP, "shard1")
                    .put(ZkStateReader.NODE_NAME_PROP, "non_existent_host:2_")
                    .put(ZkStateReader.CORE_NAME_PROP, collectionName)
                    .put(ZkStateReader.STATE_PROP, "down");
        if (zkController.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
          zkController
              .getDistributedClusterStateUpdater()
              .doSingleStateUpdate(
                  DistributedClusterStateUpdater.MutatingCommand.SliceAddReplica,
                  new ZkNodeProps(propMap),
                  zkController.getSolrCloudManager(),
                  zkController.getZkStateReader());
        } else {
          zkController.getOverseerJobQueue().offer(propMap);
        }

        zkController.getZkStateReader().forciblyRefreshAllClusterStateSlow();

        long now = System.nanoTime();
        long timeout = now + TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);
        zkController.publishAndWaitForDownStates(5);
        assertTrue(
            "The ZkController.publishAndWaitForDownStates should have timed out but it didn't",
            System.nanoTime() >= timeout);
      } finally {
        if (zkController != null) zkController.close();
        cc.shutdown();
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testTouchConfDir() throws Exception {
    Path zkDir = createTempDir("zkData");
    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(server.getZkAddress())
              .withTimeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .build()) {
        CoreContainer cc = getCoreContainer();
        try {
          CloudConfig cloudConfig =
              new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
          try (ZkController zkController =
              new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig, () -> null)) {
            final Path dir = createTempDir();
            final String configsetName = "testconfigset";
            try (ZkSolrResourceLoader loader =
                new ZkSolrResourceLoader(dir, configsetName, null, zkController)) {
              String zkpath = "/configs/" + configsetName;

              // touchConfDir doesn't make the znode
              Stat s = new Stat();
              assertFalse(zkClient.exists(zkpath, true));
              zkClient.makePath(zkpath, true);
              assertTrue(zkClient.exists(zkpath, true));
              assertNull(zkClient.getData(zkpath, null, s, true));
              assertEquals(0, s.getVersion());

              // touchConfDir should only set the data to new byte[] {0}
              ZkController.touchConfDir(loader);
              assertTrue(zkClient.exists(zkpath, true));
              assertArrayEquals(
                  ZkController.TOUCHED_ZNODE_DATA, zkClient.getData(zkpath, null, s, true));
              assertEquals(1, s.getVersion());

              // set new data to check if touchConfDir overwrites later
              byte[] data = "{\"key\", \"new data\"".getBytes(StandardCharsets.UTF_8);
              s = zkClient.setData(zkpath, data, true);
              assertEquals(2, s.getVersion());

              // make sure touchConfDir doesn't overwrite existing data.
              // touchConfDir should update version.
              assertTrue(zkClient.exists(zkpath, true));
              ZkController.touchConfDir(loader);
              assertTrue(zkClient.exists(zkpath, true));
              assertArrayEquals(data, zkClient.getData(zkpath, null, s, true));
              assertEquals(3, s.getVersion());
            }
          }
        } finally {
          cc.shutdown();
        }
      }
    } finally {
      server.shutdown();
    }
  }

  public void testCheckNoOldClusterstate() throws Exception {
    Path zkDir = createTempDir("testCheckNoOldClusterstate");
    ZkTestServer server = new ZkTestServer(zkDir);
    CoreContainer cc = getCoreContainer();
    int nThreads = 5;
    final ZkController[] controllers = new ZkController[nThreads];
    ExecutorService svc =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            nThreads, new SolrNamedThreadFactory("testCheckNoOldClusterstate-"));
    try {
      server.run();
      server
          .getZkClient()
          .create(
              "/clusterstate.json",
              "{}".getBytes(StandardCharsets.UTF_8),
              CreateMode.PERSISTENT,
              true);
      AtomicInteger idx = new AtomicInteger();
      CountDownLatch latch = new CountDownLatch(nThreads);
      CountDownLatch done = new CountDownLatch(nThreads);
      AtomicReference<Exception> exception = new AtomicReference<>();
      for (int i = 0; i < nThreads; i++) {
        svc.submit(
            () -> {
              int index = idx.getAndIncrement();
              latch.countDown();
              try {
                latch.await();
                controllers[index] =
                    new ZkController(
                        cc,
                        server.getZkAddress(),
                        TIMEOUT,
                        new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983 + index, "solr")
                            .build(),
                        () -> null);
              } catch (Exception e) {
                exception.compareAndSet(null, e);
              } finally {
                done.countDown();
              }
            });
      }
      done.await();
      assertFalse(server.getZkClient().exists("/clusterstate.json", true));
      assertNull(exception.get());
    } finally {
      ExecutorUtil.shutdownNowAndAwaitTermination(svc);
      for (ZkController controller : controllers) {
        if (controller != null) {
          controller.close();
        }
      }
      server.getZkClient().close();
      cc.shutdown();
      server.shutdown();
    }
  }

  private CoreContainer getCoreContainer() {
    return new MockCoreContainer();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private static class MockCoreContainer extends CoreContainer {
    UpdateShardHandler updateShardHandler =
        new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);

    public MockCoreContainer() {
      super(SolrXmlConfig.fromString(TEST_PATH(), "<solr/>"));
      HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
      httpShardHandlerFactory.init(new PluginInfo("shardHandlerFactory", Collections.emptyMap()));
      this.shardHandlerFactory = httpShardHandlerFactory;
      this.coreAdminHandler = new CoreAdminHandler();
      this.metricManager = mock(SolrMetricManager.class);
    }

    @Override
    public void load() {}

    @Override
    public UpdateShardHandler getUpdateShardHandler() {
      return updateShardHandler;
    }

    @Override
    public void shutdown() {
      updateShardHandler.close();
      super.shutdown();
    }

    @Override
    public SolrMetricManager getMetricManager() {
      return metricManager;
    }
  }
}
