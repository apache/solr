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

import java.nio.file.Path;
import java.util.Collections;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.*;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
public class ZkControllerTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 10000;

  @BeforeClass
  public static void beforeZkControllerTest() throws Exception {

  }

  @AfterClass
  public static void afterZkControllerTest() throws Exception {

  }

  public void testNodeNameUrlConversion() throws Exception {

    // nodeName from parts
    assertEquals("localhost:8888_solr",
            ZkController.generateNodeName("localhost", "8888", "solr"));
    assertEquals("localhost:8888_solr",
            ZkController.generateNodeName("localhost", "8888", "/solr"));
    assertEquals("localhost:8888_solr",
            ZkController.generateNodeName("localhost", "8888", "/solr/"));
    // root context
    assertEquals("localhost:8888_",
            ZkController.generateNodeName("localhost", "8888", ""));
    assertEquals("localhost:8888_",
            ZkController.generateNodeName("localhost", "8888", "/"));
    // subdir
    assertEquals("foo-bar:77_solr%2Fsub_dir",
            ZkController.generateNodeName("foo-bar", "77", "solr/sub_dir"));
    assertEquals("foo-bar:77_solr%2Fsub_dir",
            ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir"));
    assertEquals("foo-bar:77_solr%2Fsub_dir",
            ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir/"));

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    Path zkDir = SolrTestUtil.createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run(true);

      SolrZkClient client = server.getZkClient();
      client.mkdir("/cluster");
      ZkController.createClusterZkNodes(client);

      try (ZkStateReader zkStateReader = new ZkStateReader(client)) {
        zkStateReader.createClusterStateWatchersAndUpdate();

        // getBaseUrlForNodeName
        assertEquals("http://zzz.xxx:1234/solr",
                zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));
        assertEquals("http://xxx:99",
                zkStateReader.getBaseUrlForNodeName("xxx:99_"));
        assertEquals("http://foo-bar.baz.org:9999/some_dir",
                zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_some_dir"));
        assertEquals("http://foo-bar.baz.org:9999/solr/sub_dir",
                zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_solr%2Fsub_dir"));

        // generateNodeName + getBaseUrlForNodeName
        assertEquals("http://foo:9876/solr",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo", "9876", "solr")));
        assertEquals("http://foo:9876/solr",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo", "9876", "/solr")));
        assertEquals("http://foo:9876/solr",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo", "9876", "/solr/")));
        assertEquals("http://foo.bar.com:9876/solr/sub_dir",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo.bar.com", "9876", "solr/sub_dir")));
        assertEquals("http://foo.bar.com:9876/solr/sub_dir",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo.bar.com", "9876", "/solr/sub_dir/")));
        assertEquals("http://foo-bar:9876",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo-bar", "9876", "")));
        assertEquals("http://foo-bar:9876",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo-bar", "9876", "/")));
        assertEquals("http://foo-bar.com:80/some_dir",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo-bar.com", "80", "some_dir")));
        assertEquals("http://foo-bar.com:80/some_dir",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));

      }

      ClusterProperties cp = new ClusterProperties(client);
      cp.setClusterProperty("urlScheme", "https");

      //Verify the URL Scheme is taken into account

      try (ZkStateReader zkStateReader = new ZkStateReader(client)) {

        zkStateReader.createClusterStateWatchersAndUpdate();

        assertEquals("https://zzz.xxx:1234/solr",
                zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));

        assertEquals("https://foo-bar.com:80/some_dir",
                zkStateReader.getBaseUrlForNodeName
                        (ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));

      }

    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testReadConfigName() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    CoreContainer cc = new MockCoreContainer();
    try {
      server.run();

      SolrZkClient zkClient = server.getZkClient();
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkConfigManager.CONFIGS_ZKNODE, false, false);
      zkClient.makePath(ZkConfigManager.CONFIGS_ZKNODE + "/" + actualConfigName, false, false);

      Object2ObjectMap<String,Object> props = new Object2ObjectLinkedOpenHashMap<>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/"
              + COLLECTION_NAME, Utils.toJSON(zkProps),
          CreateMode.PERSISTENT, true);

      CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
      ZkController zkController = new ZkController(cc, zkClient, cloudConfig);
      zkController.start();
      try {
        String configName = zkController.getZkStateReader().readConfigName(COLLECTION_NAME);
        assertEquals(configName, actualConfigName);
      } finally {
        zkController.disconnect(false);
        zkController.close();
      }
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }

  }

  public void testGetHostName() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zkData");

    ZkTestServer server = new ZkTestServer(zkDir);
    CoreContainer cc = new MockCoreContainer();
    try {
      server.run();

      ZkController zkController = null;

      try {
        CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
        zkController = new ZkController(cc, server.getZkClient(), cloudConfig);
      } catch (IllegalArgumentException e) {
        fail("ZkController did not normalize host name correctly");
      } finally {
        if (zkController != null)
          zkController.close();
      }
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }
  }

  // Removed test: testPublishAndWaitForDownStates.
  //
  // Its sole purpose (per its own javadoc) was to verify the SOLR-6665 behavior that
  // distinguished a replica's *core name* from its *coreNodeName*: publishAndWaitForDownStates
  // was expected to time out when matching on coreNodeName but return immediately when matching
  // on core name alone. This fork eliminated that distinction -- a replica's name IS its core
  // name (Replica.getName(), documented "Also known as coreNodeName"; CloudDescriptor no longer
  // exposes setCoreNodeName/getCoreNodeName). There is no longer a publishAndWaitForDownStates
  // method at all; ZkController.publishDownStates() just publishes a DOWNNODE state update with
  // no core-name check and no waiting/timeout, so the test's central assertion can never hold.
  // The test also drove the classic overseer job queue directly
  // (getOverseerJobQueue().offer(CREATE/ADDREPLICA ...)), which this fork's StateUpdates-based
  // overseer replaced. The behavior under test was deliberately removed, so the test is obsolete.

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private static class MockCoreContainer extends CoreContainer {
    HttpShardHandlerFactory shardHandlerFactory;
    UpdateShardHandler updateShardHandler;
    public MockCoreContainer() {
      super(new SolrXmlConfig().fromString(SolrTestUtil.TEST_PATH(), "<solr/>"));
      HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
      httpShardHandlerFactory.init(new PluginInfo("shardHandlerFactory", Collections.emptyMap()));
      shardHandlerFactory = httpShardHandlerFactory;
      this.coreAdminHandler = new CoreAdminHandler();
      updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
    }

    @Override
    public void load() {
    }

    @Override
    public UpdateShardHandler getUpdateShardHandler() {
      return updateShardHandler;
    }

    @Override
    public void shutdown() {
      shardHandlerFactory.close();
      updateShardHandler.close();
      super.shutdown();
    }
  }    
}
