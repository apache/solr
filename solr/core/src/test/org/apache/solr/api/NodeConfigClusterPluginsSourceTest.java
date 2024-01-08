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

package org.apache.solr.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests that verify initialization of container plugins that are declared in solr.xml */
public class NodeConfigClusterPluginsSourceTest extends SolrCloudTestCase {

  // Any random value for the config parameter
  private static int CFG_VAL;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(NodeConfig.CONFIG_EDITING_DISABLED_ARG, "true");
    CFG_VAL = random().nextInt();
    configureCluster(1)
        .withSolrXml(
            MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
                "</solr>",
                SingletonNoConfig.configXml()
                    + SingletonWithConfig.configXml(new SingletonConfig(CFG_VAL))
                    + "</solr>"))
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  /**
   * Verifies that the cluster singleton configs declared in solr.xml are loaded into the registry
   */
  public void testClusterSingletonsRegistered() {

    CoreContainer cc = cluster.getJettySolrRunners().get(0).getCoreContainer();
    assertEquals(
        "expected 2 plugins to be installed to the container plugins registry",
        2,
        cc.getNodeConfig().getClusterSingletonPlugins().length);

    ContainerPluginsRegistry registry = cc.getContainerPluginsRegistry();
    registry.refresh();

    // Verify config for SingletonNoConfig
    ContainerPluginsRegistry.ApiInfo apiInfo = registry.getPlugin(SingletonNoConfig.NAME);
    assertNotNull("no plugin found in registry for " + SingletonNoConfig.NAME, apiInfo);
    assertEquals("incorrect plugin name", SingletonNoConfig.NAME, apiInfo.getInfo().name);
    assertEquals(
        "incorrect plugin class name", SingletonNoConfig.class.getName(), apiInfo.getInfo().klass);
    assertNull(
        "config should not be set because none was specified in solr.xml",
        apiInfo.getInfo().config);

    // Verify config for SingletonWithConfig
    apiInfo = registry.getPlugin(SingletonWithConfig.NAME);
    assertNotNull("no plugin found in registry for " + SingletonWithConfig.NAME, apiInfo);
    assertEquals("incorrect plugin name", SingletonWithConfig.NAME, apiInfo.getInfo().name);
    assertEquals(
        "incorrect plugin class name",
        SingletonWithConfig.class.getName(),
        apiInfo.getInfo().klass);
    MapWriter config = apiInfo.getInfo().config;
    assertNotNull("config should be set for " + SingletonWithConfig.NAME, config);
    Map<String, Object> configMap = new HashMap<>();
    config.toMap(configMap);
    assertEquals("incorrect config val for cfgInt parameter", CFG_VAL, configMap.get("cfgInt"));
  }

  /**
   * Verify that the container plugins Read Api is available and works with plugin configs declared
   * in solr.xml
   */
  @Test
  public void testClusterPluginsReadApi() throws Exception {
    V2Response rsp =
        new V2Request.Builder("/cluster/plugin").GET().build().process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    assertEquals(
        SingletonNoConfig.class.getName(),
        rsp._getStr("/plugin/" + SingletonNoConfig.NAME + "/class", null));

    assertEquals(
        SingletonWithConfig.class.getName(),
        rsp._getStr("/plugin/" + SingletonWithConfig.NAME + "/class", null));
  }

  /** Verify that the Edit Apis are not available for plugins declared in solr.xml */
  @Test
  public void testClusterPluginsEditApi() throws Exception {
    PluginMeta meta = SingletonNoConfig.pluginMeta();
    V2Request req =
        new V2Request.Builder("/cluster/plugin")
            .POST()
            .withPayload(Collections.singletonMap("add", meta))
            .build();
    try {
      req.process(cluster.getSolrClient());
      fail("Expected a 404 response code because the Edit Apis are not registered");
    } catch (BaseHttpSolrClient.RemoteExecutionException e) {
      assertEquals(
          "Expected a HTTP 404 response code because the /cluster/plugin API should not be registered",
          404,
          e.code());
    }
  }

  public static class SingletonNoConfig implements ClusterSingleton {

    static final String NAME = "singleton-no-config";

    static String configXml() {
      return "<clusterSingleton name=\""
          + NAME
          + "\" class=\""
          + SingletonNoConfig.class.getName()
          + "\"/>";
    }

    static PluginMeta pluginMeta() {
      PluginMeta plugin = new PluginMeta();
      plugin.name = SingletonNoConfig.NAME;
      plugin.klass = SingletonNoConfig.class.getName();
      return plugin;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public State getState() {
      return State.STOPPED;
    }

    @Override
    public void stop() {}
  }

  public static class SingletonConfig implements ReflectMapWriter {

    @JsonProperty public int cfgInt;

    public SingletonConfig() {
      this(-1);
    }

    public SingletonConfig(int cfgInt) {
      this.cfgInt = cfgInt;
    }
  }

  public static class SingletonWithConfig
      implements ClusterSingleton, ConfigurablePlugin<SingletonConfig> {

    static final String NAME = "singleton-with-config";

    static String configXml(SingletonConfig config) {
      return "<clusterSingleton name=\""
          + NAME
          + "\" class=\""
          + SingletonWithConfig.class.getName()
          + "\"><int name=\"cfgInt\">"
          + config.cfgInt
          + "</int></clusterSingleton>";
    }

    @Override
    public void configure(SingletonConfig cfg) {}

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public State getState() {
      return State.STOPPED;
    }

    @Override
    public void stop() {}
  }
}
