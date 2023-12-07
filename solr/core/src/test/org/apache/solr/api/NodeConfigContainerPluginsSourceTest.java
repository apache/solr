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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;

import java.util.Arrays;
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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests that verify initialization of container plugins that are declared in solr.xml */
public class NodeConfigContainerPluginsSourceTest extends SolrCloudTestCase {

  private static final String NODE_CONFIG_PLUGINS_SOURCE_XML =
      "<containerPluginsSource class=\"org.apache.solr.api.NodeConfigContainerPluginsSource\"/>";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .withSolrXml(
            MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
                "</solr>",
                NODE_CONFIG_PLUGINS_SOURCE_XML + SingletonNoConfig.xmlConfig() + "</solr>"))
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  /** Verifies that a cluster singleton config declared in solr.xml is loaded into the registry */
  public void testOneClusterSingleton() {
    CoreContainer cc = newCoreContainer(solrXml(SingletonNoConfig.xmlConfig()));
    try {
      assertEquals(1, cc.getNodeConfig().getContainerPlugins().length);

      ContainerPluginsRegistry registry = cc.getContainerPluginsRegistry();
      registry.refresh();
      ContainerPluginsRegistry.ApiInfo apiInfo = registry.getPlugin(SingletonNoConfig.NAME);
      assertNotNull(apiInfo);
      assertEquals("incorrect plugin name", SingletonNoConfig.NAME, apiInfo.getInfo().name);
      assertEquals(
          "incorrect plugin class name",
          SingletonNoConfig.class.getName(),
          apiInfo.getInfo().klass);
      assertNull(
          "config should not be set because none was specified in solr.xml",
          apiInfo.getInfo().config);
    } finally {
      cc.shutdown();
    }
  }

  /**
   * Verifies that solr.xml allows the declaration of multiple cluster singleton confis, and that
   * they are all loaded to the registry
   */
  @Test
  public void testMultipleClusterSingletons() {
    final int cfgVal = random().nextInt();
    CoreContainer cc =
        newCoreContainer(
            solrXml(
                SingletonNoConfig.xmlConfig(),
                SingletonWithConfig.configXml(new SingletonConfig(cfgVal))));
    try {
      assertEquals(2, cc.getNodeConfig().getContainerPlugins().length);

      ContainerPluginsRegistry registry = cc.getContainerPluginsRegistry();
      registry.refresh();

      ContainerPluginsRegistry.ApiInfo apiInfo = registry.getPlugin(SingletonNoConfig.NAME);
      assertNotNull(apiInfo);
      assertEquals(SingletonNoConfig.NAME, apiInfo.getInfo().name);
      assertEquals(SingletonNoConfig.class.getName(), apiInfo.getInfo().klass);

      apiInfo = registry.getPlugin(SingletonWithConfig.NAME);
      assertNotNull(apiInfo);
      assertEquals(SingletonWithConfig.NAME, apiInfo.getInfo().name);
      assertEquals(SingletonWithConfig.class.getName(), apiInfo.getInfo().klass);
      MapWriter config = apiInfo.getInfo().config;
      Map<String, Object> configMap = new HashMap<>();
      config.toMap(configMap);
      assertEquals(cfgVal, configMap.get("cfgInt"));
    } finally {
      cc.shutdown();
    }
  }

  /**
   * Verify that the container plugins Read Api is available and works with plugin configs declared
   * in solr.xml
   */
  @Test
  @SuppressWarnings({"unchecked"})
  public void testContainerPluginsReadApi() throws Exception {
    V2Response rsp =
        new V2Request.Builder("/cluster/plugin").GET().build().process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
    assertEquals(
        SingletonNoConfig.class.getName(),
        rsp._getStr("/plugin/" + SingletonNoConfig.NAME + "/class", null));
  }

  /** Verify that the Edit Apis are not available for plugins declared in solr.xml */
  @Test
  public void testContainerPluginsEditApi() throws Exception {
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

  @Test
  public void testContainerPlugin() throws Exception {
    CoreContainer cc = newCoreContainer(solrXml(ContainerPlugin.xmlConfig()));
    ContainerPluginsRegistry registry = cc.getContainerPluginsRegistry();
    registry.refresh();

    ContainerPluginsRegistry.ApiInfo apiInfo = registry.getPlugin(ContainerPlugin.NAME);
    assertNotNull(apiInfo);
    assertEquals(ContainerPlugin.NAME, apiInfo.getInfo().name);
    assertEquals(ContainerPlugin.class.getName(), apiInfo.getInfo().klass);
  }

  @EndPoint(
      method = GET,
      path = "/plugin/my/plugin",
      permission = PermissionNameProvider.Name.COLL_READ_PERM)
  public static class ContainerPlugin {
    static final String NAME = "container.plugin";

    @Command
    public void read(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("testkey", "testval");
    }

    static String xmlConfig() {
      return "<containerPlugin name=\""
          + NAME
          + "\" class=\""
          + ContainerPlugin.class.getName()
          + "\"/>";
    }
  }

  public static class SingletonNoConfig implements ClusterSingleton {

    static final String NAME = ".singleton-no-config";

    static String xmlConfig() {
      return "<containerPlugin name=\""
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

    static final String NAME = ".singleton-with-config";

    static String configXml(SingletonConfig config) {
      return "<containerPlugin name=\""
          + NAME
          + "\" class=\""
          + SingletonWithConfig.class.getName()
          + "\"><int name=\"cfgInt\">"
          + config.cfgInt
          + "</int></containerPlugin>";
    }

    private int cfgInt;

    @Override
    public void configure(SingletonConfig cfg) {
      this.cfgInt = cfg.cfgInt;
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

  private static CoreContainer newCoreContainer(String solrXml) {
    return createCoreContainer(TEST_PATH(), solrXml);
  }

  private static String solrXml(String... xmlContent) {
    StringBuilder solrXml = new StringBuilder();
    solrXml.append("<solr>");
    solrXml.append(NODE_CONFIG_PLUGINS_SOURCE_XML);
    Arrays.stream(xmlContent).forEach(solrXml::append);
    return solrXml.append("</solr>").toString();
  }
}
