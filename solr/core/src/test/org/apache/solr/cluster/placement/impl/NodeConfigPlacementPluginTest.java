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

package org.apache.solr.cluster.placement.impl;

import static org.hamcrest.Matchers.instanceOf;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.apache.solr.core.CoreContainer;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeConfigPlacementPluginTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty(ContainerPluginsRegistry.CLUSTER_PLUGIN_EDIT_ENABLED, "false");
    String pluginXml =
        "<replicaPlacementFactory class=\"org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory\"><int name=\"minimalFreeDiskGB\">10</int><int name=\"prioritizedFreeDiskGB\">200</int></replicaPlacementFactory>";

    configureCluster(1)
        .withSolrXml(
            MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace("</solr>", pluginXml) + "</solr>")
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testConfigurationInSolrXml() {
    CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();
    MatcherAssert.assertThat(
        cc.getPlacementPluginFactory().createPluginInstance(),
        instanceOf(AffinityPlacementFactory.AffinityPlacementPlugin.class));
    MatcherAssert.assertThat(
        cc.getPlacementPluginFactory().getConfig(), instanceOf(AffinityPlacementConfig.class));

    AffinityPlacementConfig config =
        (AffinityPlacementConfig) cc.getPlacementPluginFactory().getConfig();
    assertEquals(config.minimalFreeDiskGB, 10);
    assertEquals(config.prioritizedFreeDiskGB, 200);
    cc.shutdown();
  }
}
