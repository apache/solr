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
package org.apache.solr.util;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer.RequestWriterSupplier;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private EmbeddedSolrServer client = null;

  public EmbeddedSolrServerTestRule() {}

  public class Builder {
    private Path solrHome;
    private String schemaFile = "schema.xml";
    private String configFile = "solrconfig.xml";
    private String collectionName = "collection1";
    private RequestWriterSupplier requestWriterSupplier;

    public Builder setSolrHome(Path solrHome) {
      this.solrHome = solrHome;
      return this;
    }

    public Builder setSchemaFile(String schemaFile) {
      this.schemaFile = schemaFile;
      return this;
    }

    public Builder setConfigFile(String configFile) {
      this.configFile = configFile;
      return this;
    }

    public Builder setCollectionName(String collectionName) {
      this.collectionName = collectionName;
      return this;
    }

    public Builder setRequestWriterSupplier(RequestWriterSupplier requestWriterSupplier) {
      this.requestWriterSupplier = requestWriterSupplier;
      return this;
    }

    public Path getSolrHome() {
      return solrHome;
    }

    public String getSchemaFile() {
      return schemaFile;
    }

    public String getConfigFile() {
      return configFile;
    }

    public String getCollectionName() {
      return collectionName;
    }

    public RequestWriterSupplier getRequestWriterSupplier() {
      return requestWriterSupplier;
    }

    public void init() {

      EmbeddedSolrServerTestRule.this.init(this);
    }
  }

  private void init(Builder b) {
    Path solrHome = b.getSolrHome();
    String schemaFile = b.getSchemaFile();
    String configFile = b.getConfigFile();
    String collectionName = b.getCollectionName();

    if (b.getRequestWriterSupplier() != null) {
      SolrConfig solrConfig;

      try {
        solrConfig = new SolrConfig(solrHome.resolve(collectionName), configFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      NodeConfig nodeConfig = buildTestNodeConfig(solrHome);

      TestCoresLocator testCoreLocator =
          new TestCoresLocator(
              collectionName,
              LuceneTestCase.createTempDir("data-dir").toFile().getAbsolutePath(),
              solrConfig.getResourceName(),
              IndexSchemaFactory.buildIndexSchema(schemaFile, solrConfig).getResourceName());

      CoreContainer container = new CoreContainer(nodeConfig, testCoreLocator);
      container.load();
      client = new EmbeddedSolrServer(container, collectionName, b.getRequestWriterSupplier());
    } else {
      SolrConfig solrConfig;

      try {
        solrConfig = new SolrConfig(solrHome.resolve(collectionName), configFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      NodeConfig nodeConfig = buildTestNodeConfig(solrHome);

      TestCoresLocator testCoreLocator =
          new TestCoresLocator(
              collectionName,
              LuceneTestCase.createTempDir("data-dir").toFile().getAbsolutePath(),
              solrConfig.getResourceName(),
              IndexSchemaFactory.buildIndexSchema(schemaFile, solrConfig).getResourceName());

      CoreContainer container = new CoreContainer(nodeConfig, testCoreLocator);
      container.load();
      client = new EmbeddedSolrServer(container, collectionName);
    }
  }

  public Builder build() {
    return new Builder();
  }

  @Override
  protected void before() throws Throwable {
    super.before();
  }

  @Override
  protected void after() {

    try {

      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    client.getCoreContainer().shutdown();
  }

  @Override
  public EmbeddedSolrServer getSolrClient() {
    assert (client != null);
    return client;
  }

  @Override
  public Path getSolrHome() {
    return Path.of(client.getCoreContainer().getSolrHome());
  }

  // From TestHarness
  private static class TestCoresLocator extends ReadOnlyCoresLocator {

    final String coreName;
    final String dataDir;
    final String solrConfig;
    final String schema;

    public TestCoresLocator(String coreName, String dataDir, String solrConfig, String schema) {
      this.coreName = coreName == null ? SolrTestCaseJ4.DEFAULT_TEST_CORENAME : coreName;
      this.dataDir = dataDir;
      this.schema = schema;
      this.solrConfig = solrConfig;
    }

    @Override
    public List<CoreDescriptor> discover(CoreContainer cc) {
      return ImmutableList.of(
          new CoreDescriptor(
              coreName,
              cc.getCoreRootDirectory().resolve(coreName),
              cc,
              CoreDescriptor.CORE_DATADIR,
              dataDir,
              CoreDescriptor.CORE_CONFIG,
              solrConfig,
              CoreDescriptor.CORE_SCHEMA,
              schema,
              CoreDescriptor.CORE_COLLECTION,
              System.getProperty("collection", "collection1"),
              CoreDescriptor.CORE_SHARD,
              System.getProperty("shard", "shard1")));
    }
  }

  // From TestHarness
  private NodeConfig buildTestNodeConfig(Path solrHome) {
    CloudConfig cloudConfig =
        new CloudConfig.CloudConfigBuilder(
                System.getProperty("host"),
                Integer.getInteger("hostPort", 8983),
                System.getProperty("hostContext", ""))
            .setZkClientTimeout(Integer.getInteger("zkClientTimeout", 30000))
            .setZkHost(System.getProperty("zkHost"))
            .build();
    UpdateShardHandlerConfig updateShardHandlerConfig =
        new UpdateShardHandlerConfig(
            HttpClientUtil.DEFAULT_MAXCONNECTIONS,
            HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
            30000,
            30000,
            UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY,
            UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS);
    // universal default metric reporter
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("name", "default");
    attributes.put("class", SolrJmxReporter.class.getName());
    PluginInfo defaultPlugin = new PluginInfo("reporter", attributes);
    MetricsConfig metricsConfig =
        new MetricsConfig.MetricsConfigBuilder()
            .setMetricReporterPlugins(new PluginInfo[] {defaultPlugin})
            .build();

    return new NodeConfig.NodeConfigBuilder("testNode", solrHome)
        .setUseSchemaCache(Boolean.getBoolean("shareSchema"))
        .setCloudConfig(cloudConfig)
        .setUpdateShardHandlerConfig(updateShardHandlerConfig)
        .setMetricsConfig(metricsConfig)
        .build();
  }
}
