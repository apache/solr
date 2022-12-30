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

import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer.RequestWriterSupplier;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;

/** TODO NOCOMMIT document */
public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private EmbeddedSolrServer client = null;

  public Builder build() {
    return new Builder();
  }

  public class Builder {
    private Path solrHome; // mandatory
    private Path dataDir;
    private String collectionName = DEFAULT_TEST_COLLECTION_NAME;
    private String configFile;
    private String schemaFile;
    private RequestWriterSupplier requestWriterSupplier = RequestWriterSupplier.JavaBin;

    public Builder setSolrHome(Path solrHome) {
      this.solrHome = solrHome;
      return this;
    }

    public Builder useTempDataDir() {
      this.dataDir = LuceneTestCase.createTempDir("data-dir");
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

    public Path getDataDir() {
      return this.dataDir;
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

    NodeConfig nodeConfig = buildTestNodeConfig(b);

    var coreLocator =
        new ReadOnlyCoresLocator() {
          @Override
          public List<CoreDescriptor> discover(CoreContainer cc) {
            return List.of(buildCoreDesc(cc, b));
          }
        };

    CoreContainer container = new CoreContainer(nodeConfig, coreLocator);
    container.load();

    client = new EmbeddedSolrServer(container, b.getCollectionName(), b.getRequestWriterSupplier());
  }

  private NodeConfig buildTestNodeConfig(Builder b) {
    // TODO nocommit dedupe this with TestHarness
    var updateShardHandlerConfig =
        new UpdateShardHandlerConfig(
            HttpClientUtil.DEFAULT_MAXCONNECTIONS,
            HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
            30000,
            30000,
            UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY,
            UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS);
    return new NodeConfig.NodeConfigBuilder("testNode", b.getSolrHome())
        .setUpdateShardHandlerConfig(updateShardHandlerConfig)
        .build();
  }

  private CoreDescriptor buildCoreDesc(CoreContainer cc, Builder b) {
    Map<String, String> coreProps = new HashMap<>();
    if (b.configFile != null) {
      coreProps.put(CoreDescriptor.CORE_CONFIG, b.configFile);
    }
    if (b.schemaFile != null) {
      coreProps.put(CoreDescriptor.CORE_SCHEMA, b.schemaFile);
    }
    if (b.dataDir != null) {
      coreProps.put(CoreDescriptor.CORE_DATADIR, b.dataDir.toString());
    }

    var coreName = b.collectionName;
    var instanceDir = cc.getCoreRootDirectory().resolve(coreName);
    return new CoreDescriptor(
        coreName, instanceDir, coreProps, cc.getContainerProperties(), cc.getZkController());
  }

  @Override
  protected void after() {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      client.getCoreContainer().shutdown();
    }
    client = null; // not necessary but why not; maybe for GC
  }

  @Override
  public EmbeddedSolrServer getSolrClient() {
    assert client != null;
    return client;
  }

  @Override
  public Path getSolrHome() {
    return Path.of(client.getCoreContainer().getSolrHome());
  }
}
