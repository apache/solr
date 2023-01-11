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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;

/** TODO NOCOMMIT document */
public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private EmbeddedSolrServer adminClient = null;
  private EmbeddedSolrServer client = null;

  private CoreContainer container = null;

  //  public Builder build() {
  //    return new Builder();
  //  }

  //  public class Builder {
  //    private Path solrHome; // mandatory
  //    private Path dataDir;
  //    private String coreRootDirectory;
  //    private String configSetBaseDir;
  //    private RequestWriterSupplier requestWriterSupplier = RequestWriterSupplier.JavaBin;
  //
  //    public Builder withSolrHome(Path solrHome) {
  //      this.solrHome = solrHome;
  //      return this;
  //    }
  //
  //    public Builder withTempDataDir() {
  //      this.dataDir = LuceneTestCase.createTempDir("data-dir");
  //      return this;
  //    }
  //
  //    public Builder withRequestWriterSupplier(RequestWriterSupplier requestWriterSupplier) {
  //      this.requestWriterSupplier = requestWriterSupplier;
  //      return this;
  //    }
  //
  //    public Builder withCoreRootDirectory(String coreRootDirectory) {
  //      this.coreRootDirectory = coreRootDirectory;
  //      return this;
  //    }
  //
  //    public Builder withConfigSetBaseDir(String configSetBaseDir) {
  //      this.configSetBaseDir = configSetBaseDir;
  //      return this;
  //    }
  //
  //    public Path getSolrHome() {
  //      return solrHome;
  //    }
  //
  //    public Path getDataDir() {
  //      return this.dataDir;
  //    }
  //
  //    public String getCoreRootDirectory() {
  //      return coreRootDirectory;
  //    }
  //
  //    public String getConfigSetBaseDir() {
  //      return configSetBaseDir;
  //    }
  //
  //    public RequestWriterSupplier getRequestWriterSupplier() {
  //      return requestWriterSupplier;
  //    }
  //
  //    public void init() {
  //      EmbeddedSolrServerTestRule.this.init(this);
  //    }
  //  }

  //  private void init(Builder b) {
  //
  //    NodeConfig nodeConfig = buildTestNodeConfig(b);
  //
  //    // TODO nocommit
  //    var coreLocator =
  //        new ReadOnlyCoresLocator() {
  //          @Override
  //          public List<CoreDescriptor> discover(CoreContainer cc) {
  //            return Collections.emptyList();
  //          }
  //        };
  //
  //    container = new CoreContainer(nodeConfig, coreLocator);
  //
  //    container.load();
  //
  //    adminClient = new EmbeddedSolrServer(container, null);
  //  }

  public void startSolr(Path solrHome) {
    NodeConfig nodeConfig = newNodeConfigBuilder(solrHome);
    startSolr(nodeConfig);
  }

  public void startSolr(NodeConfig nodeConfig) {
    container = new CoreContainer(nodeConfig);
    container.load();
    adminClient = new EmbeddedSolrServer(container, null);
  }

  public NodeConfig newNodeConfigBuilder(Path solrHome) {
    // TODO nocommit dedupe this with TestHarness
    var updateShardHandlerConfig =
        new UpdateShardHandlerConfig(
            HttpClientUtil.DEFAULT_MAXCONNECTIONS,
            HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
            30000,
            30000,
            UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY,
            UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS);

    return new NodeConfig.NodeConfigBuilder("testNode", solrHome)
        .setUpdateShardHandlerConfig(updateShardHandlerConfig)
        .setCoreRootDirectory(LuceneTestCase.createTempDir("cores").toString())
        .build();
  }

  public NewCollectionBuilder newCollection(String name) {
    return new NewCollectionBuilder(name);
  }

  public class NewCollectionBuilder {
    private String name;
    private String configSet;
    private String configFile;
    private String schemaFile;

    public NewCollectionBuilder(String name) {
      this.name = name;
    }

    public NewCollectionBuilder withConfigSet(String configSet) {
      this.configSet = configSet;
      return this;
    }

    public NewCollectionBuilder withConfigFile(String configFile) {
      this.configFile = configFile;
      return this;
    }

    public NewCollectionBuilder withSchemaFile(String schemaFile) {
      this.schemaFile = schemaFile;
      return this;
    }

    public String getName() {
      return name;
    }

    public String getConfigSet() {
      return configSet;
    }

    public String getConfigFile() {
      return configFile;
    }

    public String getSchemaFile() {
      return schemaFile;
    }

    public void create() throws SolrServerException, IOException {
      EmbeddedSolrServerTestRule.this.create(this);
    }
  }

  private void create(NewCollectionBuilder b) throws SolrServerException, IOException {

    client = new EmbeddedSolrServer(container, b.getName());

    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName(b.getName());
    req.setInstanceDir(b.getName());

    if (b.getConfigFile() != null) {
      req.setConfigName(b.getConfigFile());
    }
    if (b.getSchemaFile() != null) {
      req.setSchemaName(b.getSchemaFile());
    }
    if (b.getConfigSet() != null) {
      req.setConfigSet(b.getConfigSet());
    }

    req.process(client);
  }

  //  private NodeConfig buildTestNodeConfig(Builder b) {
  //    // TODO nocommit dedupe this with TestHarness
  //    var updateShardHandlerConfig =
  //        new UpdateShardHandlerConfig(
  //            HttpClientUtil.DEFAULT_MAXCONNECTIONS,
  //            HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST,
  //            30000,
  //            30000,
  //            UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY,
  //            UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS);
  //
  //    return new NodeConfig.NodeConfigBuilder("testNode", b.getSolrHome())
  //        .setUpdateShardHandlerConfig(updateShardHandlerConfig)
  //        .setCoreRootDirectory(b.getCoreRootDirectory())
  //        .setConfigSetBaseDirectory(b.getConfigSetBaseDir())
  //        .build();
  //  }

  //  private CoreDescriptor buildCoreDesc(CoreContainer cc, Builder b) {
  //    Map<String, String> coreProps = new HashMap<>();
  //    if (b.configFile != null) {
  //      coreProps.put(CoreDescriptor.CORE_CONFIG, b.configFile);
  //    }
  //    if (b.schemaFile != null) {
  //      coreProps.put(CoreDescriptor.CORE_SCHEMA, b.schemaFile);
  //    }
  //    if (b.dataDir != null) {
  //      coreProps.put(CoreDescriptor.CORE_DATADIR, b.dataDir.toString());
  //    }
  //
  //    var coreName = b.collectionName;
  //    var instanceDir = cc.getCoreRootDirectory().resolve(coreName);
  //    return new CoreDescriptor(
  //        coreName, instanceDir, coreProps, cc.getContainerProperties(), cc.getZkController());
  //  }

  @Override
  protected void after() {

    try {
      if (adminClient != null)
      adminClient.close();

      if (client != null)
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (container != null)
      container.shutdown();
    }
    client = null;// not necessary but why not; maybe for GC
    adminClient = null;
  }

  @Override
  public EmbeddedSolrServer getSolrClient() {
    if (client == null) {
      return adminClient;
    } else {
      return client;
    }
  }

  @Override
  public EmbeddedSolrServer getSolrClient(String name) {
    if (client == null) {
      return new EmbeddedSolrServer(adminClient.getCoreContainer(), name);
    } else {
      return new EmbeddedSolrServer(client.getCoreContainer(), name);
    }
  }
}
