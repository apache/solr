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

  public EmbeddedSolrServer getAdminClient() {
    return adminClient;
  }

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

  @Override
  protected void after() {
    if (container != null) container.shutdown();
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
