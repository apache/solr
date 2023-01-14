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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;

/** TODO NOCOMMIT document */
public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private static final String CORE_DIR_PROP = "coreRootDirectory";
  private EmbeddedSolrServer adminClient = null;
  private EmbeddedSolrServer client = null;
  private CoreContainer container = null;

  private boolean clearCoreDirSysProp = false;

  public EmbeddedSolrServer getAdminClient() {
    return adminClient;
  }

  public void startSolr(Path solrHome) {
    NodeConfig nodeConfig;
    if (Files.exists(solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE))) {
      // existing solr.xml; perhaps not recommended for new/most tests

      // solr.xml coreRootDirectory is best set to a temp directory in a test so that
      //  (a) we don't load existing cores
      //      Because it's better for tests to explicitly create cores.
      //  (b) we don't write data in the test to a likely template directory
      //  But a test can insist on something if it sets the property.
      if (System.getProperty(CORE_DIR_PROP) == null) {
        clearCoreDirSysProp = true;
        System.setProperty(CORE_DIR_PROP, LuceneTestCase.createTempDir("cores").toString());
      }

      nodeConfig = SolrXmlConfig.fromSolrHome(solrHome, null);
    } else {
      // test oriented config (preferred)
      nodeConfig = newNodeConfigBuilder(solrHome).build();
    }

    startSolr(nodeConfig);
  }

  public void startSolr(NodeConfig nodeConfig) {
    container = new CoreContainer(nodeConfig);
    container.load();
    adminClient = new EmbeddedSolrServer(container, null);
  }

  public NodeConfig.NodeConfigBuilder newNodeConfigBuilder(Path solrHome) {
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
        .setCoreRootDirectory(LuceneTestCase.createTempDir("cores").toString());
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
      // Chop off "/conf" if found -- configSet can be a path.
      // This is a hack so that we can continue to use ExternalPaths.DEFAULT_CONFIGSET etc. as-is.
      // TODO FileSystemConfigSetService.locateInstanceDir should have this logic.
      // Without this, managed resources might be written to
      // conf/conf/_schema_analysis_stopwords_english.json because SolrResourceLoader points to the
      // wrong dir.
      if (configSet != null && configSet.endsWith("/conf")) {
        configSet = configSet.substring(0, configSet.length() - "/conf".length());
      }

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

    if (b.getConfigSet() != null) {
      req.setConfigSet(b.getConfigSet());
    }
    if (b.getConfigFile() != null) {
      req.setConfigName(b.getConfigFile());
    }
    if (b.getSchemaFile() != null) {
      req.setSchemaName(b.getSchemaFile());
    }

    req.process(client);
  }

  @Override
  protected void after() {
    if (container != null) container.shutdown();

    if (clearCoreDirSysProp) {
      System.clearProperty(CORE_DIR_PROP);
    }
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
