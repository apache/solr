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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.rules.ExternalResource;

/**
 * Provides access to a {@link SolrClient} instance and a running Solr in tests. Implementations
 * could run Solr in different ways (e.g. strictly embedded, adding HTTP/Jetty, adding SolrCloud, or
 * an external process). It's a JUnit {@link ExternalResource} (a {@code TestRule}), and thus closes
 * the client and Solr itself when the test completes. This test utility is encouraged to be used by
 * external projects that wish to test communicating with Solr, especially for plugin providers.
 */
public abstract class SolrClientTestRule extends ExternalResource {

  /** Starts the Solr server with empty solrHome. */
  public void startSolr() {
    startSolr(LuceneTestCase.createTempDir("solrhome"));
  }

  /**
   * Starts the Solr server with the given solrHome. If solrHome contains a solr.xml file, it is
   * used. Otherwise a default testing configuration is used.
   */
  public abstract void startSolr(Path solrHome);

  public NewCollectionBuilder newCollection(String name) {
    return new NewCollectionBuilder(name);
  }

  public NewCollectionBuilder newCollection() {
    return new NewCollectionBuilder(DEFAULT_TEST_COLLECTION_NAME);
  }

  public class NewCollectionBuilder {
    private String name;
    private String configSet;
    private String configFile;
    private String schemaFile;
    private String basicAuthUser;
    private String basicAuthPwd;

    public NewCollectionBuilder(String name) {
      this.name = name;
    }

    public NewCollectionBuilder withConfigSet(String configSet) {
      // Chop off "/conf" if found -- configSet can be a path.
      // This is a hack so that we can continue to use ExternalPaths.DEFAULT_CONFIGSET etc. as-is.
      // Without this, managed resources might be written to
      // conf/conf/_schema_analysis_stopwords_english.json because SolrResourceLoader points to the
      // wrong dir.
      if (configSet != null) {
        final var confSuffix = FileSystems.getDefault().getSeparator() + "conf";
        if (configSet.endsWith(confSuffix)) {
          configSet = configSet.substring(0, configSet.length() - confSuffix.length());
        }
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

    public NewCollectionBuilder withBasicAuthCredentials(String user, String password) {
      this.basicAuthUser = user;
      this.basicAuthPwd = password;
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
      SolrClientTestRule.this.create(this);
    }

    public String getBasicAuthUser() {
      return basicAuthUser;
    }

    public String getBasicAuthPwd() {
      return basicAuthPwd;
    }
  }

  protected void create(NewCollectionBuilder b) throws SolrServerException, IOException {

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

    if (b.getBasicAuthUser() != null) {
      req.setBasicAuthCredentials(b.getBasicAuthUser(), b.getBasicAuthPwd());
    }

    req.process(getAdminClient());
  }

  /**
   * Provides a SolrClient instance for administration actions. The caller doesn't need to close it
   */
  public SolrClient getAdminClient() {
    return getSolrClient(null);
  }

  /** Provides a SolrClient instance for collection1. The caller doesn't need to close it */
  public SolrClient getSolrClient() {
    return getSolrClient("collection1");
  }

  /**
   * Provides a SolrClient instance for caller defined collection name. The caller doesn't need to
   * close it
   */
  public abstract SolrClient getSolrClient(String collection);

  public void clearIndex() throws SolrServerException, IOException {
    new UpdateRequest().deleteByQuery("*:*").commit(getSolrClient(), null);
  }
}
