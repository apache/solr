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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
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

  public abstract SolrClient getSolrClient();

  public abstract SolrClient getSolrClient(String name);

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
      SolrClientTestRule.this.create(this);
    }
  }

  protected abstract void create(NewCollectionBuilder b) throws SolrServerException, IOException;

  public void clearIndex() throws SolrServerException, IOException {
    new UpdateRequest().deleteByQuery("*:*").commit(getSolrClient(), null);
  }
}
