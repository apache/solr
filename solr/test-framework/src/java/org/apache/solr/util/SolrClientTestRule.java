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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrBackend;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.junit.rules.ExternalResource;

/**
 * Provides access to a {@link SolrClient} instance and a running Solr in tests. Implementations
 * could run Solr in different ways (e.g. strictly embedded, adding HTTP/Jetty, adding SolrCloud, or
 * an external process). It's a JUnit {@link ExternalResource} (a {@code TestRule}), and thus closes
 * the client and Solr itself when the test completes. Wraps a {@link SolrBackend}. This test
 * utility is encouraged to be used by external projects that wish to test communicating with Solr,
 * especially for plugin providers.
 */
public abstract class SolrClientTestRule extends ExternalResource {

  protected SolrBackend backend;

  /** Returns the underlying {@link SolrBackend}. */
  public SolrBackend getBackend() {
    return backend;
  }

  @Override
  protected void after() {
    if (backend != null) {
      backend.close();
      backend = null;
    }
  }

  /** Starts the Solr server with empty solrHome. */
  public void startSolr() {
    startSolr(LuceneTestCase.createTempDir("solrhome"));
  }

  /**
   * Starts the Solr server with the given solrHome. If solrHome contains a solr.xml file, it is
   * used. Otherwise, a default testing configuration is used.
   */
  public abstract void startSolr(Path solrHome);

  /**
   * Returns a builder for creating a collection/core. Not for testing collection creation itself.
   */
  public NewCollectionBuilder newCollection(String name) {
    return new NewCollectionBuilder(name);
  }

  /**
   * Returns a builder for creating a collection/core. Not for testing collection creation itself.
   */
  public NewCollectionBuilder newCollection() {
    return new NewCollectionBuilder(DEFAULT_TEST_COLLECTION_NAME);
  }

  public class NewCollectionBuilder {
    private final String name;
    private String configSet;
    private Path configSetPath;
    private final Map<String, String> properties = new LinkedHashMap<>();

    public NewCollectionBuilder(String name) {
      this.name = name;
    }

    /** Chooses the configSet by name. */
    public NewCollectionBuilder withConfigSet(String configSet) {
      assert configSetPath == null;
      if (configSet.contains(FileSystems.getDefault().getSeparator())) {
        throw new IllegalArgumentException("wrong overload");
      }

      this.configSet = configSet;
      return this;
    }

    /** Choose the configSet path directly containing the files. */
    public NewCollectionBuilder withConfigSet(Path configSetPath) {
      // get configSet name & normalize path
      assert this.configSet == null;
      configSet = configSetPath.getFileName().toString();
      if (configSet.equals("conf")) {
        configSet = configSetPath.getParent().getFileName().toString();
      } else if (Files.exists(configSetPath.resolve("conf"))) {
        configSetPath = configSetPath.resolve("conf");
      }
      this.configSetPath = configSetPath;
      return this;
    }

    /** A {@code solrconfig.xml} alternative. */
    public NewCollectionBuilder withConfigFile(String configFile) {
      return withProperty(CoreDescriptor.CORE_CONFIG, configFile);
    }

    /** A {@code schema.xml} alternative. */
    public NewCollectionBuilder withSchemaFile(String schemaFile) {
      return withProperty(CoreDescriptor.CORE_SCHEMA, schemaFile);
    }

    public String getName() {
      return name;
    }

    public String getConfigSet() {
      return configSet;
    }

    public Path getConfigSetPath() {
      return configSetPath;
    }

    public NewCollectionBuilder withProperty(String key, String value) {
      properties.put(key, value);
      return this;
    }

    public Map<String, String> getProperties() {
      return properties;
    }

    public void create() throws SolrServerException, IOException {
      SolrClientTestRule.this.createColl(this);
    }
  }

  /**
   * Override to call either {@link #createCollSolrCloud(NewCollectionBuilder)} or {@link
   * #createCollStandalone(NewCollectionBuilder)}
   */
  protected abstract void createColl(NewCollectionBuilder b)
      throws SolrServerException, IOException;

  protected void createCollSolrCloud(NewCollectionBuilder b)
      throws SolrServerException, IOException {
    if (b.getConfigSetPath() != null) {
      if (!backend.hasConfigSet(b.getConfigSet())) {
        backend.uploadConfigSet(b.getConfigSetPath(), b.getConfigSet());
      }
    }

    var create = CollectionAdminRequest.createCollection(b.getName(), b.getConfigSet(), 1, 1);
    create.setProperties(b.getProperties());
    backend.createCollection(create);
  }

  protected void createCollStandalone(NewCollectionBuilder b)
      throws SolrServerException, IOException {
    // If there's a configSet path, we can reference it directly and not actually bother with
    // ConfigSetService
    String configSet = b.getConfigSet();
    Path configSetPath = b.getConfigSetPath();
    if (configSetPath != null) {
      // Chop off "/conf" if found.  Without this, managed resources might be written to
      // conf/conf/_schema_analysis_stopwords_english.json because SolrResourceLoader points to the
      // wrong dir.
      if (configSetPath.endsWith("conf")) {
        configSetPath = configSetPath.getParent();
      }
      configSet = configSetPath.toString();
    }

    var create = CollectionAdminRequest.createCollection(b.getName(), configSet, 1, 1);
    create.setProperties(b.getProperties());
    backend.createCollection(create);
  }

  /** Provides a SolrClient instance for administration actions. The caller must not close it. */
  public SolrClient getAdminClient() {
    return backend.getSolrClient();
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

  /**
   * @see SolrBackend#getCoreContainer()
   */
  public CoreContainer getCoreContainer() {
    return backend.getCoreContainer();
  }

  /**
   * @see SolrBackend#getBaseUrl(Random)
   */
  public String getBaseUrl(Random r) {
    return backend.getBaseUrl(r);
  }
}
