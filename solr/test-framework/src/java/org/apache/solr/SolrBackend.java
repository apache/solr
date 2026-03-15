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
package org.apache.solr;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Random;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.MetricsRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.embedded.EmbeddedSolrBackend;
import org.apache.solr.embedded.JettySolrRunner;

/**
 * Abstraction over a running Solr deployment for use in tests and benchmarks. The abstraction
 * normalizes how to shut down and perform other common operations as the methods indicate.
 */
public interface SolrBackend extends AutoCloseable {

  /**
   * Creates a new {@link SolrClient} defaulted to the given collection. The <em>caller</em> owns
   * this client and is responsible for closing it. Callers that want a long-lived client should
   * cache it themselves.
   */
  SolrClient newSolrClient(String collection);

  /**
   * Returns the node / admin (collection-less) {@link SolrClient} owned by this backend. The caller
   * must NOT close it; it is released when this backend is {@link #close()}d. A {@link
   * CloudSolrClient} should be returned if appropriate. While it *can* be used to target specific
   * collections, please use {@link #newSolrClient(String)} for that instead.
   */
  SolrClient getSolrClient();

  /**
   * Upload a configSet, possibly overwriting (creating files, updating files, NOT deleting files).
   *
   * @param configDir a path to the config set to upload
   * @param name the name to give the configSet
   */
  default void uploadConfigSet(Path configDir, String name)
      throws SolrServerException, IOException {
    getCoreContainer().getConfigSetService().uploadConfig(name, configDir);
  }

  /**
   * Checks if a configSet with the given name exists.
   *
   * @param name configSet name to check
   * @return true if the configSet exists, false otherwise
   */
  default boolean hasConfigSet(String name) throws SolrServerException, IOException {
    return getCoreContainer().getConfigSetService().checkConfigExists(name);
  }

  /**
   * Creates a collection (or core for single-node backends). Cloud backends honour {@code
   * numShards} and {@code replicationFactor}; single-node backends ({@link JettySolrRunner}, {@link
   * EmbeddedSolrBackend}) ignore those fields. Callers should use {@link #hasCollection(String)}
   * first if they want to avoid errors when the collection already exists. Tests/benchmarks that
   * want to test how this works should not use this to do so.
   */
  void createCollection(CollectionAdminRequest.Create create)
      throws SolrServerException, IOException;

  /**
   * Checks if a collection or core with the given name exists.
   *
   * @param name collection or core name to check
   * @return true if the collection/core exists, false otherwise
   */
  boolean hasCollection(String name) throws SolrServerException, IOException;

  /** Reloads a collection or core by this name. The purpose is typically to clear caches. */
  default void reloadCollection(String name) throws SolrServerException, IOException {
    getSolrClient().request(CollectionAdminRequest.reloadCollection(name));
  }

  /**
   * Provides access to an embedded/in-process {@link org.apache.solr.core.CoreContainer} -- if
   * available (else null). If there is more than one node, then one is returned.
   *
   * @return can be null.
   */
  CoreContainer getCoreContainer();

  /**
   * Returns the base URL of a Solr node. For cloud backends with multiple nodes, a live node is
   * chosen at random. The URL does not include a trailing slash.
   *
   * @return base URL (e.g., "http://localhost:8983/solr"). Null for EmbeddedSolrServer.
   */
  default String getBaseUrl(Random r) {
    // Get live nodes and pick one randomly
    SolrClient adminClient = getSolrClient();
    if (adminClient instanceof HttpSolrClientBase httpSolrClient) {
      return httpSolrClient.getBaseURL();
    } else if (adminClient instanceof CloudSolrClient cloudClient) {
      var liveNodes = cloudClient.getClusterStateProvider().getLiveNodes();
      if (liveNodes.isEmpty()) {
        return null;
      }
      String randomNode =
          liveNodes.stream().skip(r.nextInt(liveNodes.size())).findFirst().orElseThrow();
      String urlScheme = cloudClient.getClusterStateProvider().getUrlScheme();
      return URLUtil.getBaseUrlForNodeName(randomNode, urlScheme);
    } else {
      return null;
    }
  }

  /** Dumps Prometheus-format metrics to {@code out}. No-op is an acceptable implementation. */
  default void dumpMetrics(PrintStream out) {
    try {
      var request = new MetricsRequest();
      request.setResponseParser(new InputStreamResponseParser("prometheus"));
      var response = request.process(getSolrClient());
      out.println(InputStreamResponseParser.consumeResponseToString(response.getResponse()));
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Dumps JSON-format information for all cores to {@code out}. No-op is an acceptable
   * implementation. Default uses {@code GET /admin/cores?indexInfo=true}.
   */
  default void dumpCoreInfo(PrintStream out) {
    try {
      var request =
          new GenericSolrRequest(
              SolrRequest.METHOD.GET, "/admin/cores", SolrParams.of("indexInfo", "true"));
      request.setResponseParser(new InputStreamResponseParser("json"));
      var response = request.process(getSolrClient());
      out.println(InputStreamResponseParser.consumeResponseToString(response.getResponse()));
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Shuts down this backend, releases the admin client, and all other resources. */
  @Override
  void close();
}
