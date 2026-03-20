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
package org.apache.solr.client.solrj.io;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import org.apache.solr.client.solrj.impl.HttpSolrClientBuilderBase;
import org.apache.solr.client.solrj.impl.SolrHttpConstants;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.URLUtil;

/** The SolrClientCache caches SolrClients, so they can be reused by different TupleStreams. */
public class SolrClientCache implements Closeable {

  // Set the floor for timeouts to 60 seconds.
  // Timeouts can be increased by setting the system properties defined below.
  private static final int MIN_TIMEOUT = 60000;
  private static final int minConnTimeout =
      Math.max(
          Integer.getInteger(SolrHttpConstants.PROP_CONNECTION_TIMEOUT, MIN_TIMEOUT), MIN_TIMEOUT);
  private static final int minSocketTimeout =
      Math.max(Integer.getInteger(SolrHttpConstants.PROP_SO_TIMEOUT, MIN_TIMEOUT), MIN_TIMEOUT);

  protected String basicAuthCredentials = null; // Only support with the httpJettySolrClient

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final HttpSolrClientBase httpSolrClient;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicReference<String> defaultZkHost = new AtomicReference<>();

  public SolrClientCache() {
    this.httpSolrClient = null;
  }

  public SolrClientCache(HttpSolrClientBase httpSolrClient) {
    this.httpSolrClient = httpSolrClient;
  }

  public void setBasicAuthCredentials(String basicAuthCredentials) {
    this.basicAuthCredentials = basicAuthCredentials;
  }

  public void setDefaultZKHost(String zkHost) {
    if (zkHost != null) {
      zkHost = zkHost.split("/")[0];
      if (!zkHost.isEmpty()) {
        defaultZkHost.set(zkHost);
      } else {
        defaultZkHost.set(null);
      }
    }
  }

  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {
    ensureOpen();
    Objects.requireNonNull(zkHost, "ZooKeeper host cannot be null!");
    if (solrClients.containsKey(zkHost)) {
      return (CloudSolrClient) solrClients.get(zkHost);
    }
    // Can only use ZK ACLs if there is a default ZK Host, and the given ZK host contains that
    // default.
    // Basically the ZK ACLs are assumed to be only used for the default ZK host,
    // thus we should only provide the ACLs to that Zookeeper instance.
    String zkHostNoChroot = zkHost.split("/")[0];
    boolean canUseACLs =
        Optional.ofNullable(defaultZkHost.get()).map(zkHostNoChroot::equals).orElse(false);

    final var client = newCloudSolrClient(zkHost, httpSolrClient, canUseACLs);
    solrClients.put(zkHost, client);
    return client;
  }

  protected CloudSolrClient newCloudSolrClient(
      String zkHost, HttpSolrClientBase httpSolrClient, boolean canUseACLs) {
    final List<String> hosts = List.of(zkHost);
    var builder = new CloudSolrClient.Builder(hosts, Optional.empty());
    builder.canUseZkACLs(canUseACLs);
    // using internal builder to ensure the internal client gets closed
    builder = builder.withHttpClientBuilder(newHttpSolrClientBuilder(null, httpSolrClient));
    var client = builder.build();
    try {
      client.connect();
    } catch (Exception e) {
      IOUtils.closeQuietly(client);
      throw e;
    }
    return client;
  }

  /**
   * Create (and cache) a SolrClient based around the provided URL
   *
   * @param baseUrl a Solr URL. May either be a "base" URL (i.e. ending in "/solr"), or point to a
   *     particular collection or core.
   * @return a SolrClient configured to use the provided URL. The cache retains a reference to the
   *     returned client, and will close it when callers invoke {@link SolrClientCache#close()}
   */
  public synchronized SolrClient getHttpSolrClient(String baseUrl) {
    ensureOpen();
    Objects.requireNonNull(baseUrl, "Url cannot be null!");
    if (solrClients.containsKey(baseUrl)) {
      return solrClients.get(baseUrl);
    }
    final var client = newHttpSolrClientBuilder(baseUrl, httpSolrClient).build();
    solrClients.put(baseUrl, client);
    return client;
  }

  protected HttpSolrClientBuilderBase<?, ?> newHttpSolrClientBuilder(
      String url, HttpSolrClientBase httpSolrClient) {
    final var builder =
        (url == null || URLUtil.isBaseUrl(url)) // URL may be null here and set by caller
            ? new HttpJettySolrClient.Builder(url)
            : new HttpJettySolrClient.Builder(URLUtil.extractBaseUrl(url))
                .withDefaultCollection(URLUtil.extractCoreFromCoreUrl(url));
    if (httpSolrClient != null) {
      builder.withHttpClient((HttpJettySolrClient) httpSolrClient); // TODO support JDK
      // cannot set connection timeout
    } else {
      builder.withConnectionTimeout(minConnTimeout, TimeUnit.MILLISECONDS);
    }
    builder.withIdleTimeout(
        Math.max(minSocketTimeout, builder.getIdleTimeoutMillis()), TimeUnit.MILLISECONDS);
    builder.withOptionalBasicAuthCredentials(basicAuthCredentials);

    return builder;
  }

  @Override
  public synchronized void close() {
    if (isClosed.compareAndSet(false, true)) {
      for (Map.Entry<String, SolrClient> entry : solrClients.entrySet()) {
        IOUtils.closeQuietly(entry.getValue());
      }
      solrClients.clear();
    }
  }

  private void ensureOpen() {
    if (isClosed.get()) {
      throw new AlreadyClosedException();
    }
  }
}
