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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClientBase;
import org.apache.solr.client.solrj.impl.SolrHttpConstants;
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

  private final Map<String, SolrClient> httpSolrClients = new HashMap<>();
  protected final Map<CloudSolrClient.CloudSolrClientConnection, CloudSolrClient> cloudSolClients =
      new HashMap<>();
  private final HttpSolrClient httpSolrClient;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private boolean useZookeeperACL = false;
  public SolrClientCache() {
    this.httpSolrClient = null;
  }

  public SolrClientCache(HttpSolrClient httpSolrClient) {
    this.httpSolrClient = httpSolrClient;
  }

  public void setBasicAuthCredentials(String basicAuthCredentials) {
    this.basicAuthCredentials = basicAuthCredentials;
  }

  /**
   * Controls whether ZooKeeper ACL credentials may be propagated to ZooKeeper hosts used by {@link
   * CloudSolrClient} instances created by this cache.
   *
   * <p>This option is disabled by default for security reasons. Enabling it may expose ZooKeeper
   * credentials to external or untrusted ZooKeeper ensembles if arbitrary cluster connections are
   * allowed.
   *
   * @param useZookeeperACL whether ZooKeeper ACL credentials should be used by clients created from
   *     this cache
   */
  public void setUseZookeeperACL(boolean useZookeeperACL) {
    this.useZookeeperACL = useZookeeperACL;
  }

  /**
   * @deprecated use {@link #getCloudSolrClient(CloudSolrClient.CloudSolrClientConnection)}
   */
  @Deprecated
  public CloudSolrClient getCloudSolrClient(String solrConnectionString) {
    var solrConnection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnectionString);
    return getCloudSolrClient(solrConnection);
  }

  public synchronized CloudSolrClient getCloudSolrClient(
      CloudSolrClient.CloudSolrClientConnection solrConnection) {
    ensureOpen();
    if (cloudSolClients.containsKey(solrConnection)) {
      return cloudSolClients.get(solrConnection);
    }
    final var client = newCloudSolrClient(solrConnection, httpSolrClient, useZookeeperACL);
    cloudSolClients.put(solrConnection, client);
    return client;
  }

  protected CloudSolrClient newCloudSolrClient(
      CloudSolrClient.CloudSolrClientConnection cloudClientConnection,
      HttpSolrClient httpSolrClient,
      boolean canUseACLs) {
    var builder = new CloudSolrClient.Builder(cloudClientConnection);
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
    if (httpSolrClients.containsKey(baseUrl)) {
      return httpSolrClients.get(baseUrl);
    }
    final var client = newHttpSolrClientBuilder(baseUrl, httpSolrClient).build();
    httpSolrClients.put(baseUrl, client);
    return client;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected HttpSolrClient.BuilderBase<?, ?> newHttpSolrClientBuilder(
      String url, HttpSolrClient httpSolrClient) {
    final var builder =
        (url == null || URLUtil.isBaseUrl(url)) // URL may be null here and set by caller
            ? HttpSolrClient.builder(url)
            : HttpSolrClient.builder(URLUtil.extractBaseUrl(url))
                .withDefaultCollection(URLUtil.extractCoreFromCoreUrl(url));
    if (httpSolrClient != null) {
      // this generics hack works around the fact that we can't guarantee that the new client and
      // the existing passed-in client are compatible  Oh well; tough luck, best-effort.
      ((HttpSolrClient.BuilderBase) builder).withHttpClient(httpSolrClient);
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
      for (SolrClient solrClient : httpSolrClients.values()) {
        IOUtils.closeQuietly(solrClient);
      }
      httpSolrClients.clear();
      for (CloudSolrClient solrClient : cloudSolClients.values()) {
        IOUtils.closeQuietly(solrClient);
      }
      cloudSolClients.clear();
    }
  }

  private void ensureOpen() {
    if (isClosed.get()) {
      throw new AlreadyClosedException();
    }
  }
}
