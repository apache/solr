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
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientBuilder;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The SolrClientCache caches SolrClients so they can be reused by different TupleStreams. */
public class SolrClientCache implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Set the floor for timeouts to 60 seconds.
  // Timeouts cans be increased by setting the system properties defined below.
  private static final int MIN_TIMEOUT = 60000;
  private static final int minConnTimeout =
      Math.max(
          Integer.getInteger(HttpClientUtil.PROP_CONNECTION_TIMEOUT, MIN_TIMEOUT), MIN_TIMEOUT);
  private static final int minSocketTimeout =
      Math.max(Integer.getInteger(HttpClientUtil.PROP_SO_TIMEOUT, MIN_TIMEOUT), MIN_TIMEOUT);

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final HttpClient apacheHttpClient;
  private final Http2SolrClient http2SolrClient;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicReference<String> defaultZkHost = new AtomicReference<>();

  public SolrClientCache() {
    this.apacheHttpClient = null;
    this.http2SolrClient = null;
  }

  @Deprecated(since = "9.0")
  public SolrClientCache(HttpClient apacheHttpClient) {
    this.apacheHttpClient = apacheHttpClient;
    this.http2SolrClient = null;
  }

  public SolrClientCache(Http2SolrClient http2SolrClient) {
    this.apacheHttpClient = null;
    this.http2SolrClient = http2SolrClient;
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
    final CloudSolrClient client;
    if (apacheHttpClient != null) {
      client = newCloudLegacySolrClient(zkHost, apacheHttpClient, canUseACLs);
    } else {
      client = newCloudHttp2SolrClient(zkHost, http2SolrClient, canUseACLs);
    }
    solrClients.put(zkHost, client);
    return client;
  }

  @Deprecated
  private static CloudSolrClient newCloudLegacySolrClient(
      String zkHost, HttpClient httpClient, boolean canUseACLs) {
    final List<String> hosts = List.of(zkHost);
    var builder = new CloudLegacySolrClient.Builder(hosts, Optional.empty());
    builder.canUseZkACLs(canUseACLs);
    adjustTimeouts(builder, httpClient);
    var client = builder.build();
    try {
      client.connect();
    } catch (Exception e) {
      IOUtils.closeQuietly(client);
      throw e;
    }
    return client;
  }

  private static CloudHttp2SolrClient newCloudHttp2SolrClient(
      String zkHost, Http2SolrClient http2SolrClient, boolean canUseACLs) {
    final List<String> hosts = List.of(zkHost);
    var builder = new CloudHttp2SolrClient.Builder(hosts, Optional.empty());
    builder.canUseZkACLs(canUseACLs);
    // using internal builder to ensure the internal client gets closed
    builder = builder.withInternalClientBuilder(newHttp2SolrClientBuilder(null, http2SolrClient));
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
   * @param baseUrl a Solr URL. May be either a "base" URL (i.e. ending in "/solr"), or point to a
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
    final SolrClient client;
    if (apacheHttpClient != null) {
      client = newHttpSolrClient(baseUrl, apacheHttpClient);
    } else {
      client = newHttp2SolrClientBuilder(baseUrl, http2SolrClient).build();
    }
    solrClients.put(baseUrl, client);
    return client;
  }

  @Deprecated
  private static SolrClient newHttpSolrClient(String url, HttpClient httpClient) {
    final var builder =
        (URLUtil.isBaseUrl(url))
            ? new HttpSolrClient.Builder(url)
            : new HttpSolrClient.Builder(URLUtil.extractBaseUrl(url))
                .withDefaultCollection(URLUtil.extractCoreFromCoreUrl(url));
    adjustTimeouts(builder, httpClient);
    return builder.build();
  }

  @Deprecated
  private static void adjustTimeouts(SolrClientBuilder<?> builder, HttpClient httpClient) {
    builder.withHttpClient(httpClient);
    int socketTimeout = Math.max(minSocketTimeout, builder.getSocketTimeoutMillis());
    builder.withSocketTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    int connTimeout = Math.max(minConnTimeout, builder.getConnectionTimeoutMillis());
    builder.withConnectionTimeout(connTimeout, TimeUnit.MILLISECONDS);
  }

  private static Http2SolrClient.Builder newHttp2SolrClientBuilder(
      String url, Http2SolrClient http2SolrClient) {
    final var builder =
        (url == null || URLUtil.isBaseUrl(url)) // URL may be null here and set by caller
            ? new Http2SolrClient.Builder(url)
            : new Http2SolrClient.Builder(URLUtil.extractBaseUrl(url))
                .withDefaultCollection(URLUtil.extractCoreFromCoreUrl(url));
    if (http2SolrClient != null) {
      builder.withHttpClient(http2SolrClient);
    }
    long idleTimeout = minSocketTimeout;
    if (builder.getIdleTimeoutMillis() != null) {
      idleTimeout = Math.max(idleTimeout, builder.getIdleTimeoutMillis());
    }
    builder.withIdleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    long connTimeout = minConnTimeout;
    if (builder.getConnectionTimeout() != null) {
      connTimeout = Math.max(idleTimeout, builder.getConnectionTimeout());
    }
    builder.withConnectionTimeout(connTimeout, TimeUnit.MILLISECONDS);
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
