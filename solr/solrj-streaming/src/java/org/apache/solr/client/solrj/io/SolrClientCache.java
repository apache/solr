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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrHttpConstants;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.URLUtil;

/** The SolrClientCache caches SolrClients, so they can be reused by different TupleStreams. */
public class SolrClientCache implements Closeable {

  /**
   * Sentinel value meaning "no practical timeout" for streaming clients.
   *
   * <p>Tuple streams (export, analytics, etc.) can run for a very long time. We default both the
   * total request timeout and idle timeout to this value so that long-lived result streams are not
   * artificially killed. Lower-level TCP/OS timeouts still apply.
   */
  static final long NO_TIMEOUT = Long.MAX_VALUE;

  // Connection timeout floor (only used when we create the underlying transport ourselves).
  private static final int MIN_TIMEOUT = 60000;
  private static final int minConnTimeout =
      Math.max(
          Integer.getInteger(SolrHttpConstants.PROP_CONNECTION_TIMEOUT, MIN_TIMEOUT), MIN_TIMEOUT);

  /**
   * Configurable timeouts for clients created by this cache.
   *
   * <p>These default to "no timeout" because this cache is used almost exclusively by streaming
   * expressions that legitimately produce long-running responses. Use the system properties (or Env
   * var equivalents via EnvUtils) to tighten them if desired for your environment.
   *
   * <ul>
   *   <li>solr.solrclientcache.request.timeout.ms - total time allowed for a request/response
   *       (including streaming the entire result set).
   *   <li>solr.solrclientcache.idle.timeout.ms - max idle time between data on the connection
   *       (affects how long we wait for the first response bytes and gaps during streaming).
   * </ul>
   */
  private static final long streamingRequestTimeoutMs =
      EnvUtils.getPropertyAsLong("solr.solrclientcache.request.timeout.ms", NO_TIMEOUT);

  private static final long streamingIdleTimeoutMs =
      EnvUtils.getPropertyAsLong("solr.solrclientcache.idle.timeout.ms", NO_TIMEOUT);

  protected String basicAuthCredentials = null; // Only support with the httpJettySolrClient

  private final Map<String, SolrClient> httpSolrClients = new HashMap<>();
  private final Map<CloudSolrClient.CloudSolrClientConnection, CloudSolrClient> cloudSolClients =
      new HashMap<>();
  private final HttpSolrClient httpSolrClient;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicReference<String> defaultZkHost = new AtomicReference<>();

  public SolrClientCache() {
    this.httpSolrClient = null;
  }

  public SolrClientCache(HttpSolrClient httpSolrClient) {
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
    return cloudSolClients.computeIfAbsent(
        solrConnection, sc -> newCloudSolrClient(sc, httpSolrClient, useAclForZookeeper(sc)));
  }

  // Can only use ZK ACLs if there is a default ZK Host, and the given ZK host contains that
  // default.
  // Basically the ZK ACLs are assumed to be only used for the default ZK host,
  // thus we should only provide the ACLs to that Zookeeper instance.
  private boolean useAclForZookeeper(CloudSolrClient.CloudSolrClientConnection solrConnection) {
    boolean canUseACLs = false;
    if (solrConnection.isZookeeper()) {
      String zkHostNoChroot = String.join(",", solrConnection.quorumItems());
      canUseACLs =
          Optional.ofNullable(defaultZkHost.get()).map(zkHostNoChroot::equals).orElse(false);
    }
    return canUseACLs;
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
    return httpSolrClients.computeIfAbsent(
        baseUrl, url -> newHttpSolrClientBuilder(url, httpSolrClient).build());
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

    // Streaming / TupleStream use cases need very long (or unlimited) timeouts for both total
    // request duration and idle time between chunks. The header-wait path in Http*SolrClient also
    // uses the client's idle timeout, so a high value here enables slow-starting long queries.
    builder.withIdleTimeout(streamingIdleTimeoutMs, TimeUnit.MILLISECONDS);
    builder.withRequestTimeout(streamingRequestTimeoutMs, TimeUnit.MILLISECONDS);

    builder.withOptionalBasicAuthCredentials(basicAuthCredentials);

    return builder;
  }

  /**
   * Visible for testing. Returns the builder that would be used so tests can inspect the configured
   * timeouts without using reflection or forbidden APIs.
   */
  HttpSolrClient.BuilderBase<?, ?> newHttpSolrClientBuilderForTest(String url) {
    return newHttpSolrClientBuilder(url, null);
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
