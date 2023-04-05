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

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SolrClientCache caches SolrClients so they can be reused by different TupleStreams.
 *
 * <p>TODO: Cut this over to using Solr's new Http2 clients
 */
public class SolrClientCache implements Serializable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, SolrClient> solrClients = new HashMap<>();
  private final HttpClient httpClient;
  // Set the floor for timeouts to 60 seconds.
  // Timeouts cans be increased by setting the system properties defined below.
  private static final int conTimeout =
      Math.max(
          Integer.parseInt(System.getProperty(HttpClientUtil.PROP_CONNECTION_TIMEOUT, "60000")),
          60000);
  private static final int socketTimeout =
      Math.max(
          Integer.parseInt(System.getProperty(HttpClientUtil.PROP_SO_TIMEOUT, "60000")), 60000);

  public SolrClientCache() {
    httpClient = null;
  }

  @Deprecated(since = "9.0")
  public SolrClientCache(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @Deprecated(since = "9.0")
  public synchronized CloudSolrClient getCloudSolrClient(String zkHost) {

    // Timeouts should never be lower then 60000 but they can be set higher
    assert (conTimeout >= 60000);
    assert (socketTimeout >= 60000);

    if (log.isDebugEnabled()) {
      log.debug("SolrClientCache.conTimeout: {}", conTimeout);
      log.debug("SolrClientCache.socketTimeout: {}", socketTimeout);
    }

    Objects.requireNonNull(zkHost, "ZooKeeper host cannot be null!");
    CloudSolrClient client;
    if (solrClients.containsKey(zkHost)) {
      client = (CloudSolrClient) solrClients.get(zkHost);
    } else {
      final List<String> hosts = new ArrayList<>();
      hosts.add(zkHost);
      var builder =
          new CloudLegacySolrClient.Builder(hosts, Optional.empty())
              .withSocketTimeout(socketTimeout, TimeUnit.MILLISECONDS)
              .withConnectionTimeout(conTimeout, TimeUnit.MILLISECONDS);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }

      client = builder.build();
      client.connect();
      solrClients.put(zkHost, client);
    }

    return client;
  }

  @Deprecated(since = "9.0")
  public synchronized SolrClient getHttpSolrClient(String baseUrl) {
    SolrClient client;
    if (solrClients.containsKey(baseUrl)) {
      client = solrClients.get(baseUrl);
    } else {
      HttpSolrClient.Builder builder =
          new HttpSolrClient.Builder(baseUrl)
              .withSocketTimeout(socketTimeout, TimeUnit.MILLISECONDS)
              .withConnectionTimeout(conTimeout, TimeUnit.MILLISECONDS);
      if (httpClient != null) {
        builder = builder.withHttpClient(httpClient);
      }
      client = builder.build();
      solrClients.put(baseUrl, client);
    }
    return client;
  }

  public synchronized void close() {
    for (Map.Entry<String, SolrClient> entry : solrClients.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        log.error("Error closing SolrClient for {}", entry.getKey(), e);
      }
    }
    solrClients.clear();
  }
}
