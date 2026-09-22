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

package org.apache.solr.cloud;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.URLUtil;

/**
 * A restricted {@link SolrClientCache} for internal Solr use. Connections to clusters other than
 * the local one must pass a validator; see {@link ZkController#validateSolrConnection}.
 */
public class InternalSolrClientCache extends SolrClientCache {

  static {
    assert INTERNAL_IMPL_CLASS.equals(InternalSolrClientCache.class.getName())
        : "Update SolrClientCache.INTERNAL_IMPL_CLASS to match the renamed class";
  }

  private final CloudSolrClient.CloudSolrClientConnection defaultConnection;
  private final Consumer<CloudSolrClient.CloudSolrClientConnection> connectionValidator;

  public InternalSolrClientCache(
      HttpJettySolrClient httpSolrClient,
      CloudSolrClient.CloudSolrClientConnection solrConnection,
      Consumer<CloudSolrClient.CloudSolrClientConnection> connectionValidator) {
    super(); // not passing httpSolrClient down ...
    this.defaultConnection = solrConnection;
    this.connectionValidator = connectionValidator;
    // ... create one internal CloudSolrClient that is a bit special.
    var httpBuilder =
        new HttpJettySolrClient.Builder()
            .withIdleTimeout(minSocketTimeout, TimeUnit.MILLISECONDS)
            .withHttpClient(httpSolrClient);
    cloudSolClients.put(
        solrConnection,
        new CloudJettySolrClient.Builder(solrConnection)
            .canUseZkACLs(true)
            .withHttpClientBuilder(httpBuilder)
            .build());
  }

  @Override
  public synchronized CloudSolrClient getCloudSolrClient(
      CloudSolrClient.CloudSolrClientConnection solrConnection) {
    if (solrConnection == null) {
      solrConnection = defaultConnection;
    }
    CloudSolrClient client = cloudSolClients.get(solrConnection);
    if (client != null) {
      return client;
    }
    connectionValidator.accept(solrConnection); // throws if not allowed
    return super.getCloudSolrClient(solrConnection);
  }

  @Override
  protected HttpSolrClient.BuilderBase<?, ?> newHttpSolrClientBuilder(String url) {
    if (url != null) {
      // override to attempt to use the jetty client inside a matching CloudSolrClient.
      String baseUrl = URLUtil.isBaseUrl(url) ? url : URLUtil.extractBaseUrl(url);
      try {
        String nodeName = URLUtil.getNodeNameForBaseUrl(baseUrl);
        for (CloudSolrClient cloudSolrClient : cloudSolClients.values()) {
          if (cloudSolrClient.getClusterStateProvider().getLiveNodes().contains(nodeName)) {
            final var builder = new HttpJettySolrClient.Builder(baseUrl);
            if (!URLUtil.isBaseUrl(url)) {
              builder.withDefaultCollection(URLUtil.extractCoreFromCoreUrl(url));
            }
            builder.withHttpClient(
                (HttpJettySolrClient) ((CloudHttp2SolrClient) cloudSolrClient).getHttpClient());
            return builder;
          }
        }
      } catch (MalformedURLException | URISyntaxException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }
    return super.newHttpSolrClientBuilder(url);
  }
}
