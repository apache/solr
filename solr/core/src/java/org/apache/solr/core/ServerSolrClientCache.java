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
package org.apache.solr.core;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;

/** New home for default http client. */
public class ServerSolrClientCache extends SolrClientCache {

  public ServerSolrClientCache(HttpClient solrClient) {
    super(solrClient);
    Http2SolrClient.Builder http2SolrClientBuilder = new Http2SolrClient.Builder();
    InstrumentedHttpListenerFactory trackHttpSolrMetrics =
        new InstrumentedHttpListenerFactory(
            InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(
                UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY));
    long idleTimeout = minSocketTimeout;
    if (http2SolrClientBuilder.getIdleTimeoutMillis() != null) {
      idleTimeout = Math.max(idleTimeout, http2SolrClientBuilder.getIdleTimeoutMillis());
    }
    http2SolrClientBuilder.withIdleTimeout(idleTimeout, TimeUnit.MILLISECONDS);
    long connTimeout = minConnTimeout;
    if (http2SolrClientBuilder.getConnectionTimeout() != null) {
      connTimeout = Math.max(idleTimeout, http2SolrClientBuilder.getConnectionTimeout());
    }
    http2SolrClientBuilder.withConnectionTimeout(connTimeout, TimeUnit.MILLISECONDS);
    http2SolrClientBuilder.withListenerFactory(List.of(trackHttpSolrMetrics));

    http2SolrClient = http2SolrClientBuilder.build();
  }

  public SolrClient getHttpSolrClient() {
    return http2SolrClient;
  }

  @Override
  public synchronized void close() {
    super.close();
    IOUtils.closeQuietly(http2SolrClient);
  }

  public void setSecurityBuilder(HttpClientBuilderPlugin builder) {
    builder.setup(http2SolrClient);
  }
}
