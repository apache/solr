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

package org.apache.solr.client.solrj.impl;

import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.IOUtils;

/**
 * This {@link CloudSolrClient} is a base implementation using a {@link HttpSolrClientBase}. The '2'
 * in the name has no differentiating significance anymore.
 *
 * @since solr 8.0
 * @lucene.internal
 */
@SuppressWarnings("serial")
public class CloudHttp2SolrClient extends CloudSolrClient {

  private final ClusterStateProvider stateProvider;
  private final LBSolrClient lbClient;
  private final HttpSolrClientBase myClient;
  private final boolean clientIsInternal;

  /**
   * Create a new client object that connects to Zookeeper and is always aware of the SolrCloud
   * state. If there is a fully redundant Zookeeper quorum and SolrCloud has enough replicas for
   * every shard in a collection, there is no single point of failure. Updates will be sent to shard
   * leaders by default.
   *
   * @param builder a {@link CloudSolrClient.Builder} with the options used to create the client.
   */
  protected CloudHttp2SolrClient(Builder builder) {
    super(builder.shardLeadersOnly, builder.parallelUpdates, builder.directUpdatesToLeadersOnly);
    this.clientIsInternal = builder.httpClient == null;
    try {
      this.myClient = builder.createOrGetHttpClient();
      this.lbClient = builder.createOrGetLbClient(myClient);
      this.stateProvider = createClusterStateProvider(builder);
    } catch (RuntimeException e) {
      close();
      throw e;
    }
    this.retryExpiryTimeNano = builder.retryExpiryTimeNano;
    this.defaultCollection = builder.defaultCollection;
    if (builder.requestWriter != null) {
      this.myClient.requestWriter = builder.requestWriter;
    }
    if (builder.responseParser != null) {
      this.myClient.setParser(builder.responseParser);
    }

    this.collectionStateCache.timeToLiveMs =
        TimeUnit.MILLISECONDS.convert(builder.timeToLiveSeconds, TimeUnit.SECONDS);

    //  If caches are expired then they are refreshed after acquiring a lock. Set the number of
    // locks.
    this.locks = objectList(builder.parallelCacheRefreshesLocks);
  }

  private ClusterStateProvider createClusterStateProvider(Builder builder) {
    if (builder.stateProvider != null) {
      return builder.stateProvider;
    } else if (builder.zkHosts.isEmpty()) {
      return builder.createHttpClusterStateProvider(this.myClient);
    } else {
      return builder.createZkClusterStateProvider();
    }
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(stateProvider);
    IOUtils.closeQuietly(lbClient);

    if (clientIsInternal) {
      IOUtils.closeQuietly(myClient);
    }
    super.close();
  }

  @Override
  public LBSolrClient getLbClient() {
    return lbClient;
  }

  @Override
  public ClusterStateProvider getClusterStateProvider() {
    return stateProvider;
  }

  public HttpSolrClientBase getHttpClient() {
    return myClient;
  }
}
