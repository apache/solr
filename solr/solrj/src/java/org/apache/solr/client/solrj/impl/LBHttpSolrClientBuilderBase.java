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

public abstract class LBHttpSolrClientBuilderBase<
    A extends LBHttpSolrClientBase<?>,
    B extends LBHttpSolrClientBuilderBase<?, ?, ?>,
    C extends HttpSolrClientBase> {
  final C solrClient;
  protected final LBSolrClient.Endpoint[] solrEndpoints;
  long aliveCheckIntervalMillis =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 minute between checks
  protected String defaultCollection;

  public abstract A build();

  public LBHttpSolrClientBuilderBase(C http2Client, LBSolrClient.Endpoint... endpoints) {
    this.solrClient = http2Client;
    this.solrEndpoints = endpoints;
  }

  /**
   * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use
   * this to set that interval
   *
   * @param aliveCheckInterval how often to ping for aliveness
   */
  @SuppressWarnings("unchecked")
  public B setAliveCheckInterval(int aliveCheckInterval, TimeUnit unit) {
    if (aliveCheckInterval <= 0) {
      throw new IllegalArgumentException(
          "Alive check interval must be " + "positive, specified value = " + aliveCheckInterval);
    }
    this.aliveCheckIntervalMillis = TimeUnit.MILLISECONDS.convert(aliveCheckInterval, unit);
    return (B) this;
  }

  /** Sets a default for core or collection based requests. */
  @SuppressWarnings("unchecked")
  public B withDefaultCollection(String defaultCoreOrCollection) {
    this.defaultCollection = defaultCoreOrCollection;
    return (B) this;
  }
}
