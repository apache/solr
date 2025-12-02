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

import java.io.IOException;
import java.util.List;

/** A CSP that uses Solr HTTP APIs. */
public class HttpClusterStateProvider<C extends HttpSolrClientBase>
    extends BaseHttpClusterStateProvider {
  // formerly known as Http2ClusterStateProvider

  final C httpClient;

  /**
   * Provide the solr urls and a solr http client for this cluster state provider to use. It is the
   * caller's responsibility to close the client.
   *
   * @param solrUrls root path solr urls
   * @param httpClient an instance of HttpSolrClientBase
   * @throws Exception if a problem with initialization occurs
   */
  public HttpClusterStateProvider(List<String> solrUrls, C httpClient) throws Exception {
    if (httpClient == null) {
      throw new IllegalArgumentException("You must provide an Http client.");
    }
    this.httpClient = httpClient;
    initConfiguredNodes(solrUrls);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected C getSolrClient(String baseUrl) {
    return (C) httpClient.builder().withBaseSolrUrl(baseUrl).build();
  }

  public C getHttpClient() {
    return httpClient;
  }
}
