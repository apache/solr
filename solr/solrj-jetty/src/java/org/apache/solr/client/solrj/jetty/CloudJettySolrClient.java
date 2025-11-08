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

package org.apache.solr.client.solrj.jetty;

import java.util.List;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

/**
 * A {@link org.apache.solr.client.solrj.impl.CloudSolrClient} using Jetty {@code HttpClient} for
 * HTTP communication. This is Solr's most robust CloudSolrClient.
 */
public class CloudJettySolrClient extends CloudHttp2SolrClient {

  protected CloudJettySolrClient(Builder builder) {
    super(builder);
  }

  public static class Builder extends CloudHttp2SolrClient.Builder {

    public Builder(List<String> solrUrls) {
      super(solrUrls);
    }

    public Builder(ClusterStateProvider stateProvider) {
      super(stateProvider);
    }

    @Override
    public CloudJettySolrClient build() {
      return new CloudJettySolrClient(this);
    }

    @Override
    protected Http2SolrClient createOrGetHttpClient() {
      if (httpClient != null) {
        return (Http2SolrClient) httpClient;
      } else if (internalClientBuilder != null) {
        return (Http2SolrClient) internalClientBuilder.build();
      } else {
        return new Http2SolrClient.Builder().build();
      }
    }
  }

  @Override
  public Http2SolrClient getHttpClient() {
    return (Http2SolrClient) super.getHttpClient();
  }
}
