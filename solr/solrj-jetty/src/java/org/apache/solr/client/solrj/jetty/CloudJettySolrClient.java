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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;

/**
 * A {@link org.apache.solr.client.solrj.impl.CloudSolrClient} using Jetty {@code HttpClient} for
 * HTTP communication. This is Solr's most robust CloudSolrClient.
 */
public class CloudJettySolrClient extends CloudHttp2SolrClient {

  protected CloudJettySolrClient(Builder builder) {
    super(builder);
  }

  public static class Builder extends CloudSolrClient.Builder {

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
    protected HttpJettySolrClient createOrGetHttpClient() {
      if (httpClient != null) {
        return (HttpJettySolrClient) httpClient;
      } else if (internalClientBuilder != null) {
        return (HttpJettySolrClient) internalClientBuilder.build();
      } else {
        return new HttpJettySolrClient.Builder().build();
      }
    }
  }

  @Override
  public HttpJettySolrClient getHttpClient() {
    return (HttpJettySolrClient) super.getHttpClient();
  }
}
