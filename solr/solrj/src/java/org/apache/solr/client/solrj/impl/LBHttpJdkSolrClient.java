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

import org.apache.solr.client.solrj.SolrClient;

public class LBHttpJdkSolrClient extends LBHttpSolrClientBase<HttpJdkSolrClient> {

  protected LBHttpJdkSolrClient(Builder b) {
    super(b);
  }

  @Override
  protected SolrClient getClient(Endpoint endpoint) {
    return solrClient;
  }

  public static class Builder
      extends LBHttpSolrClientBuilderBase<
          LBHttpJdkSolrClient, LBHttpJdkSolrClient.Builder, HttpJdkSolrClient> {

    public Builder(HttpJdkSolrClient httpJdkClient, Endpoint... endpoints) {
      super(httpJdkClient, endpoints);
    }

    @Override
    public LBHttpJdkSolrClient build() {
      return new LBHttpJdkSolrClient(this);
    }
  }
}
