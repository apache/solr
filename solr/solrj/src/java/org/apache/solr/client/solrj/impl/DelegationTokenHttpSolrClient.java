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
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.SolrParams;

public class DelegationTokenHttpSolrClient extends HttpSolrClient {
  public static final String DELEGATION_TOKEN_PARAM = "delegation";

  /**
   * Constructor for use by {@linkplain org.apache.solr.client.solrj.impl.HttpSolrClient.Builder}.
   *
   * @lucene.internal
   */
  protected DelegationTokenHttpSolrClient(Builder builder) {
    super(builder);
  }

  @Override
  protected HttpRequestBase createMethod(final SolrRequest<?> request, String collection)
      throws IOException, SolrServerException {
    SolrParams params = request.getParams();
    if (params != null && params.getParams(DELEGATION_TOKEN_PARAM) != null) {
      throw new IllegalArgumentException(DELEGATION_TOKEN_PARAM + " parameter not supported");
    }
    return super.createMethod(request, collection);
  }
}
