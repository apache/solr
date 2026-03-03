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
package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.RealTimeGetApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RealTimeGetHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 API for fetching the latest (possibly uncommitted) version of one or more documents.
 *
 * <p>This API (GET /v2/collections/collectionName/get) is analogous to the v1
 * /solr/collectionName/get API.
 */
public class RealTimeGetAPI extends JerseyResource implements RealTimeGetApi {

  private final RealTimeGetHandler rtgHandler;
  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;

  @Inject
  public RealTimeGetAPI(
      SolrCore solrCore, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.rtgHandler = (RealTimeGetHandler) solrCore.getRequestHandler("/get");
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.READ_PERM)
  public SolrJerseyResponse getDocuments() throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    rtgHandler.handleRequestBody(solrQueryRequest, solrQueryResponse);
    return response;
  }
}
