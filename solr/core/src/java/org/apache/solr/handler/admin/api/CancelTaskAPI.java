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

import static org.apache.solr.response.SolrQueryResponse.RESPONSE_HEADER_KEY;
import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.CancelTaskApi;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryCancellationHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for cancelling a currently running "task".
 *
 * <p>This API (GET /v2/collections/{collectionName}/tasks/cancel and GET
 * /v2/cores/{coreName}/tasks/cancel) is analogous to the v1
 * /solr/{collectionName|coreName}/tasks/cancel API.
 */
public class CancelTaskAPI extends JerseyResource implements CancelTaskApi {
  private final SolrCore solrCore;
  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;

  @Inject
  public CancelTaskAPI(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    this.solrCore = solrCore;
    this.solrQueryRequest = req;
    this.solrQueryResponse = rsp;
  }

  @Override
  @PermissionName(READ_PERM)
  public FlexibleSolrJerseyResponse cancelTask(String queryUUID) throws Exception {
    final FlexibleSolrJerseyResponse response =
        instantiateJerseyResponse(FlexibleSolrJerseyResponse.class);
    ensureRequiredParameterProvided("queryUUID", queryUUID);
    final QueryCancellationHandler cancellationHandler =
        (QueryCancellationHandler) solrCore.getRequestHandler("/tasks/cancel");
    cancellationHandler.handleRequestBody(solrQueryRequest, solrQueryResponse);
    // Copy response data added by the handler into the jersey response object
    solrQueryResponse
        .getValues()
        .forEach(
            (key, value) -> {
              if (!RESPONSE_HEADER_KEY.equals(key)) {
                response.setUnknownProperty(key, value);
              }
            });
    return response;
  }
}
