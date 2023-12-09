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

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.RenameCoreApi;
import org.apache.solr.client.api.model.RenameCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for renaming an existing Solr core.
 *
 * <p>The new API (POST /v2/cores/coreName/rename is equivalent to the v1 /admin/cores?action=rename
 * command.
 */
public class RenameCore extends CoreAdminAPIBase implements RenameCoreApi {
  @Inject
  public RenameCore(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @PermissionName(CORE_EDIT_PERM)
  @Override
  public SolrJerseyResponse renameCore(String coreName, RenameCoreRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    ensureRequiredRequestBodyProvided(requestBody);
    ensureRequiredParameterProvided("to", requestBody.to);
    SolrJerseyResponse solrJerseyResponse = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (coreName.equals(requestBody.to)) return solrJerseyResponse;
    return handlePotentiallyAsynchronousTask(
        solrJerseyResponse,
        coreName,
        requestBody.async,
        "rename",
        () -> {
          coreContainer.rename(coreName, requestBody.to);
          return solrJerseyResponse;
        });
  }
}
