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
import org.apache.solr.client.api.endpoint.UnloadCoreApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UnloadCoreRequestBody;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TestInjection;

/**
 * V2 API implementation for unloading a Solr core.
 *
 * <p>The API (POST /v2/cores/coreName/unload is equivalent to the v1 /admin/cores?action=unload
 * command.
 */
public class UnloadCore extends CoreAdminAPIBase implements UnloadCoreApi {

  @Inject
  public UnloadCore(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @PermissionName(CORE_EDIT_PERM)
  @Override
  public SolrJerseyResponse unloadCore(String coreName, UnloadCoreRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    SolrJerseyResponse solrJerseyResponse = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (requestBody == null) {
      requestBody = new UnloadCoreRequestBody();
    }
    final var requestBodyFinal = requestBody; // Lambda below requires a 'final' variable
    return handlePotentiallyAsynchronousTask(
        solrJerseyResponse,
        coreName,
        requestBody.async,
        "unload",
        () -> {
          CoreDescriptor cdescr = coreContainer.getCoreDescriptor(coreName);
          coreContainer.unload(
              coreName,
              requestBodyFinal.deleteIndex == null ? false : requestBodyFinal.deleteIndex,
              requestBodyFinal.deleteDataDir == null ? false : requestBodyFinal.deleteDataDir,
              requestBodyFinal.deleteInstanceDir == null
                  ? false
                  : requestBodyFinal.deleteInstanceDir);
          assert TestInjection.injectNonExistentCoreExceptionAfterUnload(coreName);
          return solrJerseyResponse;
        });
  }
}
