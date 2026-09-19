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

import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.security.PermissionNameProvider.Name.UPDATE_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.UpdateApi;
import org.apache.solr.client.api.model.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.jersey.APIConfigProvider;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation for indexing documents.
 *
 * <p>These APIs delegate to the v1 {@link UpdateRequestHandler}. The {@code /update} and {@code
 * /update/json} paths are rewritten to {@code /update/json/docs} so that JSON arrays of documents
 * are processed by the JSON loader rather than the update-command loader.
 */
public class UpdateAPI extends JerseyResource implements UpdateApi {

  private final UpdateRequestHandler updateRequestHandler;
  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;

  @Inject
  public UpdateAPI(
      UpdateRequestHandlerConfig handlerConfig,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    this.updateRequestHandler = handlerConfig.updateRequestHandler;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  // Query parameters like commit, overwrite, etc are declared as method arguments for the JAX-RS/OpenAPI contract
  // and via magic are read in by the handler.
  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse update(
      Boolean commit, Integer commitWithin, Boolean overwrite, Boolean softCommit, Boolean versions)
      throws Exception {
    return handleUpdate(UpdateRequestHandler.DOC_PATH);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateJson(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions) {
    return handleUpdate(UpdateRequestHandler.DOC_PATH);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateXml(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions) {
    return handleUpdate(null);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateCsv(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions) {
    return handleUpdate(null);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateJavabin(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions) {
    return handleUpdate(UpdateRequestHandler.BIN_PATH);
  }

  private UpdateResponse handleUpdate(String pathOverride) {
    final UpdateResponse response = instantiateJerseyResponse(UpdateResponse.class);
    if (pathOverride != null) {
      solrQueryRequest.getContext().put(PATH, pathOverride);
    }
    updateRequestHandler.handleRequest(solrQueryRequest, solrQueryResponse);
    rethrowAnyException(solrQueryResponse);
    response.adds = takeVersionResults("adds");
    response.deletes = takeVersionResults("deletes");
    response.deleteByQuery = takeVersionResults("deleteByQuery");
    return response;
  }

  private List<Object> takeVersionResults(String name) {
    final NamedList<?> values = (NamedList<?>) solrQueryResponse.getValues().remove(name);
    if (values == null) return null;
    final List<Object> pairs = new ArrayList<>(values.size() * 2);
    for (int i = 0; i < values.size(); i++) {
      pairs.add(values.getName(i));
      pairs.add(values.getVal(i));
    }
    return pairs;
  }

  private void rethrowAnyException(SolrQueryResponse rsp) {
    final Exception ex = rsp.getException();
    if (ex instanceof SolrException solrEx) throw solrEx;
    if (ex != null) throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ex);
  }

  /** Configuration object providing access to the {@link UpdateRequestHandler} instance. */
  public record UpdateRequestHandlerConfig(UpdateRequestHandler updateRequestHandler)
      implements APIConfigProvider.APIConfig {}
}
