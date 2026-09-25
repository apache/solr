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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.UpdateApi;
import org.apache.solr.client.api.model.ToleratedUpdateError;
import org.apache.solr.client.api.model.UpdateResponse;
import org.apache.solr.client.api.model.VersionedDocument;
import org.apache.solr.client.api.model.VersionedQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
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

  // Query parameters like commit, overwrite, etc are declared as method arguments for the
  // JAX-RS/OpenAPI contract and via magic are read in by the handler.
  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse update(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions,
      InputStream requestBody)
      throws Exception {
    return handleUpdate(null);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateJson(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions,
      InputStream requestBody) {
    return handleUpdate(UpdateRequestHandler.DOC_PATH);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateXml(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions,
      InputStream requestBody) {
    return handleUpdate(null);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateCsv(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions,
      InputStream requestBody) {
    return handleUpdate(null);
  }

  @Override
  @PermissionName(UPDATE_PERM)
  public UpdateResponse updateJavabin(
      Boolean commit,
      Integer commitWithin,
      Boolean overwrite,
      Boolean softCommit,
      Boolean versions,
      InputStream requestBody) {
    return handleUpdate(UpdateRequestHandler.BIN_PATH);
  }

  private UpdateResponse handleUpdate(String pathOverride) {
    final UpdateResponse response = instantiateJerseyResponse(UpdateResponse.class);
    if (pathOverride != null) {
      solrQueryRequest.getContext().put(PATH, pathOverride);
    }
    // TolerantUpdateProcessor (errors/maxErrors) and DistributedZkUpdateProcessor (rf) write
    // their payload into the legacy response header while handling the request. Initialize it
    // for the handler, copy that payload onto the typed response, then discard the header so
    // only one responseHeader (the typed Jersey one) is returned to the client.
    //
    // Uses handleRequestWithoutMetrics rather than handleRequest: this resource is wrapped by
    // Jersey's RequestMetricHandling filters, which already record request/error metrics for
    // updateRequestHandler (see PluginBag.JaxrsResourceToHandlerMappings) -- handleRequest would
    // double-count both.
    SolrCore.preDecorateResponse(solrQueryRequest, solrQueryResponse);
    try {
      updateRequestHandler.handleRequestWithoutMetrics(solrQueryRequest, solrQueryResponse);
    } finally {
      copyToleratedUpdateMetadata(response);
      solrQueryResponse.getValues().remove("responseHeader");
    }
    rethrowAnyException(solrQueryResponse);
    response.adds = takeDocumentVersionResults("adds");
    response.deletes = takeDocumentVersionResults("deletes");
    response.deleteByQuery = takeQueryVersionResults();
    return response;
  }

  /**
   * Copies replication-factor and tolerant-update-error metadata off the legacy response header and
   * onto the typed response, before that header is discarded.
   *
   * @see org.apache.solr.update.processor.TolerantUpdateProcessor
   * @see org.apache.solr.update.processor.DistributedZkUpdateProcessor
   */
  @SuppressWarnings("unchecked")
  private void copyToleratedUpdateMetadata(UpdateResponse response) {
    final NamedList<Object> header = solrQueryResponse.getResponseHeader();
    if (header == null) return;

    final Object rf = header.get("rf");
    if (rf != null) response.rf = ((Number) rf).intValue();

    final Object maxErrors = header.get("maxErrors");
    if (maxErrors != null) response.maxErrors = ((Number) maxErrors).intValue();

    final var rawErrors = (List<? extends NamedList<String>>) header.get("errors");
    if (rawErrors != null && !rawErrors.isEmpty()) {
      response.errors = new ArrayList<>(rawErrors.size());
      for (NamedList<String> rawError : rawErrors) {
        final ToleratedUpdateError error = new ToleratedUpdateError();
        error.type = rawError.get("type");
        error.id = rawError.get("id");
        error.message = rawError.get("message");
        response.errors.add(error);
      }
    }
  }

  private List<VersionedDocument> takeDocumentVersionResults(String name) {
    final NamedList<?> values = (NamedList<?>) solrQueryResponse.getValues().remove(name);
    if (values == null) return null;
    final List<VersionedDocument> results = new ArrayList<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      final VersionedDocument result = new VersionedDocument();
      result.id = values.getName(i);
      result.version = ((Number) values.getVal(i)).longValue();
      results.add(result);
    }
    return results;
  }

  private List<VersionedQuery> takeQueryVersionResults() {
    final NamedList<?> values =
        (NamedList<?>) solrQueryResponse.getValues().remove("deleteByQuery");
    if (values == null) return null;
    final List<VersionedQuery> results = new ArrayList<>(values.size());
    for (int i = 0; i < values.size(); i++) {
      final VersionedQuery result = new VersionedQuery();
      result.query = values.getName(i);
      result.version = ((Number) values.getVal(i)).longValue();
      results.add(result);
    }
    return results;
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
