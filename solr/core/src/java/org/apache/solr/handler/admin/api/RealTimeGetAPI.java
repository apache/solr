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
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.RealTimeGetApi;
import org.apache.solr.client.api.model.GetDocumentsResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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

  /**
   * Fetches the latest (possibly uncommitted) version of one or more documents.
   *
   * <p>The {@code id} and {@code ids} parameters are declared here to satisfy the interface
   * contract and to drive OpenAPI spec and client code generation. They are not used directly in
   * this method body because {@link #solrQueryRequest} is populated from the full HTTP request
   * (including all query parameters) before Jersey dispatches to this method, and {@link
   * RealTimeGetHandler#handleRequestBody} reads {@code id}/{@code ids} from there.
   */
  @Override
  @PermissionName(PermissionNameProvider.Name.READ_PERM)
  public GetDocumentsResponse getDocuments(String id, List<String> ids) throws Exception {
    final var response = instantiateJerseyResponse(GetDocumentsResponse.class);
    rtgHandler.handleRequestBody(solrQueryRequest, solrQueryResponse);
    populateResponse(response);
    return response;
  }

  /**
   * Copies the results written by {@link RealTimeGetHandler} into {@code solrQueryResponse} into
   * the typed fields of the {@link GetDocumentsResponse}.
   *
   * <p>When the {@code id} parameter is used, the handler places a single {@link SolrDocument}
   * under the {@code "doc"} key. When the {@code ids} parameter is used, it places a {@link
   * SolrDocumentList} under the {@code "response"} key.
   */
  private void populateResponse(GetDocumentsResponse response) {
    final Object doc = solrQueryResponse.getValues().get("doc");
    if (doc instanceof SolrDocument solrDoc) {
      response.doc = SolrDocumentFieldConverter.toFieldMap(solrDoc);
    }

    final Object docList = solrQueryResponse.getValues().get("response");
    if (docList instanceof SolrDocumentList list) {
      final var result = new GetDocumentsResponse.DocumentsResult();
      result.numFound = list.getNumFound();
      result.start = list.getStart();
      result.numFoundExact = list.getNumFoundExact();
      result.maxScore = list.getMaxScore();
      result.docs = new ArrayList<>();
      for (SolrDocument solrDoc : list) {
        result.docs.add(SolrDocumentFieldConverter.toFieldMap(solrDoc));
      }
      response.response = result;
    }
  }
}
