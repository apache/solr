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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.DocumentApi;
import org.apache.solr.client.api.model.GetDocumentResponse;
import org.apache.solr.client.api.model.ListDocumentsResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 API for fetching documents as path-identified resources.
 *
 * <p>Unlike {@link RealTimeGetAPI} (the JAX-RS mirror of the v1 {@code /get} handler, which is
 * relied on internally by SolrCloud peer-sync machinery and so must keep its exact {@code
 * id}/{@code ids} query-param contract), this API calls {@link RealTimeGetComponent}'s underlying
 * document-lookup utilities directly rather than delegating to a request handler, and returns a
 * {@code 404} for a document that doesn't exist.
 *
 * @see DocumentApi
 */
public class Documents extends JerseyResource implements DocumentApi {

  private final SolrCore solrCore;

  @Inject
  public Documents(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.READ_PERM)
  public GetDocumentResponse getDocument(String id) throws IOException {
    final var response = instantiateJerseyResponse(GetDocumentResponse.class);

    final SolrDocument doc = fetchDocument(id);
    if (doc == null) {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such document: " + id);
    }
    response.doc = SolrDocumentFieldConverter.toFieldMap(doc);
    return response;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.READ_PERM)
  public ListDocumentsResponse listDocuments(List<String> ids) throws IOException {
    final var response = instantiateJerseyResponse(ListDocumentsResponse.class);

    final List<Map<String, Object>> docs = new ArrayList<>();
    if (ids != null) {
      // Each raw "ids" value may itself be a comma-separated list (e.g. "ids=a,b,c"), matching
      // the same convention RealTimeGetComponent.IdsRequested uses for the v1-mirrored /get API.
      for (String rawIds : ids) {
        for (String id : StrUtils.splitSmart(rawIds, ",", true)) {
          final SolrDocument doc = fetchDocument(id);
          if (doc != null) {
            docs.add(SolrDocumentFieldConverter.toFieldMap(doc));
          }
        }
      }
    }

    response.docs = docs;
    response.numFound = docs.size();
    response.start = 0;
    response.numFoundExact = true;
    return response;
  }

  private SolrDocument fetchDocument(String id) throws IOException {
    final SchemaField idField = solrCore.getLatestSchema().getUniqueKeyField();
    final FieldType fieldType = idField.getType();
    final BytesRefBuilder idBytesBuilder = new BytesRefBuilder();
    fieldType.readableToIndexed(id, idBytesBuilder);
    final BytesRef idBytes = idBytesBuilder.get();

    final SolrInputDocument inputDoc =
        RealTimeGetComponent.getInputDocument(
            solrCore, idBytes, idBytes, null, null, RealTimeGetComponent.Resolution.DOC);
    if (inputDoc == null) {
      return null;
    }
    return RealTimeGetComponent.toSolrDoc(inputDoc, solrCore.getLatestSchema());
  }
}
