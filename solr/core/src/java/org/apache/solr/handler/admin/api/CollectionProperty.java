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

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.io.IOException;
import org.apache.solr.client.api.endpoint.CollectionPropertyApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpdateCollectionPropertyRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementations for modifying collection-level properties.
 *
 * <p>These APIs (PUT and DELETE /api/collections/collName/properties/propName) are analogous to the
 * v1 /admin/collections?action=COLLECTIONPROP command.
 */
public class CollectionProperty extends AdminAPIBase implements CollectionPropertyApi {

  public CollectionProperty(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createOrUpdateCollectionProperty(
      String collName, String propName, UpdateCollectionPropertyRequestBody requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    recordCollectionForLogAndTracing(collName, solrQueryRequest);
    modifyCollectionProperty(collName, propName, requestBody.value);
    return response;
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse deleteCollectionProperty(String collName, String propName)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    recordCollectionForLogAndTracing(collName, solrQueryRequest);
    modifyCollectionProperty(collName, propName, null);
    return response;
  }

  private void modifyCollectionProperty(
      String collection, String propertyName, String propertyValue /* May be null for deletes */)
      throws IOException {
    String resolvedCollection = coreContainer.getAliases().resolveSimpleAlias(collection);
    CollectionProperties cp =
        new CollectionProperties(coreContainer.getZkController().getZkClient());
    cp.setCollectionProperty(resolvedCollection, propertyName, propertyValue);
  }
}
