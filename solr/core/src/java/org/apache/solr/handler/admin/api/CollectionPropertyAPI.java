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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for modifying collection-level properties.
 *
 * <p>These APIs (PUT and DELETE /api/collections/collName/properties/propName) are analogous to the
 * v1 /admin/collections?action=COLLECTIONPROP command.
 */
@Path("/collections/{collName}/properties/{propName}")
public class CollectionPropertyAPI extends AdminAPIBase {

  public CollectionPropertyAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @PUT
  @PermissionName(COLL_EDIT_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  public SolrJerseyResponse createOrUpdateCollectionProperty(
      @PathParam("collName") String collName,
      @PathParam("propName") String propName,
      UpdateCollectionPropertyRequestBody requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    recordCollectionForLogAndTracing(collName, solrQueryRequest);
    modifyCollectionProperty(collName, propName, requestBody.value);
    return response;
  }

  @DELETE
  @PermissionName(COLL_EDIT_PERM)
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  public SolrJerseyResponse deleteCollectionProperty(
      @PathParam("collName") String collName, @PathParam("propName") String propName)
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

  public static class UpdateCollectionPropertyRequestBody implements JacksonReflectMapWriter {
    public UpdateCollectionPropertyRequestBody() {}

    public UpdateCollectionPropertyRequestBody(String value) {
      this.value = value;
    }

    @JsonProperty(required = true)
    public String value;
  }
}
