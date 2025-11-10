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
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ClusterPropertyApis;
import org.apache.solr.client.api.model.ClusterPropertyDetails;
import org.apache.solr.client.api.model.GetClusterPropertyResponse;
import org.apache.solr.client.api.model.ListClusterPropertiesResponse;
import org.apache.solr.client.api.model.SetClusterPropertyRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class ClusterProperty extends AdminAPIBase implements ClusterPropertyApis {
  protected final ClusterProperties clusterProperties;

  @Inject
  public ClusterProperty(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
    this.clusterProperties =
        new ClusterProperties(
            fetchAndValidateZooKeeperAwareCoreContainer().getZkController().getZkClient());
  }

  /**
   * V2 API for listing cluster properties.
   *
   * <p>This API (GET /api/cluster/properties) has no v1 equivalent.
   */
  @Override
  @PermissionName(COLL_READ_PERM)
  public ListClusterPropertiesResponse listClusterProperties() {
    ListClusterPropertiesResponse response =
        instantiateJerseyResponse(ListClusterPropertiesResponse.class);

    try {
      response.clusterProperties =
          new ArrayList<>(clusterProperties.getClusterProperties().keySet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return response;
  }

  /**
   * V2 API for returning the value of a cluster property.
   *
   * <p>This API (GET /api/cluster/properties/{propertyName}) has no v1 equivalent.
   */
  @Override
  @PermissionName(COLL_READ_PERM)
  public SolrJerseyResponse getClusterProperty(String propertyName) {
    GetClusterPropertyResponse response =
        instantiateJerseyResponse(GetClusterPropertyResponse.class);

    try {
      Object value = clusterProperties.getClusterProperties().get(propertyName);
      if (value != null) {
        response.clusterProperty = new ClusterPropertyDetails();
        response.clusterProperty.name = propertyName;
        response.clusterProperty.value = value;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND, "No such cluster property [" + propertyName + "]");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return response;
  }

  /**
   * V2 API for setting the value of a single new or existing cluster property.
   *
   * <p>This API (PUT /api/cluster/properties/{propertyName} with an object listing the value) is
   * equivalent to the v1 GET
   * /solr/admin/collections?action=CLUSTERPROP&amp;name={propertyName}&amp;val={propertyValue} API.
   */
  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createOrUpdateClusterProperty(
      String propertyName, SetClusterPropertyRequestBody requestBody) throws IOException {
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    clusterProperties.setClusterProperty(propertyName, requestBody.value);
    return response;
  }

  /**
   * V2 API for setting the value of nested cluster properties.
   *
   * <p>This API (PUT /api/cluster/properties with an object listing those properties) has no v1
   * equivalent.
   */
  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createOrUpdateNestedClusterProperty(
      Map<String, Object> propertyValuesByName) {
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    try {
      clusterProperties.setClusterProperties(propertyValuesByName);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
    }
    return response;
  }

  /**
   * V2 API for deleting a cluster property.
   *
   * <p>This API (DELETE /api/cluster/properties/{propertyName}) is equivalent to the v1 GET
   * /solr/admin/collections?action=CLUSTERPROP&amp;name={propertyName} API.
   */
  @PermissionName(COLL_EDIT_PERM)
  @Override
  public SolrJerseyResponse deleteClusterProperty(String propertyName) {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);

    try {
      clusterProperties.setClusterProperty(propertyName, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return response;
  }
}
