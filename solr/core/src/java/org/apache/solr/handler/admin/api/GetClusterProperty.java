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
import org.apache.solr.client.api.endpoint.GetClusterPropertyApi;
import org.apache.solr.client.api.model.ClusterPropertyDetails;
import org.apache.solr.client.api.model.GetClusterPropertyResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;

/**
 * V2 API for returning the value of a cluster property.
 *
 * <p>This API (GET /api/cluster/properties/{propertyName}) has no v1 equivalent.
 */
public class GetClusterProperty extends ClusterPropertiesAPIBase implements GetClusterPropertyApi {

  @Inject
  public GetClusterProperty(CoreContainer coreContainer) {
    super(coreContainer.getZkController().getZkClient());
  }

  @Override
  public SolrJerseyResponse getClusterProperty(String propertyName) {
    final var response = instantiateJerseyResponse(GetClusterPropertyResponse.class);

    try {
      Object value = clusterProperties.getClusterProperties().get(propertyName);
      if (value != null) {
        response.clusterProperty = new ClusterPropertyDetails();
        response.clusterProperty.name = propertyName;
        response.clusterProperty.value = value;
      } else {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "No such cluster property [" + propertyName + "]");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return response;
  }
}
