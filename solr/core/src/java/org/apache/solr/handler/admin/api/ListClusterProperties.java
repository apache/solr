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
import org.apache.solr.client.api.endpoint.ListClusterPropertiesApi;
import org.apache.solr.client.api.model.ListClusterPropertiesResponse;
import org.apache.solr.core.CoreContainer;

/**
 * V2 API for listing cluster properties.
 *
 * <p>This API (GET /api/cluster/properties) has no v1 equivalent.
 */
public class ListClusterProperties extends ClusterPropertiesAPIBase
    implements ListClusterPropertiesApi {

  @Inject
  public ListClusterProperties(CoreContainer coreContainer) {
    super(coreContainer.getZkController().getZkClient());
  }

  @Override
  public ListClusterPropertiesResponse listClusterProperties() {
    final var response = instantiateJerseyResponse(ListClusterPropertiesResponse.class);

    try {
      response.clusterProperties =
          new ArrayList<>(clusterProperties.getClusterProperties().keySet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return response;
  }
}
