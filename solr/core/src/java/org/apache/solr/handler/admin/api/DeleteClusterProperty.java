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

import jakarta.inject.Inject;
import java.io.IOException;
import org.apache.solr.client.api.endpoint.DeleteClusterPropertyApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;

/**
 * V2 API for deleting a cluster property.
 *
 * <p>This API (DELETE /api/cluster/properties/{propertyName} is equivalent to the v1 GET
 * /solr/admin/collections?action=CLUSTERPROP&amp;name={propertyName} API.
 */
public class DeleteClusterProperty extends ClusterPropertiesAPIBase
    implements DeleteClusterPropertyApi {

  @Inject
  public DeleteClusterProperty(CoreContainer coreContainer) {
    super(coreContainer.getZkController().getZkClient());
  }

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
