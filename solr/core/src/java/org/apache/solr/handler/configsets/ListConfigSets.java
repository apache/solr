/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.configsets;

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListConfigsetsApi;
import org.apache.solr.client.api.model.ListConfigsetsResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;

/**
 * V2 API implementation for listing all available configsets.
 *
 * <p>This API (GET /v2/cluster/configs) is analogous to the v1 /admin/configs?action=LIST command.
 */
public class ListConfigSets extends JerseyResource implements ListConfigsetsApi {

  @Context public HttpHeaders headers;

  private final CoreContainer coreContainer;

  @Inject
  public ListConfigSets(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ListConfigsetsResponse listConfigSet() throws Exception {
    final ListConfigsetsResponse response = instantiateJerseyResponse(ListConfigsetsResponse.class);
    response.configSets = coreContainer.getConfigSetService().listConfigs();
    return response;
  }
}
