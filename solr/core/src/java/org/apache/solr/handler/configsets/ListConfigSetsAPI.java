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

import org.apache.solr.api.JerseyResource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.lang.invoke.MethodHandles;

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

/**
 * V2 API for adding or updating a single file within a configset.
 *
 * <p>This API (GET /v2/cluster/configs) is analogous to the v1 /admin/configs?action=LIST command.
 */

@Path("/cluster/configs")
public class ListConfigSetsAPI extends JerseyResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;

  @Inject
  public ListConfigSetsAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @GET
  @Produces("application/json")
  @PermissionName(CONFIG_READ_PERM)
  public ListConfigsetsResponse listConfigSet() throws Exception {
    log.info("CoreContainer={}", coreContainer);
    final ListConfigsetsResponse response = new ListConfigsetsResponse();
    response.configSets = coreContainer.getConfigSetService().listConfigs();
    log.info("Finished building response, ready to return and deserialize");
    return response;
  }

}
