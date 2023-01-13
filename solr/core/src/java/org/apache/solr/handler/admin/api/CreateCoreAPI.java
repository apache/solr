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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.CREATE;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.handler.api.V2ApiUtils.flattenToCommaDelimitedString;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.CreateCorePayload;
import org.apache.solr.handler.admin.CoreAdminHandler;

/**
 * V2 API for creating a new core on the receiving node.
 *
 * <p>This API (POST /v2/cores {'create': {...}}) is analogous to the v1 /admin/cores?action=CREATE
 * command.
 *
 * @see CreateCorePayload
 */
@EndPoint(
    path = {"/cores"},
    method = POST,
    permission = CORE_EDIT_PERM)
public class CreateCoreAPI {
  public static final String V2_CREATE_CORE_CMD = "create";

  private final CoreAdminHandler coreAdminHandler;

  public CreateCoreAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @Command(name = V2_CREATE_CORE_CMD)
  public void createCore(PayloadObj<CreateCorePayload> obj) throws Exception {
    final CreateCorePayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, CREATE.name().toLowerCase(Locale.ROOT));

    if (v2Body.isTransient != null) {
      v1Params.put("transient", v1Params.remove("isTransient"));
    }
    if (v2Body.properties != null) {
      v1Params.remove("properties");
      flattenMapWithPrefix(v2Body.properties, v1Params, PROPERTY_PREFIX);
    }
    if (v2Body.roles != null && !v2Body.roles.isEmpty()) {
      v1Params.remove("roles"); // Not strictly needed, since the method below would overwrite it.
      flattenToCommaDelimitedString(v1Params, v2Body.roles, "roles");
    }

    coreAdminHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }
}
