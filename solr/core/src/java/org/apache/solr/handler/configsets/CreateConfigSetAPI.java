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

package org.apache.solr.handler.configsets;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DISABLE_CREATE_AUTH_CHECKS;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.CreateConfigPayload;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.CoreContainer;

/**
 * V2 API for creating a new configset as a copy of an existing one.
 *
 * <p>This API (POST /v2/cluster/configs {"create": {...}}) is analogous to the v1
 * /admin/configs?action=CREATE command.
 */
@EndPoint(method = POST, path = "/cluster/configs", permission = CONFIG_EDIT_PERM)
public class CreateConfigSetAPI extends ConfigSetAPIBase {

  public CreateConfigSetAPI(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Command(name = "create")
  public void create(PayloadObj<CreateConfigPayload> obj) throws Exception {
    final CreateConfigPayload createConfigPayload = obj.get();
    if (configSetService.checkConfigExists(createConfigPayload.name)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "ConfigSet already exists: " + createConfigPayload.name);
    }

    // is there a base config that already exists
    if (!configSetService.checkConfigExists(createConfigPayload.baseConfigSet)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Base ConfigSet does not exist: " + createConfigPayload.baseConfigSet);
    }

    if (!DISABLE_CREATE_AUTH_CHECKS
        && !isTrusted(obj.getRequest().getUserPrincipal(), coreContainer.getAuthenticationPlugin())
        && configSetService.isConfigSetTrusted(createConfigPayload.baseConfigSet)) {
      throw new SolrException(
          SolrException.ErrorCode.UNAUTHORIZED,
          "Can't create a configset with an unauthenticated request from a trusted "
              + ConfigSetCmds.BASE_CONFIGSET);
    }

    final Map<String, Object> configsetCommandMsg = new HashMap<>();
    configsetCommandMsg.put(NAME, createConfigPayload.name);
    configsetCommandMsg.put(ConfigSetCmds.BASE_CONFIGSET, createConfigPayload.baseConfigSet);
    if (createConfigPayload.properties != null) {
      for (Map.Entry<String, Object> e : createConfigPayload.properties.entrySet()) {
        configsetCommandMsg.put(
            ConfigSetCmds.CONFIG_SET_PROPERTY_PREFIX + e.getKey(), e.getValue());
      }
    }

    runConfigSetCommand(
        obj.getResponse(), ConfigSetParams.ConfigSetAction.CREATE, configsetCommandMsg);
  }
}
