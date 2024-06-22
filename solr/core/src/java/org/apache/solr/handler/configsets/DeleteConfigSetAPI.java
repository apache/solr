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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for deleting an existing configset
 *
 * <p>This API (DELETE /v2/cluster/configs/configsetName) is analogous to the v1
 * /admin/configs?action=DELETE command.
 */
public class DeleteConfigSetAPI extends ConfigSetAPIBase {

  public static final String CONFIGSET_NAME_PLACEHOLDER = "name";

  public DeleteConfigSetAPI(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @EndPoint(
      method = DELETE,
      path = "/cluster/configs/{" + CONFIGSET_NAME_PLACEHOLDER + "}",
      permission = CONFIG_EDIT_PERM)
  public void deleteConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String configSetName = req.getPathTemplateValues().get("name");
    if (StrUtils.isNullOrEmpty(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "No configset name provided to delete");
    }
    final Map<String, Object> configsetCommandMsg = new HashMap<>();
    configsetCommandMsg.put(NAME, configSetName);

    runConfigSetCommand(rsp, ConfigSetParams.ConfigSetAction.DELETE, configsetCommandMsg);
  }
}
