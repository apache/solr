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
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CORE;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTRECOVERY;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.RequestCoreRecoveryPayload;
import org.apache.solr.handler.admin.CoreAdminHandler;

/**
 * Internal V2 API triggering recovery on a core.
 *
 * <p>Only valid in SolrCloud mode. This API (POST /v2/cores/coreName {'request-recovery': {}}) is
 * analogous to the v1 /admin/cores?action=REQUESTRECOVERY command.
 *
 * @see RequestCoreRecoveryPayload
 */
@EndPoint(
    path = {"/cores/{core}"},
    method = POST,
    permission = CORE_EDIT_PERM)
public class RequestCoreRecoveryAPI {
  public static final String V2_REQUEST_RECOVERY_CMD = "request-recovery";

  private final CoreAdminHandler coreAdminHandler;

  public RequestCoreRecoveryAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @Command(name = V2_REQUEST_RECOVERY_CMD)
  public void requestCoreRecovery(PayloadObj<RequestCoreRecoveryPayload> obj) throws Exception {
    final RequestCoreRecoveryPayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, REQUESTRECOVERY.name().toLowerCase(Locale.ROOT));
    v1Params.put(CORE, obj.getRequest().getPathTemplateValues().get("core"));

    coreAdminHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }
}
