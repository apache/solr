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
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.OverseerOperationPayload;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;

/**
 * V2 API for triggering a node to rejoin leader election for the 'overseer' role.
 *
 * <p>This API (POST /v2/node {'overseer-op': {...}}) is analogous to the v1
 * /admin/cores?action=overseerop command.
 *
 * @see OverseerOperationPayload
 */
@EndPoint(
    path = {"/node"},
    method = POST,
    permission = CORE_EDIT_PERM)
public class OverseerOperationAPI {

  // TODO rename this command, this API doesn't really have anything to do with overseer-ops, its
  // about leader election
  public static final String OVERSEER_OP_CMD = "overseer-op";

  private final CoreAdminHandler coreAdminHandler;

  public OverseerOperationAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @Command(name = OVERSEER_OP_CMD)
  public void joinOverseerLeaderElection(PayloadObj<OverseerOperationPayload> payload)
      throws Exception {
    final Map<String, Object> v1Params = payload.get().toMap(new HashMap<>());
    v1Params.put(
        ACTION, CoreAdminParams.CoreAdminAction.OVERSEEROP.name().toLowerCase(Locale.ROOT));
    coreAdminHandler.handleRequestBody(
        wrapParams(payload.getRequest(), v1Params), payload.getResponse());
  }
}
