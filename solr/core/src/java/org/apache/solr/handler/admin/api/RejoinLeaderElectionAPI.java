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
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REJOINLEADERELECTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.RejoinLeaderElectionPayload;
import org.apache.solr.handler.admin.CoreAdminHandler;

/**
 * V2 API for triggering a core to rejoin leader election for the shard it constitutes.
 *
 * <p>This API (POST /v2/node {'rejoin-leader-election': {...}}) is analogous to the v1
 * /admin/cores?action=REJOINLEADERELECTION command.
 */
@EndPoint(
    path = {"/node"},
    method = POST,
    permission = CORE_EDIT_PERM)
public class RejoinLeaderElectionAPI {
  public static final String REJOIN_LEADER_ELECTION_CMD = "rejoin-leader-election";

  private final CoreAdminHandler coreAdminHandler;

  public RejoinLeaderElectionAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @Command(name = REJOIN_LEADER_ELECTION_CMD)
  public void rejoinLeaderElection(PayloadObj<RejoinLeaderElectionPayload> payload)
      throws Exception {
    final RejoinLeaderElectionPayload v2Body = payload.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, REJOINLEADERELECTION.name().toLowerCase(Locale.ROOT));
    if (v2Body.electionNode != null) {
      v1Params.remove("electionNode");
      v1Params.put(ELECTION_NODE_PROP, v2Body.electionNode);
    }
    if (v2Body.coreNodeName != null) {
      v1Params.remove("coreNodeName");
      v1Params.put(CORE_NODE_NAME_PROP, v2Body.coreNodeName);
    }

    coreAdminHandler.handleRequestBody(
        wrapParams(payload.getRequest(), v1Params), payload.getResponse());
  }
}
