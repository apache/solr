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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.common.params.CollectionParams.ACTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for checking the status of a core-level asynchronous command.
 *
 * <p>This API (GET /v2/cores/command-status/someId is analogous to the v1
 * /admin/cores?action=REQUESTSTATUS command. It is not to be confused with the more robust
 * asynchronous command support offered under the v2 `/cluster/command-status` path (or the
 * corresponding v1 path `/solr/admin/collections?action=REQUESTSTATUS`). Async support at the core
 * level differs in that command IDs are local to individual Solr nodes and are not persisted across
 * restarts.
 *
 * @see org.apache.solr.client.solrj.request.beans.RequestSyncShardPayload
 */
public class RequestCoreCommandStatusAPI {

  private final CoreAdminHandler coreAdminHandler;

  public RequestCoreCommandStatusAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @EndPoint(method = GET, path = "/cores/{core}/command-status/{id}", permission = CORE_READ_PERM)
  public void getCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final Map<String, Object> v1Params = new HashMap<>();
    v1Params.put(
        ACTION, CoreAdminParams.CoreAdminAction.REQUESTSTATUS.name().toLowerCase(Locale.ROOT));
    v1Params.put(CoreAdminParams.REQUESTID, req.getPathTemplateValues().get("id"));
    coreAdminHandler.handleRequestBody(wrapParams(req, v1Params), rsp);
  }
}
