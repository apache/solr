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
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.REQUESTBUFFERUPDATES;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.RequestBufferUpdatesPayload;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.handler.admin.CoreAdminHandler;

/**
 * Internal V2 API used to start update-buffering on the specified core.
 *
 * <p>Only valid in SolrCloud mode. This API (POST /v2/cores/coreName {'request-buffer-updates':
 * {}}) is analogous to the v1 /admin/cores?action=REQUESTBUFFERUPDATES command.
 *
 * @see org.apache.solr.client.solrj.request.beans.RequestSyncShardPayload
 */
@EndPoint(
    path = {"/cores/{core}"},
    method = POST,
    permission = CORE_EDIT_PERM)
public class RequestBufferUpdatesAPI {
  public static final String V2_REQUEST_BUFFER_UPDATES_CMD = "request-buffer-updates";

  private final CoreAdminHandler coreAdminHandler;

  public RequestBufferUpdatesAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @Command(name = V2_REQUEST_BUFFER_UPDATES_CMD)
  public void requestBufferUpdates(PayloadObj<RequestBufferUpdatesPayload> obj) throws Exception {
    final RequestBufferUpdatesPayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, REQUESTBUFFERUPDATES.name().toLowerCase(Locale.ROOT));
    v1Params.put(CoreAdminParams.NAME, obj.getRequest().getPathTemplateValues().get("core"));

    coreAdminHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }
}
