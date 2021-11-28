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

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.InvokeClassPayload;
import org.apache.solr.handler.admin.CoreAdminHandler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CoreAdminParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.CoreAdminAction.INVOKE;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

/**
 * V2 API for triggering "invocable" classes.
 *
 * This API (POST /v2/node {'invoke': {...}}) is analogous to the v1 /admin/cores?action=INVOKE command.
 */
@EndPoint(
        path = {"/node"},
        method = POST,
        permission = CORE_EDIT_PERM)
public class InvokeClassAPI {
    public static final String INVOKE_CMD = "invoke";

    private final CoreAdminHandler coreAdminHandler;

    public InvokeClassAPI(CoreAdminHandler coreAdminHandler) {
        this.coreAdminHandler = coreAdminHandler;
    }

    @Command(name = INVOKE_CMD)
    public void invokeClasses(PayloadObj<InvokeClassPayload> payload) throws Exception {
        final InvokeClassPayload v2Body = payload.get();
        final Map<String, Object> v1Params = new HashMap<>();
        v1Params.put(ACTION, INVOKE.name().toLowerCase(Locale.ROOT));
        v1Params.put("class", v2Body.classes);

        coreAdminHandler.handleRequestBody(wrapParams(payload.getRequest(), v1Params), payload.getResponse());
    }
}
