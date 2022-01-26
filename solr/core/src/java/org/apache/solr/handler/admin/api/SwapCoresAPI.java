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
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.handler.admin.CoreAdminHandler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

/**
 * V2 API for swapping two existing Solr cores.
 *
 * Not intended for use in SolrCloud mode.
 *
 * The new API (POST /v2/cores/coreName {'swap': {...}}) is equivalent to the v1
 * /admin/cores?action=swap command.
 */
@EndPoint(
        path = {"/cores/{core}"},
        method = POST,
        permission = CORE_EDIT_PERM)
public class SwapCoresAPI {
    private static final String V2_SWAP_CORES_CMD = "swap";

    private final CoreAdminHandler coreHandler;

    public SwapCoresAPI(CoreAdminHandler coreHandler) {
        this.coreHandler = coreHandler;
    }

    @Command(name = V2_SWAP_CORES_CMD)
    public void swapCores(PayloadObj<SwapCoresPayload> obj) throws Exception {
        final SwapCoresPayload v2Body = obj.get();
        final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
        v1Params.put(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.SWAP.name().toLowerCase(Locale.ROOT));
        v1Params.put(CoreAdminParams.CORE, obj.getRequest().getPathTemplateValues().get(CoreAdminParams.CORE));

        // V1 API uses 'other' instead of 'with' to represent the second/replacement core.
        v1Params.put(CoreAdminParams.OTHER, v1Params.remove("with"));

        coreHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    public static class SwapCoresPayload implements ReflectMapWriter {
        @JsonProperty(required = true)
        public String with;

        @JsonProperty
        public String async;
    }
}
