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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.handler.admin.CoreAdminHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

/**
 * V2 API for merging one or more Solr cores into the target core.
 *
 * The new API (POST /v2/cores/coreName {'merge-indexes': {...}}) is equivalent to the v1
 * /admin/cores?action=mergeindexes command.
 */
@EndPoint(
        path = {"/cores/{core}"},
        method = POST,
        permission = CORE_EDIT_PERM)
public class MergeIndexesAPI {
    private static final String V2_MERGE_INDEXES_CORE_CMD = "merge-indexes";

    private final CoreAdminHandler coreHandler;

    public MergeIndexesAPI(CoreAdminHandler coreHandler) {
        this.coreHandler = coreHandler;
    }

    @Command(name = V2_MERGE_INDEXES_CORE_CMD)
    public void mergeIndexesIntoCore(PayloadObj<MergeIndexesPayload> obj) throws Exception {
        final MergeIndexesPayload v2Body = obj.get();
        final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
        v1Params.put(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.MERGEINDEXES.name().toLowerCase(Locale.ROOT));
        v1Params.put(CoreAdminParams.CORE, obj.getRequest().getPathTemplateValues().get(CoreAdminParams.CORE));
        if (! CollectionUtils.isEmpty(v2Body.indexDir)) {
            v1Params.put("indexDir", v2Body.indexDir.toArray(new String[v2Body.indexDir.size()]));
        }
        if (! CollectionUtils.isEmpty(v2Body.srcCore)) {
            v1Params.put("srcCore", v2Body.srcCore.toArray(new String[v2Body.srcCore.size()]));
        }
        // V1 API uses 'update.chain' instead of 'updateChain'.
        if (v2Body.updateChain != null) {
            v1Params.put(UpdateParams.UPDATE_CHAIN, v1Params.remove("updateChain"));
        }

        coreHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    public static class MergeIndexesPayload implements ReflectMapWriter {
        @JsonProperty
        public List<String> indexDir;

        @JsonProperty
        public List<String> srcCore;

        @JsonProperty
        public String updateChain;

        @JsonProperty
        public String async;
    }
}
