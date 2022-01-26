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
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.handler.admin.CoreAdminHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.TARGET_CORE;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

/**
 * V2 API for splitting a single core into multiple pieces
 *
 * The new API (POST /v2/cores/coreName {'split': {...}}) is equivalent to the v1
 * /admin/cores?action=split command.
 */
@EndPoint(
        path = {"/cores/{core}"},
        method = POST,
        permission = CORE_EDIT_PERM)
public class SplitCoreAPI {
    private static final String V2_SPLIT_CORE_CMD = "split";

    private final CoreAdminHandler coreHandler;

    public SplitCoreAPI(CoreAdminHandler coreHandler) {
        this.coreHandler = coreHandler;
    }

    @Command(name = V2_SPLIT_CORE_CMD)
    public void splitCore(PayloadObj<SplitCorePayload> obj) throws Exception {
        final SplitCorePayload v2Body = obj.get();
        final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
        v1Params.put(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.SPLIT.name().toLowerCase(Locale.ROOT));
        v1Params.put(CoreAdminParams.CORE, obj.getRequest().getPathTemplateValues().get(CoreAdminParams.CORE));

        if (! CollectionUtils.isEmpty(v2Body.path)) {
            v1Params.put(PATH, v2Body.path.toArray(new String[v2Body.path.size()]));
        }
        if (! CollectionUtils.isEmpty(v2Body.targetCore)) {
            v1Params.put(TARGET_CORE, v2Body.targetCore.toArray(new String[v2Body.targetCore.size()]));
        }

        if (v2Body.splitKey != null) {
            v1Params.put(SPLIT_KEY, v1Params.remove("splitKey"));
        }

        coreHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    public static class SplitCorePayload implements ReflectMapWriter {
        @JsonProperty
        public List<String> path;

        @JsonProperty
        public List<String> targetCore;

        @JsonProperty
        public String splitKey;

        @JsonProperty
        public String splitMethod;

        @JsonProperty
        public Boolean getRanges;

        @JsonProperty
        public String ranges;

        @JsonProperty
        public String async;
    }
}
