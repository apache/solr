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

package org.apache.solr.handler;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.CreateConfigPayload;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

/**
 * V2 API for creating, editing, or deleting configsets.
 */
public class ConfigSetsAPI {

    private final ConfigSetsHandler configSetsHandler;
    public final ConfigSetCommands createConfigset = new ConfigSetCommands();

    public ConfigSetsAPI(ConfigSetsHandler configSetsHandler) {
        this.configSetsHandler = configSetsHandler;
    }

    @EndPoint(method = POST, path = "/cluster/configs", permission = CONFIG_EDIT_PERM)
    public class ConfigSetCommands {

        @Command(name = "create")
        @SuppressWarnings("unchecked")
        public void create(PayloadObj<CreateConfigPayload> obj) throws Exception {
            Map<String, Object> mapVals = obj.get().toMap(new HashMap<>());
            Map<String, Object> customProps = obj.get().properties;
            if (customProps != null) {
                customProps.forEach((k, o) -> mapVals.put(ConfigSetCmds.CONFIG_SET_PROPERTY_PREFIX + k, o));
            }
            mapVals.put("action", ConfigSetParams.ConfigSetAction.CREATE.toString());
            configSetsHandler.handleRequestBody(wrapParams(obj.getRequest(), mapVals), obj.getResponse());
        }
    }

    @EndPoint(method = PUT, path = "/cluster/configs/{name}", permission = CONFIG_EDIT_PERM)
    public void uploadConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req =
                wrapParams(
                        req,
                        "action",
                        ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
                        CommonParams.NAME,
                        req.getPathTemplateValues().get("name"),
                        ConfigSetParams.OVERWRITE,
                        true,
                        ConfigSetParams.CLEANUP,
                        false);
        configSetsHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = PUT, path = "/cluster/configs/{name}/*", permission = CONFIG_EDIT_PERM)
    public void insertIntoConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        String path = req.getPathTemplateValues().get("*");
        if (path == null || path.isBlank()) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "In order to insert a file in a configSet, a filePath must be provided in the url after the name of the configSet.");
        }
        req =
                wrapParams(
                        req,
                        Map.of(
                                "action",
                                ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
                                CommonParams.NAME,
                                req.getPathTemplateValues().get("name"),
                                ConfigSetParams.FILE_PATH,
                                path,
                                ConfigSetParams.OVERWRITE,
                                true,
                                ConfigSetParams.CLEANUP,
                                false));
        configSetsHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = GET, path = "/cluster/configs", permission = CONFIG_READ_PERM)
    public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req = wrapParams(req, "action", ConfigSetParams.ConfigSetAction.LIST.toString());
        configSetsHandler.handleRequestBody(req, rsp);
    }

    @EndPoint(method = DELETE, path = "/cluster/configs/{name}", permission = CONFIG_EDIT_PERM)
    public void deleteConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        req =
                wrapParams(
                        req,
                        "action",
                        ConfigSetParams.ConfigSetAction.DELETE.toString(),
                        CommonParams.NAME,
                        req.getPathTemplateValues().get("name"));
        configSetsHandler.handleRequestBody(req, rsp);
    }

    public static SolrQueryRequest wrapParams(SolrQueryRequest req, Object... def) {
        Map<String, Object> m = Utils.makeMap(def);
        return wrapParams(req, m);
    }

    public static SolrQueryRequest wrapParams(SolrQueryRequest req, Map<String, Object> m) {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        m.forEach(
                (k, v) -> {
                    if (v == null) return;
                    if (v instanceof String[]) {
                        solrParams.add(k, (String[]) v);
                    } else {
                        solrParams.add(k, String.valueOf(v));
                    }
                });
        DefaultSolrParams dsp = new DefaultSolrParams(req.getParams(), solrParams);
        req.setParams(dsp);
        return req;
    }
}
