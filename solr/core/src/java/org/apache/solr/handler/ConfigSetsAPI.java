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

import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

/**
 * V2 API for creating, editing, or deleting configsets.
 */
public class ConfigSetsAPI {

    private final ConfigSetsHandler configSetsHandler;
    private final CoreContainer coreContainer;
    private final ConfigSetService configSetService;
    private final Optional<DistributedCollectionConfigSetCommandRunner>
            distributedCollectionConfigSetCommandRunner;

    public ConfigSetsAPI(ConfigSetsHandler configSetsHandler, CoreContainer coreContainer) {
        this.configSetsHandler = configSetsHandler;
        this.coreContainer = coreContainer;
        this.configSetService = coreContainer.getConfigSetService();
        this.distributedCollectionConfigSetCommandRunner = coreContainer.getDistributedCollectionCommandRunner();
    }

    @EndPoint(method = GET, path = "/cluster/configs", permission = CONFIG_READ_PERM)
    public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        final NamedList<Object> results = new NamedList<>();
        List<String> configSetsList = configSetService.listConfigs();
        results.add("configSets", configSetsList);
        SolrResponse response = new OverseerSolrResponse(results);
        rsp.getValues().addAll(response.getResponse());
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
