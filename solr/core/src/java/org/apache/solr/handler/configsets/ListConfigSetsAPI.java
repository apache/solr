/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.configsets;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import java.util.List;
import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for adding or updating a single file within a configset.
 *
 * <p>This API (GET /v2/cluster/configs) is analogous to the v1 /admin/configs?action=LIST command.
 */
public class ListConfigSetsAPI extends ConfigSetAPIBase {
  public ListConfigSetsAPI(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @EndPoint(method = GET, path = "/cluster/configs", permission = CONFIG_READ_PERM)
  public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final NamedList<Object> results = new NamedList<>();
    List<String> configSetsList = configSetService.listConfigs();
    results.add("configSets", configSetsList);
    SolrResponse response = new OverseerSolrResponse(results);
    rsp.getValues().addAll(response.getResponse());
  }
}
