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
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.STATUS;
import static org.apache.solr.common.params.CoreAdminParams.CORE;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for checking the status of a specific core.
 *
 * <p>This API (GET /v2/cores/coreName is analogous to the v1
 * /admin/cores?action=status&amp;core=coreName command.
 *
 * @see AllCoresStatusAPI
 */
public class SingleCoreStatusAPI {

  private final CoreAdminHandler coreAdminHandler;

  public SingleCoreStatusAPI(CoreAdminHandler coreAdminHandler) {
    this.coreAdminHandler = coreAdminHandler;
  }

  @EndPoint(
      path = {"/cores/{core}"},
      method = GET,
      permission = CORE_READ_PERM)
  public void getStatusOfSingleCore(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req, ACTION, STATUS, CORE, req.getPathTemplateValues().get(CORE));
    coreAdminHandler.handleRequestBody(req, rsp);
  }
}
