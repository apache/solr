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
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import java.lang.invoke.MethodHandles;
import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.admin.SystemInfoHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API for getting "system" information from the receiving node.
 *
 * <p>This includes current resource utilization, information about the installation (location,
 * version, etc.), and JVM settings.
 *
 * <p>This API (GET /v2/node/system) is analogous to the v1 /admin/info/system.
 *
 * @deprecated Use the JAX-RS API: NodeSystemInfo (/node/info/system), implementing NodeSystemApi,
 *     and returning NodeSystemResponse.
 */
@Deprecated
public class NodeSystemInfoAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SystemInfoHandler handler;

  public NodeSystemInfoAPI(SystemInfoHandler handler) {
    this.handler = handler;
  }

  @EndPoint(
      path = {"/node/system"},
      method = GET,
      permission = CONFIG_READ_PERM)
  public void getSystemInformation(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    log.warn("Request to deprecated endpoint: /node/system.  Please use /node/info/system");
    handler.handleRequestBody(req, rsp);
  }
}
