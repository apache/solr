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

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for retrieving some or all configuration relevant to a particular collection (or core).
 *
 * <p>This class covers a handful of distinct paths under GET /v2/c/collectionName/config: /,
 * /overlay, /params, /znodeVersion, etc.
 */
public class GetConfigAPI {
  private final SolrConfigHandler configHandler;

  public GetConfigAPI(SolrConfigHandler configHandler) {
    this.configHandler = configHandler;
  }

  @EndPoint(
      path = {"/config"},
      method = GET,
      permission = CONFIG_READ_PERM)
  public void getAllConfig(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }

  @EndPoint(
      path = {"/config/params"},
      method = GET,
      permission = CONFIG_READ_PERM)
  public void getAllParamSets(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }

  @EndPoint(
      path = {"/config/params/{paramset}"},
      method = GET,
      permission = CONFIG_READ_PERM)
  public void getSingleParamSet(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }

  // This endpoint currently covers a whole list of paths by using the "component" placeholder
  @EndPoint(
      path = {"/config/{component}"},
      method = GET,
      permission = CONFIG_READ_PERM)
  public void getComponentConfig(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }
}
