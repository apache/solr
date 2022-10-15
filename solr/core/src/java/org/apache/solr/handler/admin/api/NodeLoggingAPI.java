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
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.admin.LoggingHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for getting or setting log levels on an individual node.
 *
 * <p>This API (GET /v2/node/logging) is analogous to the v1 /admin/info/logging.
 */
public class NodeLoggingAPI {

  private final LoggingHandler handler;

  public NodeLoggingAPI(LoggingHandler handler) {
    this.handler = handler;
  }

  // TODO See SOLR-15823 for background on the (less than ideal) permission chosen here.
  @EndPoint(
      path = {"/node/logging"},
      method = GET,
      permission = CONFIG_EDIT_PERM)
  public void getOrSetLogLevels(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    handler.handleRequestBody(req, rsp);
  }
}
