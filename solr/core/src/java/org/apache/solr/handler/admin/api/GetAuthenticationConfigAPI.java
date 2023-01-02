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
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_READ_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for fetching the authentication section from Solr's security.json configuration.
 *
 * <p>This API (GET /v2/cluster/security/authentication) is analogous to the v1 `GET
 * /solr/admin/authentication` API.
 */
public class GetAuthenticationConfigAPI {

  private final SecurityConfHandler securityConfHandler;

  public GetAuthenticationConfigAPI(SecurityConfHandler securityConfHandler) {
    this.securityConfHandler = securityConfHandler;
  }

  @EndPoint(
      path = {"/cluster/security/authentication"},
      method = GET,
      permission = SECURITY_READ_PERM)
  public void fetchAuthenticationConfig(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    securityConfHandler.handleRequestBody(req, rsp);
  }
}
