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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_EDIT_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for POST requests received when no authorization plugin is active.
 *
 * <p>Solr's security APIs only supports authz config modifications once an Authorization plugin is
 * in place. So this API serves solely as a placeholder that allows {@link SecurityConfHandler} to
 * return a helpful error message (instead of the opaque 404 that users would get without this API).
 */
public class ModifyNoAuthzPluginSecurityConfigAPI {
  private final SecurityConfHandler securityConfHandler;

  public ModifyNoAuthzPluginSecurityConfigAPI(SecurityConfHandler securityConfHandler) {
    this.securityConfHandler = securityConfHandler;
  }

  @EndPoint(
      path = {"/cluster/security/authorization"},
      method = POST,
      permission = SECURITY_EDIT_PERM)
  public void updateAuthorizationConfig(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    securityConfHandler.handleRequestBody(req, rsp);
  }
}
