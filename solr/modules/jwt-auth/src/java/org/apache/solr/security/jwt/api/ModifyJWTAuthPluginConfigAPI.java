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

package org.apache.solr.security.jwt.api;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.security.PermissionNameProvider.Name.SECURITY_EDIT_PERM;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;

/** V2 API for modifying configuration for Solr's JWTAuthPlugin. */
@EndPoint(
    path = {"/cluster/security/authentication"},
    method = POST,
    permission = SECURITY_EDIT_PERM)
public class ModifyJWTAuthPluginConfigAPI {

  @Command(name = "set-property")
  public void setProperties(PayloadObj<JWTConfigurationPayload> propertyPayload) {
    // Method stub used only to produce v2 API spec; implementation/body empty.
    throw new IllegalStateException();
  }
}
