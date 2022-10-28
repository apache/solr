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

import java.util.List;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.security.MultiAuthPlugin;

/** V2 API to modify configuration options for the {@link MultiAuthPlugin}. */
@EndPoint(
    path = {"/cluster/security/authentication"},
    method = POST,
    permission = SECURITY_EDIT_PERM)
public class ModifyMultiPluginAuthConfigAPI {

  @Command(name = "set-user")
  public void createUsers(PayloadObj<Map<String, String>> usersToCreatePayload) {
    // Method stub used only to produce v2 API spec; implementation/body empty.
    throw new IllegalStateException();
  }

  @Command(name = "delete-user")
  public void deleteUsers(PayloadObj<List<String>> usersToDeletePayload) {
    // Method stub used only to produce v2 API spec; implementation/body empty.
    throw new IllegalStateException();
  }

  // TODO This is a good candidate to create an actual Payload class instead of relying on the
  // generic Map, if someone understands what syntax this API actually takes.  The apispec
  // this code was mirrored from is vague, and the ref-guide has no examples afaict.
  @Command(name = "set-property")
  public void setProperties(PayloadObj<Map<String, Object>> propertyPayload) {
    // Method stub used only to produce v2 API spec; implementation/body empty.
    throw new IllegalStateException();
  }
}
