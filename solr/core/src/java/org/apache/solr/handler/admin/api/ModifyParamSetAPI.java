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
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import java.lang.invoke.MethodHandles;
import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 APIs for creating, modifying, or deleting paramsets.
 *
 * <p>This API (POST /v2/collections/collectionName/config/params {...}) is analogous to the
 * commands supported by the v1 /techproducts/config/params
 *
 * <p>Typically v2 "POST" API implementations separate each "command" into a different method. This
 * is not done here because the v1 API code in SolrConfigHandler expects to consume the request body
 * itself (meaning that nothing _before_ SolrConfigHandler can consume the request body).
 *
 * <p>As a result the single method below handles all three "commands" supported by the POST
 * /config/params API: 'set', 'delete', and 'update'.
 */
public class ModifyParamSetAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrConfigHandler configHandler;

  public ModifyParamSetAPI(SolrConfigHandler configHandler) {
    this.configHandler = configHandler;
  }

  @EndPoint(
      path = {"/config/params"},
      method = POST,
      permission = CONFIG_EDIT_PERM)
  public void modifyParamSets(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }
}
