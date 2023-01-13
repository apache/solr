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

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for creating, updating, or deleting individual components in a collection configuration.
 *
 * <p>This API (POST /v2/collections/collectionName/config {...}) is analogous to the commands
 * supported by the v1 POST /collectionName/config API.
 *
 * <p>Typically v2 "POST" API implementations separate each "command" into a different method. This
 * is not done here because the v1 API code in SolrConfigHandler expects to consume the request body
 * itself (meaning that nothing _before_ SolrConfigHandler can consume the request body).
 *
 * <p>As a result the single method below handles all "commands" supported by the POST /config API:
 * 'set-property, 'unset-property', 'add-requesthandler', 'update-requesthandler',
 * 'delete-requesthandler', 'add-searchcomponent', 'update-searchcomponent',
 * 'delete-searchcomponent', 'add-initparams', 'update-initparams', 'delete-initparams',
 * 'add-queryresponsewriter', 'update-queryresponsewriter', 'delete-queryresponsewriter',
 * 'add-queryparser', 'update-queryparser', 'delete-queryparser', 'add-valuesourceparser',
 * 'update-valuesourceparser', 'delete-valuesourceparser', 'add-transformer', 'update-transformer',
 * 'delete-transformer', 'add-updateprocessor', 'update-updateprocessor', 'delete-updateprocessor',
 * 'add-queryconverter', 'update-queryconverter', 'delete-queryconverter', 'add-listener',
 * 'update-listener', and 'delete-listener'.
 */
public class ModifyConfigComponentAPI {
  private final SolrConfigHandler configHandler;

  public ModifyConfigComponentAPI(SolrConfigHandler configHandler) {
    this.configHandler = configHandler;
  }

  @EndPoint(
      path = {"/config"},
      method = POST,
      permission = CONFIG_EDIT_PERM)
  public void modifyConfigComponent(SolrQueryRequest req, SolrQueryResponse rsp) {
    configHandler.handleRequest(req, rsp);
  }
}
