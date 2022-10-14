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

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 APIs for creating, updating, or deleting individual components in a collection's schema.
 *
 * <p>This API (POST /v2/collections/collectionName/schema {...}) is analogous to the commands
 * supported by the v1 POST /collectionName/schema API.
 *
 * <p>Typically v2 "POST" API implementations separate each "command" into a different method. This
 * is not done here because the v1 API code in SchemaHandler expects to consume the request body
 * itself (meaning that nothing _before_ SchemaHandler can consume the request body).
 *
 * <p>As a result the single method below handles all "commands" supported by the POST /schema API:
 * 'add-field', 'delete-field', 'replace-field', 'add-dynamic-field', 'delete-dynamic-field',
 * 'replace-dynamic-field', 'add-field-type', 'delete-field-type', 'replace-field-type',
 * 'add-copy-field', and 'delete-copy-field'.
 */
public class SchemaBulkModifyAPI {
  private final SchemaHandler schemaHandler;

  public SchemaBulkModifyAPI(SchemaHandler schemaHandler) {
    this.schemaHandler = schemaHandler;
  }

  @EndPoint(
      path = {"/schema"},
      method = POST,
      permission = PermissionNameProvider.Name.SCHEMA_EDIT_PERM)
  public void bulkModifySchema(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    schemaHandler.handleRequestBody(req, rsp);
  }
}
