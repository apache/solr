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

import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.PermissionNameProvider;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;

/**
 * V2 API for checking the name of an in-use schema.
 *
 * <p>This API (GET /v2/collections/collectionName/schema/name) is analogous to the v1
 * /solr/collectionName/schema/name API.
 */
public class SchemaNameAPI {

  @EndPoint(
      path = {"/schema/name"},
      method = GET,
      permission = PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public void getSchemaName(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String schemaName = req.getSchema().getSchemaName();
    if (null == schemaName) {
      rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "Schema has no name"));
    }
    rsp.add(IndexSchema.NAME, schemaName);
  }
}
