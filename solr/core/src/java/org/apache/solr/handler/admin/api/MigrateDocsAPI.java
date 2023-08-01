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
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.MigrateDocsPayload;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

/**
 * V2 API for migrating docs from one collection to another.
 *
 * <p>The new API (POST /v2/collections/collectionName {'migrate-docs': {...}}) is analogous to the
 * v1 /admin/collections?action=MIGRATE command.
 *
 * @see MigrateDocsPayload
 */
@EndPoint(
    path = {"/c/{collection}", "/collections/{collection}"},
    method = POST,
    permission = COLL_EDIT_PERM)
public class MigrateDocsAPI {
  private static final String V2_MIGRATE_DOCS_CMD = "migrate-docs";

  private final CollectionsHandler collectionsHandler;

  public MigrateDocsAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @Command(name = V2_MIGRATE_DOCS_CMD)
  public void migrateDocs(PayloadObj<MigrateDocsPayload> obj) throws Exception {
    final MigrateDocsPayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, CollectionParams.CollectionAction.MIGRATE.toLower());
    v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

    if (v2Body.splitKey != null) {
      v1Params.remove("splitKey");
      v1Params.put("split.key", v2Body.splitKey);
    }
    if (v2Body.target != null) {
      v1Params.remove("target");
      v1Params.put("target.collection", v2Body.target);
    }
    if (v2Body.forwardTimeout != null) {
      v1Params.remove("forwardTimeout");
      v1Params.put("forward.timeout", v2Body.forwardTimeout);
    }

    collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }
}
