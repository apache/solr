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
import static org.apache.solr.common.params.CollectionParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.SHARD;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.SyncShardPayload;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

/**
 * V2 API for triggering a shard-sync operation within a particular collection and shard.
 *
 * <p>This API (POST /v2/collections/collectionName/shards/shardName {'sync-shard': {}}) is
 * analogous to the v1 /admin/collections?action=SYNCSHARD command.
 *
 * @see SyncShardPayload
 */
@EndPoint(
    path = {"/c/{collection}/shards/{shard}", "/collections/{collection}/shards/{shard}"},
    method = POST,
    permission = COLL_EDIT_PERM)
public class SyncShardAPI {
  private static final String V2_SYNC_SHARD_CMD = "sync-shard";

  private final CollectionsHandler collectionsHandler;

  public SyncShardAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @Command(name = V2_SYNC_SHARD_CMD)
  public void syncShard(PayloadObj<SyncShardPayload> obj) throws Exception {
    final Map<String, Object> addedV1Params = Maps.newHashMap();
    final Map<String, String> pathParams = obj.getRequest().getPathTemplateValues();
    addedV1Params.put(ACTION, CollectionParams.CollectionAction.SYNCSHARD.toLower());
    addedV1Params.put(COLLECTION, pathParams.get(COLLECTION));
    addedV1Params.put(SHARD, pathParams.get(SHARD));

    collectionsHandler.handleRequestBody(
        wrapParams(obj.getRequest(), addedV1Params), obj.getResponse());
  }
}
