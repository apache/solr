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
package org.apache.solr.handler;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.AddReplicaPropertyPayload;
import org.apache.solr.client.solrj.request.beans.BackupCollectionPayload;
import org.apache.solr.client.solrj.request.beans.BalanceShardUniquePayload;
import org.apache.solr.client.solrj.request.beans.DeleteReplicaPropertyPayload;
import org.apache.solr.client.solrj.request.beans.MigrateDocsPayload;
import org.apache.solr.client.solrj.request.beans.ModifyCollectionPayload;
import org.apache.solr.client.solrj.request.beans.MoveReplicaPayload;
import org.apache.solr.client.solrj.request.beans.RebalanceLeadersPayload;
import org.apache.solr.client.solrj.request.beans.ReloadCollectionPayload;
import org.apache.solr.client.solrj.request.beans.SetCollectionPropertyPayload;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;


// TODO This classname sucks - I'm trying to convey that the APIs in this class all operate on a single specific
//  collection (i.e. they're for the /v2/collections/<coll> path).  But the name is a clunky way to do that.
/**
 * All V2 APIs for the /v2/collections/{collName} path
 */
public class SpecificCollectionAPI {

  private static final String V2_MODIFY_COLLECTION_CMD = "modify";
  private static final String V2_RELOAD_COLLECTION_CMD = "reload";
  private static final String V2_MOVE_REPLICA_CMD = "move-replica";
  private static final String V2_MIGRATE_DOCS_CMD = "migrate-docs";
  private static final String V2_BALANCE_SHARD_UNIQUE_CMD = "balance-shard-unique";
  private static final String V2_REBALANCE_LEADERS_CMD = "rebalance-leaders";
  private static final String V2_ADD_REPLICA_PROPERTY_CMD = "add-replica-property";
  private static final String V2_DELETE_REPLICA_PROPERTY_CMD = "delete-replica-property";
  private static final String V2_SET_COLLECTION_PROPERTY_CMD = "set-collection-property";

  public final SpecificCollectionCommands specificCollectionCommands = new SpecificCollectionCommands();
  private final CollectionsHandler collectionsHandler;

  public SpecificCollectionAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @EndPoint(path = {"/c/{collection}", "/collections/{collection}"},
          method = DELETE,
          permission = COLL_EDIT_PERM)
  public void deleteCollection(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req, ACTION,
            CollectionParams.CollectionAction.DELETE.toString(),
            NAME, req.getPathTemplateValues().get(ZkStateReader.COLLECTION_PROP));
    collectionsHandler.handleRequestBody(req, rsp);
  }

  // TODO What is the purpose of nesting all the POSTs under a separate class in CollectionsAPI.CollectionCommands?
  //    - try to add the annotation and stuff to the top-level SpecificCollectionAPI once I get everything working in the mirrored, nested fashion
  @EndPoint(
          path = {"/c/{collection}", "/collections/{collection}"},
          method = POST,
          permission = COLL_EDIT_PERM) // TODO is this the correct permission for all POST commands at this path
  public class SpecificCollectionCommands {

    @Command(name = V2_MODIFY_COLLECTION_CMD)
    public void modifyCollection(PayloadObj<ModifyCollectionPayload> obj) throws Exception {
      final ModifyCollectionPayload v2Body = obj.get();

      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));
      if (v2Body.config != null) {
        v1Params.remove("config");
        v1Params.put(COLL_CONF, v2Body.config);
      }
      if (v2Body.properties != null && !v2Body.properties.isEmpty()) {
        v1Params.remove("properties");
        flattenMapWithPrefix(v2Body.properties, v1Params, "property.");
      }

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_RELOAD_COLLECTION_CMD)
    public void reloadCollection(PayloadObj<ReloadCollectionPayload> obj) throws Exception {
      final ReloadCollectionPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.RELOAD.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_MOVE_REPLICA_CMD)
    public void moveReplica(PayloadObj<MoveReplicaPayload> obj) throws Exception {
      final MoveReplicaPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
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

    @Command(name = V2_BALANCE_SHARD_UNIQUE_CMD)
    public void balanceShardUnique(PayloadObj<BalanceShardUniquePayload> obj) throws Exception {
      final BalanceShardUniquePayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_REBALANCE_LEADERS_CMD)
    public void rebalanceLeaders(PayloadObj<RebalanceLeadersPayload> obj) throws Exception {
      final RebalanceLeadersPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.REBALANCELEADERS.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_ADD_REPLICA_PROPERTY_CMD)
    public void addReplicaProperty(PayloadObj<AddReplicaPropertyPayload> obj) throws Exception {
      final AddReplicaPropertyPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.ADDREPLICAPROP.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));
      v1Params.put("property", v1Params.remove("name"));
      v1Params.put("property.value", v1Params.remove("value"));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_DELETE_REPLICA_PROPERTY_CMD)
    public void deleteReplicaProperty(PayloadObj<DeleteReplicaPropertyPayload> obj) throws Exception {
      final DeleteReplicaPropertyPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
      v1Params.put(ACTION, CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower());
      v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }

    @Command(name = V2_SET_COLLECTION_PROPERTY_CMD)
    public void setCollectionProperty(PayloadObj<SetCollectionPropertyPayload> obj) throws Exception {
      final SetCollectionPropertyPayload v2Body = obj.get();
      final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

      v1Params.put("propertyName", v1Params.remove("name"));
      if (v2Body.value != null) {
        v1Params.put("propertyValue", v2Body.value);
      }
      v1Params.put(ACTION, CollectionParams.CollectionAction.COLLECTIONPROP.toLower());
      v1Params.put(NAME, obj.getRequest().getPathTemplateValues().get(COLLECTION));

      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }
  }

  private void flattenMapWithPrefix(Map<String, Object> toFlatten, Map<String, Object> destination,
                                    String additionalPrefix) {
    if (toFlatten == null || toFlatten.isEmpty() || destination == null) {
      return;
    }

    toFlatten.forEach((k, v) -> destination.put(additionalPrefix + k, v));
  }
}
