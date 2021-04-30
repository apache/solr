package org.apache.solr.handler;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.BackupCollectionPayload;
import org.apache.solr.client.solrj.request.beans.ModifyCollectionPayload;
import org.apache.solr.client.solrj.request.beans.ReloadCollectionPayload;
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
        v1Params.put("collection.configName", v2Body.config);
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
  }

  private void flattenMapWithPrefix(Map<String, Object> toFlatten, Map<String, Object> destination,
                                    String additionalPrefix) {
    if (toFlatten == null || toFlatten.isEmpty() || destination == null) {
      return;
    }

    toFlatten.forEach((k, v) -> destination.put(additionalPrefix + k, v));
  }
}
