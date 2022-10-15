package org.apache.solr.handler.admin.api;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.RenameClusterPayload;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

@EndPoint(
        path = {"/cluster/rename/{cluster}"},
        method = POST,
        permission = COLL_EDIT_PERM)
public class RenameClusterAPI {
    private static final String V2_RENAME_CLUSTER_CMD = "rename";

    private final CollectionsHandler collectionsHandler;


    public RenameClusterAPI(CollectionsHandler collectionsHandler) {
        this.collectionsHandler = collectionsHandler;
    }

    @Command(name = V2_RENAME_CLUSTER_CMD)
    public void renameCluster(PayloadObj<RenameClusterPayload> obj) throws Exception {
       final RenameClusterPayload v2Body = obj.get();
        final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

        v1Params.put(
                CollectionParams.ACTION, CollectionParams.CollectionAction.RENAME.name().toLowerCase(Locale.ROOT));
        v1Params.put(
                CollectionAdminParams.COLLECTION, obj.getRequest().getPathTemplateValues().get(CollectionAdminParams.COLLECTION));

        v1Params.put(CollectionAdminParams.TARGET, v1Params.remove("to"));

        collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
    }
}
