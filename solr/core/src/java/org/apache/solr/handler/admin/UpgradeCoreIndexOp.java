package org.apache.solr.handler.admin;

import org.apache.lucene.index.IndexWriter;
import org.apache.solr.client.api.model.UpgradeCoreIndexRequestBody;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.api.UpgradeCoreIndex;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.index.LatestVersionFilterMergePolicy;
import org.apache.solr.util.RefCounted;

public class UpgradeCoreIndexOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    final var requestBody = new UpgradeCoreIndexRequestBody();
    requestBody.updateChain = params.get(UpdateParams.UPDATE_CHAIN);
    RefCounted<IndexWriter> iwRef = null;
    SolrCore core = it.req.getCore();
    try {
      if (iwRef == null) {
        iwRef = core.getSolrCoreState().getIndexWriter(core);
      }
      // set LatestVersionFilterMergePolicy as the merge policy to prevent older segments from
      // participating in merges while we reindex
      if (iwRef != null) {
        IndexWriter iw = iwRef.get();
        iw.getConfig()
            .setMergePolicy(new LatestVersionFilterMergePolicy(iw.getConfig().getMergePolicy()));
      }
      UpgradeCoreIndex upgradeCoreIndexApi =
          new UpgradeCoreIndex(
              it.handler.coreContainer, it.handler.coreAdminAsyncTracker, it.req, it.rsp);
      final var response = upgradeCoreIndexApi.upgradeCoreIndex(cname, requestBody);
      V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
    } finally {
      if (iwRef != null) {
        iwRef.decref();
      }
    }
  }
}
