package org.apache.solr.handler.admin;

import org.apache.solr.client.api.model.UpgradeCoreIndexRequestBody;
import org.apache.solr.client.api.model.UpgradeCoreIndexResponse;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.UpgradeCoreIndex;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class UpgradeCoreIndexOp implements CoreAdminHandler.CoreAdminOp {
  @FunctionalInterface
  public interface UpgradeCoreIndexFactory {
    UpgradeCoreIndex create(
        CoreContainer coreContainer,
        CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
        SolrQueryRequest req,
        SolrQueryResponse rsp);
  }

  static UpgradeCoreIndexFactory UPGRADE_CORE_INDEX_FACTORY = UpgradeCoreIndex::new;

  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    final boolean isAsync = params.get(CommonAdminParams.ASYNC) != null;
    final var requestBody = new UpgradeCoreIndexRequestBody();
    requestBody.updateChain = params.get(UpdateParams.UPDATE_CHAIN);

    UpgradeCoreIndex upgradeCoreIndexApi =
        UPGRADE_CORE_INDEX_FACTORY.create(
            it.handler.coreContainer, it.handler.coreAdminAsyncTracker, it.req, it.rsp);
    final UpgradeCoreIndexResponse response =
        upgradeCoreIndexApi.upgradeCoreIndex(cname, requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);

    if (isAsync) {
      final var opResponse = new NamedList<>();
      V2ApiUtils.squashIntoNamedListWithoutHeader(opResponse, response);
      // REQUESTSTATUS is returning the inner response NamedList as a positional array
      // ([k1,v1,k2,v2...]).
      // so converting to a map
      it.rsp.addResponse(opResponse.asMap(1));
    }
  }
}
