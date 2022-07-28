package org.apache.solr.handler;

import org.apache.solr.api.EndPoint;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.List;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

public class ListConfigSetsAPI extends ConfigSetAPI {
    public ListConfigSetsAPI(CoreContainer coreContainer) {
        super(coreContainer);
    }

    @EndPoint(method = GET, path = "/cluster/configs", permission = CONFIG_READ_PERM)
    public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        final NamedList<Object> results = new NamedList<>();
        List<String> configSetsList = configSetService.listConfigs();
        results.add("configSets", configSetsList);
        SolrResponse response = new OverseerSolrResponse(results);
        rsp.getValues().addAll(response.getResponse());
    }
}
