package org.apache.solr.handler.admin.api;

import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.V2ApiMappingTest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.TARGET;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.mockito.Mockito.mock;


public class V2ClusterAPIMappingTest extends V2ApiMappingTest<CollectionsHandler> {

@Test
public void testRenameClusterAllParams() throws Exception {
        final SolrParams v1Params = captureConvertedV1Params(
                "/clusters/rename/clusterName", "POST", "{\"rename\": {\"to\": \"targetCluster\"}}");

        assertEquals("rename", v1Params.get(ACTION));
        assertEquals("clusterName", v1Params.get(COLLECTION));
        assertEquals("targetCluster", v1Params.get(TARGET));
}




    @Override
    public void populateApiBag() {

    }

    @Override
    public CollectionsHandler createUnderlyingRequestHandler() {
        return null;
    }

    @Override
    public boolean isCoreSpecific() {
        return false;
    }
}
