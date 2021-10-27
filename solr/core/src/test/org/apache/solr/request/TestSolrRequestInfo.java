package org.apache.solr.request;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSolrRequestInfo extends SolrTestCaseJ4 {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig.xml","schema11.xml");
    }

    public void testCloseHookTwice(){
        final SolrRequestInfo info = new SolrRequestInfo(
                new LocalSolrQueryRequest(h.getCore(), new MapSolrParams(Map.of())),
                new SolrQueryResponse());
        AtomicInteger counter = new AtomicInteger();
        info.addCloseHook(counter::incrementAndGet);
        SolrRequestInfo.setRequestInfo(info);
        SolrRequestInfo.setRequestInfo(info);
        SolrRequestInfo.clearRequestInfo();
        assertNotNull(SolrRequestInfo.getRequestInfo());
        SolrRequestInfo.clearRequestInfo();
        assertEquals("hook should be closed only once", 1, counter.get());
        assertNull(SolrRequestInfo.getRequestInfo());
    }
}
