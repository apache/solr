package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HttpSolrClientJdkImplTest extends Http2SolrClientTestBase<HttpSolrClientJdkImpl.Builder> {


    @Test
    public void testQueryGet() throws Exception {
        super.testQueryGet();
    }
    @Test
    public void testQueryPost() throws Exception {
        super.testQueryPost();
    }
    @Test
    public void testQueryPut() throws Exception {
        super.testQueryPut();
    }
    @Test
    public void testQueryXmlGet() throws Exception {
        super.testQueryXmlGet();
    }

    @Test
    public void testQueryXmlPost() throws Exception {
        super.testQueryXmlPost();
    }

    @Test
    public void testQueryXmlPut() throws Exception {
        super.testQueryXmlPut();
    }

    @Test
    public void testDelete() throws Exception {
        DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(url).build()) {
            try {
                client.deleteById("id");
            } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
            }
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
            assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
            validateDelete();
        }
    }
    @Test
    public void testDeleteXml() throws Exception {
       DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client =
                     new HttpSolrClientJdkImpl.Builder(url).withResponseParser(new XMLResponseParser()).build()) {
            try {
                client.deleteByQuery("*:*");
            } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
            }
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
            assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
            validateDelete();
        }
    }
    protected String expectedUserAgent() {
        return "Solr[" + HttpSolrClientJdkImpl.class.getName() + "] 1.0";
    }

    @Override
    protected void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception {
        DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        SolrQuery q = new SolrQuery("foo");
        q.setParam("a", "\u1234");
        HttpSolrClientJdkImpl.Builder b = new HttpSolrClientJdkImpl.Builder(url);
        if(rp != null) {
            b.withResponseParser(rp);
        }
        try (HttpSolrClientJdkImpl client = b.build()) {
            client.query(q, method);
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }
    }

    @Override
    protected HttpSolrClientJdkImpl.Builder builder(String url, int connectionTimeout, int socketTimeout) {
        return new HttpSolrClientJdkImpl.Builder(url).withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    }

}
