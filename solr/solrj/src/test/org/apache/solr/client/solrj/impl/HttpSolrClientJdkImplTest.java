package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;

import java.util.concurrent.TimeUnit;

public class HttpSolrClientJdkImplTest extends Http2SolrClientTestBase<HttpSolrClientBuilderBase> {

    protected String expectedUserAgent() {
        return "Solr[" + HttpSolrClientJdkImpl.class.getName() + "] 2.0";
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
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }
    }

    @Override
    protected HttpSolrClientBuilderBase builder(String url, int connectionTimeout, int socketTimeout) {
        return new HttpSolrClientBuilderBase(url).withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    }

}
