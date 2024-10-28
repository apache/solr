package org.apache.solr.client.solrj.impl;

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class LBHttp2SolrClientTest extends LBHttpSolrClientTestBase<Http2SolrClient> {

    @Test
    public void testWithTheseParamNamesInTheUrl() {
        String url = "http://127.0.0.1:8080";
        Set<String> urlParamNames = Collections.singleton("param1");
        Http2SolrClient delegate = new Http2SolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames).build();
        super.testWithTheseParamNamesInTheUrl(url, urlParamNames, delegate);
    }

    @Override
    protected LBClientHolder buildLoadBalancingClient(Http2SolrClient delegate, LBSolrClient.Endpoint... endpoints) {
        return new LBClientHolder(new LBHttp2SolrClient.Builder(delegate, endpoints).build(), delegate);
    }

}
