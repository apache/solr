package org.apache.solr.client.solrj.impl;

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class LBHttpJdkSolrClientTest extends LBHttpSolrClientTestBase<HttpJdkSolrClient> {

    @Test
    public void testWithTheseParamNamesInTheUrl() {
        String url = "http://127.0.0.1:8080";
        Set<String> urlParamNames = Collections.singleton("param1");
        HttpJdkSolrClient delegate = new HttpJdkSolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames).build();
        super.testWithTheseParamNamesInTheUrl(url, urlParamNames, delegate);
    }

    @Override
    protected LBClientHolder buildLoadBalancingClient(HttpJdkSolrClient delegate, LBSolrClient.Endpoint... endpoints) {
        return new LBClientHolder(new LBHttpJdkSolrClient.Builder(delegate, endpoints).build(), delegate);
    }
}
