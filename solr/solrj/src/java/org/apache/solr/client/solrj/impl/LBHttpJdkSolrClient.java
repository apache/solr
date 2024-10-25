package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrClient;

public class LBHttpJdkSolrClient extends LBHttpSolrClientBase<HttpJdkSolrClient> {

    protected LBHttpJdkSolrClient(Builder b) {
        super(b);
    }

    @Override
    protected SolrClient getClient(Endpoint endpoint) {
        return solrClient;
    }

    public static class Builder extends LBHttpSolrClientBuilderBase<LBHttpJdkSolrClient, LBHttpJdkSolrClient.Builder, HttpJdkSolrClient> {

        public Builder(HttpJdkSolrClient httpJdkClient, Endpoint... endpoints) {
            super(httpJdkClient, endpoints);
        }

        @Override
        public LBHttpJdkSolrClient build() {
            return new LBHttpJdkSolrClient(this);
        }
    }
}
