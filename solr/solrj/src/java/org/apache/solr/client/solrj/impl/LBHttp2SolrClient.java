package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;

import java.util.Set;

public class LBHttp2SolrClient extends LBHttpSolrClientBase {
    protected final Http2SolrClient solrClient;

    protected LBHttp2SolrClient(Builder builder) {
        super(builder);
        this.solrClient = builder.http2SolrClient;
        this.aliveCheckIntervalMillis = builder.aliveCheckIntervalMillis;
        this.defaultCollection = builder.defaultCollection;
    }

    @Override
    protected SolrClient getClient(Endpoint endpoint) {
        return solrClient;
    }

    @Override
    public ResponseParser getParser() {
        return solrClient.getParser();
    }

    @Override
    public RequestWriter getRequestWriter() {
        return solrClient.getRequestWriter();
    }

    public Set<String> getUrlParamNames() {
        return solrClient.getUrlParamNames();
    }

    public static class Builder extends LBHttpSolrClientBuilderBase<LBHttp2SolrClient, LBHttp2SolrClient.Builder, Http2SolrClient> {

        public Builder(Http2SolrClient http2Client, Endpoint... endpoints) {
            super(http2Client, endpoints);
        }

        @Override
        public LBHttp2SolrClient build() {
            return new LBHttp2SolrClient(this);
        }
    }
}
