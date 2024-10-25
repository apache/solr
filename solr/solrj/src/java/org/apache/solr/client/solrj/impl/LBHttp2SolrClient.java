package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.RequestWriter;

import java.util.Arrays;
import java.util.Set;

public class LBHttp2SolrClient extends LBHttp2SolrClientBase {
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
}
