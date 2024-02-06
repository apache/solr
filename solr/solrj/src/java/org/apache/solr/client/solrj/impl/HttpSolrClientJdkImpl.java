package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;

public class HttpSolrClientJdkImpl extends Http2SolrClientBase {
    protected HttpSolrClientJdkImpl(String serverBaseUrl, HttpSolrClientBuilderBase builder) {
        super(serverBaseUrl, builder);
    }

    @Override
    public NamedList<Object> request(SolrRequest<?> request, String collection) throws SolrServerException, IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    public static class Builder extends HttpSolrClientBuilderBase {

        public Builder() {
            super();
        }
        public  Builder(String baseSolrUrl) {
           super(baseSolrUrl);
        }
        public HttpSolrClientJdkImpl build() {
            return null;
        }
    }
}
