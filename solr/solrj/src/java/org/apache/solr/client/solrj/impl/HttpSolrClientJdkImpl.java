package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.Collection;

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

    @Override
    protected boolean isFollowRedirects() {
        return false;
    }

    @Override
    protected boolean processorAcceptsMimeType(Collection<String> processorSupportedContentTypes, String mimeType) {
        return false;
    }

    @Override
    protected String allProcessorSupportedContentTypesCommaDelimited(Collection<String> processorSupportedContentTypes) {
        return null;
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
