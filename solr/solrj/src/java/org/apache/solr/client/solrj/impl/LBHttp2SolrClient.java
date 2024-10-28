package org.apache.solr.client.solrj.impl;

public class LBHttp2SolrClient extends LBHttpSolrClientBase<Http2SolrClient> {

  protected LBHttp2SolrClient(Builder builder) {
    super(builder);
  }

  public static class Builder
      extends LBHttpSolrClientBuilderBase<
          LBHttp2SolrClient, LBHttp2SolrClient.Builder, Http2SolrClient> {

    public Builder(Http2SolrClient http2Client, Endpoint... endpoints) {
      super(http2Client, endpoints);
    }

    @Override
    public LBHttp2SolrClient build() {
      return new LBHttp2SolrClient(this);
    }
  }
}
