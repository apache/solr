package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.eclipse.jetty.client.InputStreamResponseListener;
import java.io.IOException;

public class ConcurrentUpdateSolrClient extends ConcurrentUpdateBaseSolrClient {

  private final HttpSolrClientBase client;

  @Override
  protected InputStreamResponseListener doSendUpdateStream(Update update) throws IOException {
    UpdateRequest req = update.request();
    String collection = update.collection();
    try {
      client.request(req, collection);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    return null;
  }

  public static class Builder extends ConcurrentUpdateBaseSolrClient.Builder {
    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase)
     */
    public Builder(String baseUrl, HttpSolrClientBase client) {
      this(baseUrl, client, false);
    }

    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase, boolean)
     */
    public Builder(String baseSolrUrl, HttpSolrClientBase client, boolean closeHttpClient) {
      super(baseSolrUrl, client, closeHttpClient);
    }

    @Override
    public ConcurrentUpdateSolrClient build() {
      return new ConcurrentUpdateSolrClient(this);
    }
  }

  protected ConcurrentUpdateSolrClient(ConcurrentUpdateSolrClient.Builder builder) {
    super(builder);
    this.client = builder.getClient();
  }

}
