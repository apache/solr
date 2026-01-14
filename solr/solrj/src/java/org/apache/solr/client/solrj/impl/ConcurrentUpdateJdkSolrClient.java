package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.UpdateRequest;

public class ConcurrentUpdateJdkSolrClient extends ConcurrentUpdateBaseSolrClient {

  private final HttpJdkSolrClient client;

  protected ConcurrentUpdateJdkSolrClient(ConcurrentUpdateJdkSolrClient.Builder builder) {
    super(builder);
    this.client = (HttpJdkSolrClient) builder.getClient();
  }

  @Override
  protected StreamingResponse doSendUpdateStream(Update update) {
    UpdateRequest req = update.request();
    String collection = update.collection();
    CompletableFuture<HttpResponse<InputStream>> resp =
        client.requestInputStreamAsync(basePath, req, collection);

    return new StreamingResponse() {

      @Override
      public int awaitResponse(long timeoutMillis) throws Exception {
        return resp.get(timeoutMillis, TimeUnit.MILLISECONDS).statusCode();
      }

      @Override
      public InputStream getInputStream() {
        try {
          return resp.get().body();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return InputStream.nullInputStream();
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Object getUnderlyingResponse() {
        return resp;
      }

      @Override
      public void close() throws IOException {
        getInputStream().close();
      }
    };
  }

  public static class Builder extends ConcurrentUpdateBaseSolrClient.Builder {
    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase)
     */
    public Builder(String baseUrl, HttpJdkSolrClient client) {

      this(baseUrl, client, false);
      // The base class uses idle timeout with StreamingResponse#awaitResponse so it needs to be
      // set!
      this.idleTimeoutMillis = 1000;
    }

    /**
     * @see org.apache.solr.client.solrj.impl.ConcurrentUpdateBaseSolrClient.Builder#Builder(String,
     *     HttpSolrClientBase, boolean)
     */
    public Builder(String baseSolrUrl, HttpSolrClientBase client, boolean closeHttpClient) {
      super(baseSolrUrl, client, closeHttpClient);
    }

    @Override
    public ConcurrentUpdateJdkSolrClient build() {
      return new ConcurrentUpdateJdkSolrClient(this);
    }
  }
}
