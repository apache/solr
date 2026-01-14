package org.apache.solr.client.solrj.impl;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentUpdateJdkSolrClientTest extends ConcurrentUpdateSolrClientTestBase {

  @Override
  public HttpSolrClientBase solrClient(Integer overrideIdleTimeoutMs) {
    var builder =
        new HttpJdkSolrClient.Builder().withSSLContext(MockTrustManager.ALL_TRUSTING_SSL_CONTEXT);
    if (overrideIdleTimeoutMs != null) {
      builder.withIdleTimeout(overrideIdleTimeoutMs, TimeUnit.MILLISECONDS);
    }
    return builder.build();
  }

  @Override
  public ConcurrentUpdateBaseSolrClient concurrentClient(
      HttpSolrClientBase solrClient,
      String baseUrl,
      String defaultCollection,
      int queueSize,
      int threadCount,
      boolean disablePollQueue) {
    var builder =
        new ConcurrentUpdateJdkSolrClient.Builder(baseUrl, (HttpJdkSolrClient) solrClient)
            .withQueueSize(queueSize)
            .withThreadCount(threadCount);
    if (defaultCollection != null) {
      builder.withDefaultCollection(defaultCollection);
    }
    if (disablePollQueue) {
      builder.setPollQueueTime(0, TimeUnit.MILLISECONDS);
    }
    return builder.build();
  }

  @Override
  public ConcurrentUpdateBaseSolrClient outcomeCountingConcurrentClient(
      String serverUrl,
      int queueSize,
      int threadCount,
      HttpSolrClientBase solrClient,
      AtomicInteger successCounter,
      AtomicInteger failureCounter,
      StringBuilder errors) {
    return new OutcomeCountingConcurrentUpdateSolrClient.Builder(
            serverUrl, (HttpJdkSolrClient) solrClient, successCounter, failureCounter, errors)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .setPollQueueTime(0, TimeUnit.MILLISECONDS)
        .build();
  }

  public static class OutcomeCountingConcurrentUpdateSolrClient
      extends ConcurrentUpdateJdkSolrClient {
    private final AtomicInteger successCounter;
    private final AtomicInteger failureCounter;
    private final StringBuilder errors;

    public OutcomeCountingConcurrentUpdateSolrClient(
        OutcomeCountingConcurrentUpdateSolrClient.Builder builder) {
      super(builder);
      this.successCounter = builder.successCounter;
      this.failureCounter = builder.failureCounter;
      this.errors = builder.errors;
    }

    @Override
    public void handleError(Throwable ex) {
      failureCounter.incrementAndGet();
      errors.append(" " + ex);
    }

    @Override
    public void onSuccess(Object responseMetadata, InputStream respBody) {
      successCounter.incrementAndGet();
    }

    public static class Builder extends ConcurrentUpdateJdkSolrClient.Builder {
      protected final AtomicInteger successCounter;
      protected final AtomicInteger failureCounter;
      protected final StringBuilder errors;

      public Builder(
          String baseSolrUrl,
          HttpJdkSolrClient http2Client,
          AtomicInteger successCounter,
          AtomicInteger failureCounter,
          StringBuilder errors) {
        super(baseSolrUrl, http2Client);
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.errors = errors;
      }

      @Override
      public OutcomeCountingConcurrentUpdateSolrClient build() {
        return new OutcomeCountingConcurrentUpdateSolrClient(this);
      }
    }
  }
}
