package org.apache.solr.client.solrj.impl;

import java.util.concurrent.TimeUnit;

public abstract class LBHttpSolrClientBuilderBase<
    A extends LBHttpSolrClientBase<?>,
    B extends LBHttpSolrClientBuilderBase<?, ?, ?>,
    C extends HttpSolrClientBase> {
  final C solrClient;
  protected final LBSolrClient.Endpoint[] solrEndpoints;
  long aliveCheckIntervalMillis =
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS); // 1 minute between checks
  protected String defaultCollection;

  public abstract A build();

  public LBHttpSolrClientBuilderBase(C http2Client, LBSolrClient.Endpoint... endpoints) {
    this.solrClient = http2Client;
    this.solrEndpoints = endpoints;
  }

  /**
   * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use
   * this to set that interval
   *
   * @param aliveCheckInterval how often to ping for aliveness
   */
  @SuppressWarnings("unchecked")
  public B setAliveCheckInterval(int aliveCheckInterval, TimeUnit unit) {
    if (aliveCheckInterval <= 0) {
      throw new IllegalArgumentException(
          "Alive check interval must be " + "positive, specified value = " + aliveCheckInterval);
    }
    this.aliveCheckIntervalMillis = TimeUnit.MILLISECONDS.convert(aliveCheckInterval, unit);
    return (B) this;
  }

  /** Sets a default for core or collection based requests. */
  @SuppressWarnings("unchecked")
  public B withDefaultCollection(String defaultCoreOrCollection) {
    this.defaultCollection = defaultCoreOrCollection;
    return (B) this;
  }
}
