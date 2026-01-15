package org.apache.solr.crossdc.manager.consumer;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;

public class NoopConsumerMetrics implements ConsumerMetrics {
  public static final NoopConsumerMetrics INSTANCE = new NoopConsumerMetrics();

  @Override
  public void incrementCollapsedCounter() {

  }

  @Override
  public void incrementInputCounter(String type, String subType) {

  }

  @Override
  public void incrementInputCounter(String type, String subType, int delta) {

  }

  @Override
  public void incrementOutputCounter(String type, String result) {

  }

  @Override
  public void incrementOutputCounter(String type, String result, int delta) {

  }

  @Override
  public void recordOutputBatchSize(MirroredSolrRequest.Type type, SolrRequest<?> solrRequest) {

  }

  @Override
  public void recordOutputBackoffSize(MirroredSolrRequest.Type type, long backoffTimeMs) {

  }

  @Override
  public void recordOutputFirstAttemptSize(MirroredSolrRequest.Type type, long firstAttemptTimeNs) {

  }

  private static final ConsumerTimer NOOP_TIMER = new ConsumerTimer() {
    @Override
    public double observeDuration() {
      return 0;
    }
  };

  @Override
  public ConsumerTimer startOutputTimeTimer(String requestType) {
    return NOOP_TIMER;
  }
}
