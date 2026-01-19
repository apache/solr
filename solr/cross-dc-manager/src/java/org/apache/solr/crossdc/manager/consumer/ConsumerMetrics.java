package org.apache.solr.crossdc.manager.consumer;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;

public interface ConsumerMetrics {

  interface ConsumerTimer {
    double observeDuration();

    default void close() {
      observeDuration();
    }
  }

  void incrementCollapsedCounter();

  void incrementInputCounter(String type, String subType);

  void incrementInputCounter(String type, String subType, int delta);

  void incrementOutputCounter(String type, String result);

  void incrementOutputCounter(String type, String result, int delta);

  void recordOutputBatchSize(MirroredSolrRequest.Type type, SolrRequest<?> solrRequest);

  void recordOutputBackoffSize(MirroredSolrRequest.Type type, long backoffTimeMs);

  void recordOutputFirstAttemptSize(MirroredSolrRequest.Type type, long firstAttemptTimeNs);

  ConsumerTimer startOutputTimeTimer(String requestType);
}
