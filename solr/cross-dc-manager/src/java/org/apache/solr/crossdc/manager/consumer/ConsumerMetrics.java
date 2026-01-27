/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crossdc.manager.consumer;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;

/**
 * Interface for tracking and recording metrics related to the processing of messages and requests
 * in the {@link Consumer}. Provides methods to increment counters, record timing metrics, and
 * capture other performance-related data points.
 */
public interface ConsumerMetrics {

  /** No-op implementation of {@link ConsumerMetrics}. */
  ConsumerMetrics NOOP =
      new ConsumerMetrics() {
        @Override
        public void incrementCollapsedCounter() {}

        @Override
        public void incrementInputMsgCounter(long delta) {}

        @Override
        public void incrementInputReqCounter(String type, String subType, int delta) {}

        @Override
        public void incrementOutputCounter(String type, String result, int delta) {}

        @Override
        public void recordOutputBatchSize(
            MirroredSolrRequest.Type type, SolrRequest<?> solrRequest) {}

        @Override
        public void recordOutputBackoffTime(MirroredSolrRequest.Type type, long backoffTimeMs) {}

        @Override
        public void recordOutputFirstAttemptTime(
            MirroredSolrRequest.Type type, long firstAttemptTimeMs) {}

        @Override
        public ConsumerTimer startOutputTimeTimer(String requestType) {
          return () -> 0;
        }
      };

  /**
   * Represents a timer interface used for measuring and observing the duration of tasks. Start
   * measuring elapsed time when created.
   */
  interface ConsumerTimer {
    /**
     * Return the elapsed time in milliseconds.
     *
     * @return
     */
    double observeDuration();

    /** */
    default void close() {
      observeDuration();
    }
  }

  /** Increments the counter for input messages. */
  default void incrementInputMsgCounter() {
    incrementInputMsgCounter(1L);
  }

  /**
   * Increments the counter for input messages.
   *
   * @param delta increase the counter by this value
   */
  void incrementInputMsgCounter(long delta);

  /** Increments the counter for collapsed "add" requests. */
  void incrementCollapsedCounter();

  /**
   * Increments the counter for input requests by type and subtype.
   *
   * @param type request type, one of {@link
   *     org.apache.solr.crossdc.common.MirroredSolrRequest.Type} values.
   * @param subType additional subtype: add, delete_by_id, delete_by_query, or action for other
   *     request types.
   */
  default void incrementInputReqCounter(String type, String subType) {
    incrementInputReqCounter(type, subType, 1);
  }

  /**
   * Increments the counter for input requests by type, subtype, and delta.
   *
   * @param type request type, one of {@link
   *     org.apache.solr.crossdc.common.MirroredSolrRequest.Type} values.
   * @param subType additional subtype: add, delete_by_id, delete_by_query, or action for other
   *     request types.
   * @param delta increase the counter by this value
   */
  void incrementInputReqCounter(String type, String subType, int delta);

  /**
   * Increments the counter for output requests by type and result.
   *
   * @param type the type of the request
   * @param result the result of the request, such as success or failure
   */
  default void incrementOutputCounter(String type, String result) {
    incrementOutputCounter(type, result, 1);
  }

  /**
   * Increments the counter for output requests by type, result, and delta.
   *
   * @param type the type of the request
   * @param result the result of the request, such as success or failure
   * @param delta the value by which the counter should be increased
   */
  void incrementOutputCounter(String type, String result, int delta);

  /**
   * Records the batch size of the output request. Batch size is defined as the number of operations
   * in an output {@link SolrRequest} (which may be different than the input size due to
   * collapsing).
   *
   * @param type the type of the request, corresponding to one of the {@link
   *     MirroredSolrRequest.Type} values
   * @param solrRequest SolrRequest object for which the batch size is being recorded
   */
  void recordOutputBatchSize(MirroredSolrRequest.Type type, SolrRequest<?> solrRequest);

  /**
   * Records the backoff time for output requests. Backoff time represents the delay before the next
   * retry for the specified request type.
   *
   * @param type the type of the request, corresponding to one of the {@link
   *     MirroredSolrRequest.Type} values.
   * @param backoffTimeMs the backoff time in milliseconds.
   */
  void recordOutputBackoffTime(MirroredSolrRequest.Type type, long backoffTimeMs);

  /**
   * Records the latency between the time when the message was sent at source and the time of the
   * first attempt at processing.
   *
   * @param type the type of the request, corresponding to one of the {@link
   *     MirroredSolrRequest.Type} values
   * @param firstAttemptTimeMs the latency of the first attempt in milliseconds.
   */
  void recordOutputFirstAttemptTime(MirroredSolrRequest.Type type, long firstAttemptTimeMs);

  /**
   * Starts a timer to measure the duration of an output request processing by the given request
   * type.
   *
   * @param requestType the type of the request for which the timer is started
   * @return a {@link ConsumerTimer} that allows to measure the elapsed time
   */
  ConsumerTimer startOutputTimeTimer(String requestType);
}
