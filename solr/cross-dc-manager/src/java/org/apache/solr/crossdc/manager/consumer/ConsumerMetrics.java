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

public interface ConsumerMetrics {

  ConsumerMetrics NOOP =
      new ConsumerMetrics() {
        @Override
        public void incrementCollapsedCounter() {}

        @Override
        public void incrementInputCounter(String type, String subType) {}

        @Override
        public void incrementInputCounter(String type, String subType, int delta) {}

        @Override
        public void incrementOutputCounter(String type, String result) {}

        @Override
        public void incrementOutputCounter(String type, String result, int delta) {}

        @Override
        public void recordOutputBatchSize(
            MirroredSolrRequest.Type type, SolrRequest<?> solrRequest) {}

        @Override
        public void recordOutputBackoffTime(MirroredSolrRequest.Type type, long backoffTimeMs) {}

        @Override
        public void recordOutputFirstAttemptTime(
            MirroredSolrRequest.Type type, long firstAttemptTimeNs) {}

        @Override
        public ConsumerTimer startOutputTimeTimer(String requestType) {
          return () -> 0;
        }
      };

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

  void recordOutputBackoffTime(MirroredSolrRequest.Type type, long backoffTimeMs);

  void recordOutputFirstAttemptTime(MirroredSolrRequest.Type type, long firstAttemptTimeNs);

  ConsumerTimer startOutputTimeTimer(String requestType);
}
