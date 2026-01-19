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

import io.prometheus.metrics.core.datapoints.Timer;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;

public class PrometheusMetrics implements ConsumerMetrics {

  protected PrometheusRegistry registry;
  protected Counter input;
  protected Counter collapsed;
  protected Counter output;
  protected Histogram outputBatchSizeHistogram;
  protected Histogram outputTimeHistogram;
  protected Histogram outputBackoffHistogram;
  protected Histogram outputFirstAttemptHistogram;

  public PrometheusMetrics() {
    register(new PrometheusRegistry());
  }

  protected void register(PrometheusRegistry registry) {
    this.registry = registry;
    input =
        Counter.builder()
            .name("consumer_input_total")
            .help("Total number of input messages")
            .labelNames("type", "subtype")
            .register(registry);

    collapsed =
        Counter.builder()
            .name("consumer_collapsed_total")
            .help("Total number of collapsed messages")
            .register(registry);

    output =
        Counter.builder()
            .name("consumer_output_total")
            .help("Total number of output requests")
            .labelNames("type", "result")
            .register(registry);

    outputBatchSizeHistogram =
        Histogram.builder()
            .name("consumer_output_batch_size_histogram")
            .help("Histogram of output batch sizes")
            .labelNames("type", "subtype")
            .register(registry);

    outputBackoffHistogram =
        Histogram.builder()
            .name("consumer_output_backoff_histogram")
            .help("Histogram of output backoff sleep times")
            .labelNames("type")
            .register(registry);

    outputTimeHistogram =
        Histogram.builder()
            .name("consumer_output_time_histogram")
            .help("Histogram of output request times")
            .labelNames("type")
            .register(registry);

    outputFirstAttemptHistogram =
        Histogram.builder()
            .name("consumer_output_first_attempt_histogram")
            .help("Histogram of first attempt request times")
            .labelNames("type")
            .register(registry);
  }

  public PrometheusRegistry getRegistry() {
    return registry;
  }

  @Override
  public void incrementCollapsedCounter() {
    collapsed.inc();
  }

  @Override
  public void incrementInputCounter(String type, String subType) {
    incrementInputCounter(type, subType, 1);
  }

  @Override
  public void incrementInputCounter(String type, String subType, int delta) {
    input.labelValues(type, subType).inc(delta);
  }

  @Override
  public void incrementOutputCounter(String type, String result) {
    incrementOutputCounter(type, result, 1);
  }

  @Override
  public void incrementOutputCounter(String type, String result, int delta) {
    input.labelValues(type, result).inc(delta);
  }

  @Override
  public void recordOutputBatchSize(MirroredSolrRequest.Type type, SolrRequest<?> solrRequest) {
    if (type != MirroredSolrRequest.Type.UPDATE) {
      outputBatchSizeHistogram.labelValues(type.name(), solrRequest.getPath()).observe(1);
      return;
    }
    UpdateRequest req = (UpdateRequest) solrRequest;
    int addCount = req.getDocuments() == null ? 0 : req.getDocuments().size();
    int dbiCount = req.getDeleteById() == null ? 0 : req.getDeleteById().size();
    int dbqCount = req.getDeleteQuery() == null ? 0 : req.getDeleteQuery().size();
    if (addCount > 0) {
      outputBatchSizeHistogram.labelValues(type.name(), "add").observe(addCount);
    }
    if (dbiCount > 0) {
      outputBatchSizeHistogram.labelValues(type.name(), "dbi").observe(dbiCount);
    }
    if (dbqCount > 0) {
      outputBatchSizeHistogram.labelValues(type.name(), "dbq").observe(dbqCount);
    }
  }

  @Override
  public void recordOutputBackoffSize(MirroredSolrRequest.Type type, long backoffTimeMs) {
    outputBackoffHistogram.labelValues(type.name()).observe(backoffTimeMs);
  }

  @Override
  public void recordOutputFirstAttemptSize(MirroredSolrRequest.Type type, long firstAttemptTimeNs) {
    outputFirstAttemptHistogram.labelValues(type.name()).observe(firstAttemptTimeNs);
  }

  @Override
  public ConsumerTimer startOutputTimeTimer(String requestType) {
    final Timer timer = outputTimeHistogram.labelValues(requestType).startTimer();
    return () -> timer.observeDuration();
  }
}
