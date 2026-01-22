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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.opentelemetry.OtlpExporterFactory;
import org.apache.solr.util.RTimer;

public class OtelMetrics implements ConsumerMetrics {

  public static final String REGISTRY = "crossdc.consumer.registry";
  public static final String NAME_PREFIX = "crossdc_consumer_";
  public static final String ATTR_TYPE = "type";
  public static final String ATTR_SUBTYPE = "subtype";
  public static final String ATTR_RESULT = "result";

  protected final Map<String, Attributes> attributesCache = new ConcurrentHashMap<>();

  protected SolrMetricManager metricManager;

  protected LongCounter input;
  protected LongCounter collapsed;
  protected LongCounter output;
  protected LongHistogram outputBatchSizeHistogram;
  protected LongHistogram outputTimeHistogram;
  protected LongHistogram outputBackoffHistogram;
  protected LongHistogram outputFirstAttemptHistogram;

  public OtelMetrics() {
    register(REGISTRY);
  }

  protected void register(String scope) {
    this.metricManager = new SolrMetricManager(new OtlpExporterFactory().getExporter());
    SolrMetricsContext metricsContext = new SolrMetricsContext(metricManager, scope);

    input =
        metricsContext.longCounter(NAME_PREFIX + "input_total", "Total number of input messages");

    collapsed =
        metricsContext.longCounter(
            NAME_PREFIX + "collapsed_total", "Total number of collapsed messages");

    output =
        metricsContext.longCounter(NAME_PREFIX + "output_total", "Total number of output requests");

    outputBatchSizeHistogram =
        metricsContext.longHistogram(
            NAME_PREFIX + "output_batch_size", "Histogram of output batch sizes");

    outputBackoffHistogram =
        metricsContext.longHistogram(
            NAME_PREFIX + "output_backoff_size", "Histogram of output backoff sleep times");

    outputTimeHistogram =
        metricsContext.longHistogram(
            NAME_PREFIX + "output_time",
            "Histogram of output request times",
            OtelUnit.MILLISECONDS);

    outputFirstAttemptHistogram =
        metricsContext.longHistogram(
            NAME_PREFIX + "output_first_attempt_time",
            "Histogram of first attempt request times",
            OtelUnit.NANOSECONDS);
  }

  protected static final String KEY_SEPARATOR = "#";

  protected Attributes attr(String key1, String value1) {
    String key = key1 + KEY_SEPARATOR + value1;
    return attributesCache.computeIfAbsent(
        key, k -> Attributes.builder().put(key1, value1).build());
  }

  protected Attributes attr(String key1, String value1, String key2, String value2) {
    String key = key1 + KEY_SEPARATOR + value1 + KEY_SEPARATOR + key2 + KEY_SEPARATOR + value2;
    return attributesCache.computeIfAbsent(
        key, k -> Attributes.builder().put(key1, value1).put(key2, value2).build());
  }

  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  @Override
  public void incrementCollapsedCounter() {
    collapsed.add(1L);
  }

  @Override
  public void incrementInputCounter(String type, String subType) {
    incrementInputCounter(type, subType, 1);
  }

  @Override
  public void incrementInputCounter(String type, String subType, int delta) {
    input.add(delta, attr("type", type, "subtype", subType));
  }

  @Override
  public void incrementOutputCounter(String type, String result) {
    incrementOutputCounter(type, result, 1);
  }

  @Override
  public void incrementOutputCounter(String type, String result, int delta) {
    output.add(delta, attr("type", type, "result", result));
  }

  @Override
  public void recordOutputBatchSize(MirroredSolrRequest.Type type, SolrRequest<?> solrRequest) {
    if (type != MirroredSolrRequest.Type.UPDATE) {
      outputBatchSizeHistogram.record(
          1, attr(ATTR_TYPE, type.name(), ATTR_SUBTYPE, solrRequest.getPath()));
      return;
    }
    UpdateRequest req = (UpdateRequest) solrRequest;
    int addCount = req.getDocuments() == null ? 0 : req.getDocuments().size();
    int dbiCount = req.getDeleteById() == null ? 0 : req.getDeleteById().size();
    int dbqCount = req.getDeleteQuery() == null ? 0 : req.getDeleteQuery().size();
    if (addCount > 0) {
      outputBatchSizeHistogram.record(addCount, attr(ATTR_TYPE, type.name(), ATTR_SUBTYPE, "add"));
    }
    if (dbiCount > 0) {
      outputBatchSizeHistogram.record(
          dbiCount, attr(ATTR_TYPE, type.name(), ATTR_SUBTYPE, "delete_by_id"));
    }
    if (dbqCount > 0) {
      outputBatchSizeHistogram.record(
          dbiCount, attr(ATTR_TYPE, type.name(), ATTR_SUBTYPE, "delete_by_query"));
    }
  }

  @Override
  public void recordOutputBackoffTime(MirroredSolrRequest.Type type, long backoffTimeMs) {
    outputBackoffHistogram.record(backoffTimeMs, attr(ATTR_TYPE, type.name()));
  }

  @Override
  public void recordOutputFirstAttemptTime(MirroredSolrRequest.Type type, long firstAttemptTimeNs) {
    outputFirstAttemptHistogram.record(firstAttemptTimeNs, attr(ATTR_TYPE, type.name()));
  }

  @Override
  public ConsumerTimer startOutputTimeTimer(final String requestType) {
    final RTimer timer =
        new RTimer(TimeUnit.MILLISECONDS) {
          @Override
          public double stop() {
            double elapsedTime = super.stop();
            outputTimeHistogram.record(
                Double.valueOf(elapsedTime).longValue(), attr(ATTR_TYPE, requestType));
            return elapsedTime;
          }
        };
    return () -> timer.stop();
  }
}
