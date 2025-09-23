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
package org.apache.solr.crossdc.update.processor;

import static org.apache.solr.metrics.SolrCoreMetricManager.COLLECTION_ATTR;
import static org.apache.solr.metrics.SolrCoreMetricManager.CORE_ATTR;
import static org.apache.solr.metrics.SolrCoreMetricManager.REPLICA_TYPE_ATTR;
import static org.apache.solr.metrics.SolrCoreMetricManager.SHARD_ATTR;
import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

import io.opentelemetry.api.common.Attributes;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.metrics.otel.instruments.AttributedLongCounter;
import org.apache.solr.metrics.otel.instruments.AttributedLongHistogram;

/** Metrics presented for each SolrCore using `crossdc.producer.` path. */
public class ProducerMetrics {

  private final AttributedLongCounter local;
  private final AttributedLongCounter localError;
  private final AttributedLongCounter submitted;
  private final AttributedLongCounter submitError;
  private final AttributedLongHistogram documentSize;
  private final AttributedLongCounter documentTooLarge;

  public ProducerMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
    CoreDescriptor coreDescriptor = solrCore.getCoreDescriptor();
    CloudDescriptor cloudDescriptor =
        (coreDescriptor != null) ? coreDescriptor.getCloudDescriptor() : null;
    var attributesBuilder = Attributes.builder().put(CORE_ATTR, solrCore.getName());
    if (cloudDescriptor != null) {
      attributesBuilder
          .put(COLLECTION_ATTR, coreDescriptor.getCollectionName())
          .put(SHARD_ATTR, cloudDescriptor.getShardId())
          .put(REPLICA_TYPE_ATTR, cloudDescriptor.getReplicaType().toString());
    }
    var attributes = attributesBuilder.build();

    var localProcessed =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_local_processed",
            "The number of local documents processed (success or error)");
    var localSubmitted =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted",
            "The number of documents submitted to the Kafka topic (success or error)");
    var histogramDocSizes =
        solrMetricsContext.longHistogram(
            "solr_core_crossdc_producer_document_size",
            "Histogram of the processed document sizes processed",
            OtelUnit.BYTES);
    var tooLargeErrors =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_doc_too_large_errors",
            "The number of documents that were too large to send to the Kafka topic");

    this.local =
        new AttributedLongCounter(
            localProcessed, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.localError =
        new AttributedLongCounter(
            localProcessed, attributes.toBuilder().put(TYPE_ATTR, "error").build());
    this.submitted =
        new AttributedLongCounter(
            localSubmitted, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.submitError =
        new AttributedLongCounter(
            localSubmitted, attributes.toBuilder().put(TYPE_ATTR, "error").build());
    this.documentSize = new AttributedLongHistogram(histogramDocSizes, attributes);
    this.documentTooLarge = new AttributedLongCounter(tooLargeErrors, attributes);
  }

  /** Counter representing the number of local documents processed successfully. */
  public AttributedLongCounter getLocal() {
    return this.local;
  }

  /** Counter representing the number of local documents processed with error. */
  public AttributedLongCounter getLocalError() {
    return this.localError;
  }

  /** Counter representing the number of documents submitted to the Kafka topic. */
  public AttributedLongCounter getSubmitted() {
    return this.submitted;
  }

  /**
   * Counter representing the number of documents that were not submitted to the Kafka topic because
   * of exception during execution.
   */
  public AttributedLongCounter getSubmitError() {
    return this.submitError;
  }

  /** Histogram of the processed document size. */
  public AttributedLongHistogram getDocumentSize() {
    return this.documentSize;
  }

  /**
   * Counter representing the number of documents that were too large to send to the Kafka topic.
   */
  public AttributedLongCounter getDocumentTooLarge() {
    return this.documentTooLarge;
  }
}
