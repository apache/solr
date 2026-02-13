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

import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

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
  private final AttributedLongCounter submittedAdd;
  private final AttributedLongCounter submittedAddError;
  private final AttributedLongCounter submittedDeleteById;
  private final AttributedLongCounter submittedDeleteByIdError;
  private final AttributedLongCounter submittedDeleteByQuery;
  private final AttributedLongCounter submittedDeleteByQueryError;
  private final AttributedLongCounter submittedCommit;
  private final AttributedLongCounter submittedCommitError;
  private final AttributedLongHistogram documentSize;
  private final AttributedLongCounter documentTooLarge;

  public ProducerMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
    var attributes = solrCore.getCoreAttributes();

    var localProcessed =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_local_processed",
            "The number of local documents processed (success or error)");
    var localSubmitted =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted",
            "The number of documents submitted to the Kafka topic (success or error)");
    var localSubmittedAdd =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted_add",
            "The number of add requests submitted to the Kafka topic (success or error)");
    var localSubmittedDbi =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted_delete_by_id",
            "The number of Delete-By-Id requests submitted to the Kafka topic (success or error)");
    var localSubmittedDbq =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted_delete_by_query",
            "The number of Delete-By-Query requests submitted to the Kafka topic (success or error)");
    var localSubmittedCommit =
        solrMetricsContext.longCounter(
            "solr_core_crossdc_producer_submitted_commit",
            "The number of standalone Commit requests submitted to the Kafka topic (success or error)");
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
    this.submittedAdd =
        new AttributedLongCounter(
            localSubmittedAdd, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.submittedAddError =
        new AttributedLongCounter(
            localSubmittedAdd, attributes.toBuilder().put(TYPE_ATTR, "error").build());
    this.submittedDeleteById =
        new AttributedLongCounter(
            localSubmittedDbi, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.submittedDeleteByIdError =
        new AttributedLongCounter(
            localSubmittedDbi, attributes.toBuilder().put(TYPE_ATTR, "error").build());
    this.submittedDeleteByQuery =
        new AttributedLongCounter(
            localSubmittedDbq, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.submittedDeleteByQueryError =
        new AttributedLongCounter(
            localSubmittedDbq, attributes.toBuilder().put(TYPE_ATTR, "error").build());
    this.submittedCommit =
        new AttributedLongCounter(
            localSubmittedCommit, attributes.toBuilder().put(TYPE_ATTR, "success").build());
    this.submittedCommitError =
        new AttributedLongCounter(
            localSubmittedCommit, attributes.toBuilder().put(TYPE_ATTR, "error").build());
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

  /** Counter representing the number of add requests submitted to the Kafka topic. */
  public AttributedLongCounter getSubmittedAdd() {
    return this.submittedAdd;
  }

  /**
   * Counter representing the number of add requests that were not submitted to the Kafka topic
   * because of exception during execution.
   */
  public AttributedLongCounter getSubmittedAddError() {
    return this.submittedAddError;
  }

  /** Counter representing the number of delete-by-id requests submitted to the Kafka topic. */
  public AttributedLongCounter getSubmittedDeleteById() {
    return this.submittedDeleteById;
  }

  /**
   * Counter representing the number of delete-by-id requests that were not submitted to the Kafka
   * topic because of exception during execution.
   */
  public AttributedLongCounter getSubmittedDeleteByIdError() {
    return this.submittedDeleteByIdError;
  }

  /** Counter representing the number of delete-by-query requests submitted to the Kafka topic. */
  public AttributedLongCounter getSubmittedDeleteByQuery() {
    return this.submittedDeleteByQuery;
  }

  /**
   * Counter representing the number of delete-by-query requests that were not submitted to the
   * Kafka topic because of exception during execution.
   */
  public AttributedLongCounter getSubmittedDeleteByQueryError() {
    return this.submittedDeleteByQueryError;
  }

  /** Counter representing the number of standalone Commit requests submitted to the Kafka topic. */
  public AttributedLongCounter getSubmittedCommit() {
    return this.submittedCommit;
  }

  /**
   * Counter representing the number of standalone Commit requests that were not submitted to the
   * Kafka topic because of exception during execution.
   */
  public AttributedLongCounter getSubmittedCommitError() {
    return this.submittedCommitError;
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
