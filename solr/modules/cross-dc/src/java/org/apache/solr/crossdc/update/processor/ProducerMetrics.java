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
package org.apache.solr.update.processor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * Metrics presented for each SolrCore using `crossdc.producer.` path.
 */
public class ProducerMetrics {

    private final Counter local;
    private final Counter localError;
    private final Counter submitted;
    private final Counter submitError;
    private final Histogram documentSize;
    private final Counter documentTooLarge;

    public ProducerMetrics(SolrMetricsContext solrMetricsContext, SolrCore solrCore) {
        this.local = solrMetricsContext.counter(solrCore, "local", "crossdc", "producer");
        this.localError = solrMetricsContext.counter(solrCore, "local", "crossdc", "producer", "errors");
        this.submitted = solrMetricsContext.counter(solrCore, "submitted", "crossdc", "producer");
        this.submitError = solrMetricsContext.counter(solrCore, "submit", "crossdc", "producer", "errors");
        this.documentSize = solrMetricsContext.histogram(solrCore, "documentSize", "crossdc", "producer");
        this.documentTooLarge = solrMetricsContext.counter(solrCore, "documentTooLarge", "crossdc", "producer", "errors");
    }

    /**
     * Counter representing the number of local documents processed successfully.
     */
    public Counter getLocal() {
        return this.local;
    }

    /**
     * Counter representing the number of local documents processed with error.
     */
    public Counter getLocalError() {
        return this.localError;
    }

    /**
     * Counter representing the number of documents submitted to the Kafka topic.
     */
    public Counter getSubmitted() {
        return this.submitted;
    }

    /**
     * Counter representing the number of documents that were not submitted to the Kafka topic because of exception during execution.
     */
    public Counter getSubmitError() {
        return this.submitError;
    }

    /**
     * Histogram of the processed document size.
     */
    public Histogram getDocumentSize() {
        return this.documentSize;
    }

    /**
     * Counter representing the number of documents that were too large to send to the Kafka topic.
     */
    public Counter getDocumentTooLarge() {
        return this.documentTooLarge;
    }
}
