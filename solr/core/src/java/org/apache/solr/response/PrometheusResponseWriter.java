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
package org.apache.solr.response;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response writer for Prometheus metrics. This is used only by the {@link
 * org.apache.solr.handler.admin.MetricsHandler}
 */
@SuppressWarnings(value = "unchecked")
public class PrometheusResponseWriter implements QueryResponseWriter {
  // not TextQueryResponseWriter because Prometheus libs work with an OutputStream

  private static final String CONTENT_TYPE_PROMETHEUS = "text/plain; version=0.0.4";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException {
    var prometheusTextFormatWriter = new PrometheusTextFormatWriter(false);
    prometheusTextFormatWriter.write(out, (MetricSnapshots) response.getValues().get("metrics"));
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_PROMETHEUS;
  }
}
