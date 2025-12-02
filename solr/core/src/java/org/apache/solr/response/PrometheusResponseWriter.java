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

import static org.apache.solr.handler.admin.MetricsHandler.OPEN_METRICS_WT;

import io.prometheus.metrics.expositionformats.OpenMetricsTextFormatWriter;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.SolrQueryRequest;

/** Response writer for Prometheus metrics. This is used only by the {@link MetricsHandler} */
@SuppressWarnings(value = "unchecked")
public class PrometheusResponseWriter implements QueryResponseWriter {
  // not TextQueryResponseWriter because Prometheus libs work with an OutputStream

  private static final String CONTENT_TYPE_PROMETHEUS = "text/plain; version=0.0.4";
  private static final String CONTENT_TYPE_OPEN_METRICS =
      "application/openmetrics-text; version=1.0.0; charset=utf-8";

  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException {

    // Check if we have pre-formatted Prometheus text (from single-node proxy)
    if (response.getValues().get("stream") instanceof InputStream stream) {
      try {
        stream.transferTo(out);
      } finally {
        stream.close();
      }
      return;
    }

    if (response.getException() != null) {
      out.write(response.getException().toString().getBytes(StandardCharsets.UTF_8));
      return;
    }

    // Otherwise handle MetricSnapshots
    var metrics = response.getValues().get("metrics");
    if (metrics == null) {
      throw new IOException("No metrics found in response");
    }
    MetricSnapshots snapshots = (MetricSnapshots) metrics;
    if (writeOpenMetricsFormat(request)) {
      new OpenMetricsTextFormatWriter(false, true).write(out, snapshots);
    } else {
      new PrometheusTextFormatWriter(false).write(out, snapshots);
    }
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return writeOpenMetricsFormat(request) ? CONTENT_TYPE_OPEN_METRICS : CONTENT_TYPE_PROMETHEUS;
  }

  private boolean writeOpenMetricsFormat(SolrQueryRequest request) {
    String wt = request.getParams().get(CommonParams.WT);
    if (OPEN_METRICS_WT.equals(wt)) {
      return true;
    }

    String acceptHeader =
        request.getHttpSolrCall() != null
            ? request.getHttpSolrCall().getReq().getHeader("Accept")
            : null;

    if (acceptHeader == null) {
      return false;
    }

    return acceptHeader.contains("application/openmetrics-text")
        && (acceptHeader.contains("version=1.0.0"));
  }
}
