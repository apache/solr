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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.PrometheusResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ServletUtils;
import org.apache.solr.servlet.SolrRequestParsers;

/**
 * Helper servlet that exports collected metrics in Prometheus format using {@link MetricsHandler}.
 */
public class MetricsServlet extends HttpServlet {
  private static final long serialVersionUID = -2881083456665410780L;

  public static final String SOLR_METRICS_MANAGER_ATTRIBUTE =
      MetricsServlet.class.getName() + ".solrMetricsManager";

  private SolrMetricManager metricManager;
  private MetricsHandler metricsHandler;
  private static final PrometheusResponseWriter writer = new PrometheusResponseWriter();

  @Override
  public void init() throws ServletException {
    metricManager =
        (SolrMetricManager) getServletContext().getAttribute(SOLR_METRICS_MANAGER_ATTRIBUTE);
    metricsHandler = new MetricsHandler(metricManager);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      final SolrQueryResponse solrQueryResponse = new SolrQueryResponse();
      final String path = ServletUtils.getPathAfterContext(req);
      SolrQueryRequest solrQueryRequest = SolrRequestParsers.DEFAULT.parse(null, path, req);
      metricsHandler.handleRequestBody(solrQueryRequest, solrQueryResponse);
      resp.setStatus(HttpServletResponse.SC_OK);
      final String contentType = writer.getContentType(solrQueryRequest, solrQueryResponse);
      resp.setContentType(contentType);
      resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
      writer.write(resp.getOutputStream(), solrQueryRequest, solrQueryResponse, contentType);
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }
}
