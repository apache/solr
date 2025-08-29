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

package org.apache.solr.handler.admin;

import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link MetricsHandler} */
public class MetricsHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema.xml");
    h.getCoreContainer().waitForLoadingCoresToFinish(30000);
  }

  @Test
  public void testMetricNamesFiltering() throws Exception {
    String expectedRequestsMetricName = "solr_core_requests";
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.METRIC_NAME_PARAM,
            expectedRequestsMetricName),
        resp);
    var metrics = resp.getValues().get("metrics");
    MetricSnapshots snapshots = (MetricSnapshots) metrics;
    assertEquals(1, snapshots.size());
    assertEquals(expectedRequestsMetricName, snapshots.get(0).getMetadata().getPrometheusName());

    handler.close();
  }

  @Test
  public void testMultipleMetricNamesFiltering() throws Exception {
    String expectedRequestsMetricName = "solr_core_requests";
    String expectedSearcherMetricName = "solr_core_searcher_new";
    var expected = Set.of(expectedRequestsMetricName, expectedSearcherMetricName);
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.METRIC_NAME_PARAM,
            expectedRequestsMetricName + "," + expectedSearcherMetricName),
        resp);

    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    assertEquals(2, metrics.size());
    Set<String> actual =
        metrics.stream().map(m -> m.getMetadata().getPrometheusName()).collect(Collectors.toSet());
    assertEquals(expected, actual);

    handler.close();
  }

  @Test
  public void testNonExistentMetricNameFiltering() throws Exception {
    String nonexistentMetricName = "nonexistent_metric_name";
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.METRIC_NAME_PARAM,
            nonexistentMetricName),
        resp);
    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    assertEquals(0, metrics.size());
    handler.close();
  }

  @Test
  public void testLabelFiltering() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.CATEGORY_PARAM,
            "QUERY"),
        resp);
    var metrics = (MetricSnapshots) resp.getValues().get("metrics");

    metrics.forEach(
        (ms) -> {
          ms.getDataPoints()
              .forEach(
                  (dp) -> {
                    assertEquals("QUERY", dp.getLabels().get(MetricsHandler.CATEGORY_PARAM));
                  });
        });

    handler.close();
  }

  @Test
  public void testMultipleLabelFiltering() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.CATEGORY_PARAM,
            "QUERY" + "," + "SEARCHER"),
        resp);

    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    metrics.forEach(
        (ms) -> {
          ms.getDataPoints()
              .forEach(
                  (dp) -> {
                    assertTrue(
                        dp.getLabels().get(MetricsHandler.CATEGORY_PARAM).equals("QUERY")
                            || dp.getLabels()
                                .get(MetricsHandler.CATEGORY_PARAM)
                                .equals("SEARCHER"));
                  });
        });

    handler.close();
  }

  @Test
  public void testNonExistentLabelFiltering() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.CORE_PARAM,
            "nonexistent_core_name"),
        resp);

    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    assertEquals(0, metrics.size());
    handler.close();
  }

  @Test
  public void testMixedLabelFiltering() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.CORE_PARAM,
            "collection1",
            MetricsHandler.CATEGORY_PARAM,
            "SEARCHER"),
        resp);

    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    metrics.forEach(
        (ms) -> {
          ms.getDataPoints()
              .forEach(
                  (dp) -> {
                    assertTrue(
                        dp.getLabels().get(MetricsHandler.CATEGORY_PARAM).equals("SEARCHER")
                            && dp.getLabels().get(MetricsHandler.CORE_PARAM).equals("collection1"));
                  });
        });

    handler.close();
  }

  @Test
  public void testMetricNamesAndLabelFiltering() throws Exception {
    String expectedMetricName = "solr_core_segments";
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    assertQ(req("*:*"), "//result[@numFound='0']");

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            MetricsHandler.PROMETHEUS_METRICS_WT,
            MetricsHandler.CATEGORY_PARAM,
            "CORE",
            MetricsHandler.METRIC_NAME_PARAM,
            expectedMetricName),
        resp);

    var metrics = (MetricSnapshots) resp.getValues().get("metrics");
    assertEquals(1, metrics.size());
    var actualDatapoint = metrics.get(0).getDataPoints().getFirst();
    assertEquals(expectedMetricName, metrics.get(0).getMetadata().getPrometheusName());
    assertEquals("CORE", actualDatapoint.getLabels().get(MetricsHandler.CATEGORY_PARAM));
    handler.close();
  }

  public static class DumpRequestHandler extends RequestHandlerBase {

    static String key = DumpRequestHandler.class.getName();
    Map<String, Object> gaugevals;

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      rsp.add("key", key);
    }

    @Override
    public String getDescription() {
      return "DO nothing";
    }

    @Override
    public void initializeMetrics(
        SolrMetricsContext parentContext, Attributes attributes, String scope) {
      super.initializeMetrics(parentContext, attributes, scope);
      MetricsMap metrics = new MetricsMap(map -> gaugevals.forEach((k, v) -> map.putNoEx(k, v)));
      solrMetricsContext.gauge(metrics, true, "dumphandlergauge", getCategory().toString(), scope);
    }

    @Override
    public Boolean registerV2() {
      return Boolean.FALSE;
    }

    @Override
    public Name getPermissionName(AuthorizationContext request) {
      return Name.METRICS_READ_PERM;
    }
  }
}
