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

package org.apache.solr.cloud;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.SolrMetricTestUtils;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for cloud tests wanting to track authentication metrics. The assertions provided
 * by this base class require a *minimum* count, not exact count from metrics. Warning: Make sure
 * that your test case does not break when beasting.
 */
public class SolrCloudAuthTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<String> AUTH_METRICS_KEYS =
      Arrays.asList(
          "solr_core_authentication_plugin_errors",
          "solr_core_authentication_plugin_requests",
          "solr_core_authentication_plugin_num_authenticated",
          "solr_core_authentication_plugin_num_pass_through",
          "solr_core_authentication_plugin_fail_wrong_credentials",
          "solr_core_authentication_plugin_fail_missing_credentials",
          "solr_core_authentication_plugin_request_times",
          "solr_core_authentication_plugin_total_time");
  private static final List<String> AUTH_METRICS_METER_KEYS = Arrays.asList("solr_core_authentication_plugin_errors", "count");
  private static final List<String> AUTH_METRICS_TIMER_KEYS =
      Collections.singletonList("solr_core_authentication_plugin_request_times");

  @SuppressWarnings({"rawtypes"})
  public static final Predicate NOT_NULL_PREDICATE = o -> o != null;

  private static final List<String> AUDIT_METRICS_KEYS = Arrays.asList("count");
  private static final List<String> AUDIT_METRICS_TO_COMPARE = Arrays.asList("count");

  @BeforeClass
  public static void enableMetrics() {
    System.setProperty("metricsEnabled", "true");
  }

  @AfterClass
  public static void disableMetrics() {
    System.clearProperty("metricsEnabled");
  }

  /** Used to check metric counts for PKI auth */
  protected void assertPkiAuthMetricsMinimums(
      int requests,
      int authenticated,
      int passThrough,
      int failWrongCredentials,
      int failMissingCredentials,
      int errors)
      throws InterruptedException {
    String handler = "/authentication/pki";
    String registryName = "solr.node";
    Labels labels =
        Labels.of("otel_scope_name", "org.apache.solr", "category", "SECURITY", "handler", handler);
    assertAuthMetricsMinimumsPrometheus(
        handler,
        registryName,
        labels,
        requests,
        authenticated,
        passThrough,
        failWrongCredentials,
        failMissingCredentials,
        errors);
  }

  /**
   * Used to check metric counts for the AuthPlugin in use (except PKI)
   *
   * <p>TODO: many of these params have to be under specified - this should wait a bit to see the
   * desired params and timeout
   */
  protected void assertAuthMetricsMinimums(
      int requests,
      int authenticated,
      int passThrough,
      int failWrongCredentials,
      int failMissingCredentials,
      int errors)
      throws InterruptedException {
    String handler = "/authentication";
    String registryName = "solr.node";
    Labels labels =
        Labels.of("otel_scope_name", "org.apache.solr", "category", "SECURITY", "handler", handler);
    assertAuthMetricsMinimumsPrometheus(
        handler,
        registryName,
        labels,
        requests,
        authenticated,
        passThrough,
        failWrongCredentials,
        failMissingCredentials,
        errors);
  }

  /**
   * Common test method to be able to check security from any authentication plugin
   *
   * @param cluster the MiniSolrCloudCluster to fetch metrics from
   * @param prefix the metrics key prefix, currently "SECURITY./authentication." for basic auth and
   *     "SECURITY./authentication/pki." for PKI
   * @param keys what keys to examine
   */
  Map<String, Long> countSecurityMetrics(
      MiniSolrCloudCluster cluster, String prefix, List<String> keys) {
    List<Map<String, Metric>> metrics = new ArrayList<>();
    cluster
        .getJettySolrRunners()
        .forEach(
            r -> {
              MetricRegistry registry =
                  r.getCoreContainer().getMetricManager().registry("solr.node");
              assertNotNull(registry);
              metrics.add(registry.getMetrics());
            });

    Map<String, Long> counts = new HashMap<>();
    keys.forEach(
        k -> {
          counts.put(k, sumCount(prefix, k, metrics));
        });
    return counts;
  }

  /** Common test method to be able to check auth metrics from any authentication plugin */
  void assertAuthMetricsMinimumsPrometheus(
      String handler,
      String registryName,
      Labels labels,
      int requests,
      int authenticated,
      int passThrough,
      int failWrongCredentials,
      int failMissingCredentials,
      int errors) {
    Map<String, Long> expectedCounts = new HashMap<>();
    expectedCounts.put("solr_core_authentication_plugin_requests", (long) requests);
    expectedCounts.put("solr_core_authentication_plugin_num_authenticated", (long) authenticated);
    expectedCounts.put("solr_core_authentication_plugin_num_pass_through", (long) passThrough);
    expectedCounts.put(
        "solr_core_authentication_plugin_fail_wrong_credentials", (long) failWrongCredentials);
    expectedCounts.put(
        "solr_core_authentication_plugin_fail_missing_credentials", (long) failMissingCredentials);
    expectedCounts.put("solr_core_authentication_plugin_errors", (long) errors);

    final Map<String, Long> counts =
        countSecurityMetricsPrometheus(cluster, AUTH_METRICS_KEYS, registryName, labels);
    final boolean success =
        expectedCounts.keySet().stream().allMatch(k -> counts.get(k) >= expectedCounts.get(k));

    assertTrue(
        "Expected metric minimums for handler "
            + handler
            + ": "
            + expectedCounts
            + ", but got: "
            + counts
            + "(Possible cause is delay in loading modified "
            + "security.json; see SOLR-13464 for test work around)",
        success);

    if (counts.get("solr_core_authentication_plugin_requests") > 0) {
      assertTrue(
          "requestTimes count not > 0",
          counts.get("solr_core_authentication_plugin_request_times") > 0);
      assertTrue("totalTime not > 0", counts.get("solr_core_authentication_plugin_total_time") > 0);
    }
  }

  /**
   * Common test method to sum the prometheus metrics from any authentication plugin from all solr
   * core containers
   */
  Map<String, Long> countSecurityMetricsPrometheus(
      MiniSolrCloudCluster cluster, List<String> keys, String registryName, Labels labels) {
    List<Map<String, DataPointSnapshot>> metrics = new ArrayList<>();
    cluster
        .getJettySolrRunners()
        .forEach(
            r -> {
              metrics.add(getMetricValues(r.getCoreContainer(), keys, registryName, labels));
            });

    Map<String, Long> counts = new HashMap<>();
    keys.forEach(
        k -> {
          counts.put(k, sumCountPrometheus(k, metrics));
        });
    return counts;
  }

  private long counterToLong(CounterSnapshot.CounterDataPointSnapshot metric) {
    if (metric == null) {
      return 0L;
    }
    return (long) metric.getValue();
  }

  private long histogramToLongCount(HistogramSnapshot.HistogramDataPointSnapshot metric) {
    if (metric == null) {
      return 0;
    }
    return metric.getCount();
  }

  // Have to sum the metrics from all three shards/nodes
  private long sumCountPrometheus(String key, List<Map<String, DataPointSnapshot>> metricsPerNode) {
    assertTrue("Metric " + key + " does not exist", metricsPerNode.get(0).containsKey(key));
    if (AUTH_METRICS_METER_KEYS.contains(key))
      return metricsPerNode.stream()
          .mapToLong(
              nodeMap -> counterToLong((CounterSnapshot.CounterDataPointSnapshot) nodeMap.get(key)))
          .sum();
    else if (AUTH_METRICS_TIMER_KEYS.contains(key)) {
      // Sum of the count of timer metrics (NOT the sum of their values)
      return metricsPerNode.stream()
          .mapToLong(
              nodeMap ->
                  histogramToLongCount(
                      (HistogramSnapshot.HistogramDataPointSnapshot) nodeMap.get(key)))
          .sum();
    } else {
      return metricsPerNode.stream()
          .mapToLong(
              nodeMap -> counterToLong((CounterSnapshot.CounterDataPointSnapshot) nodeMap.get(key)))
          .sum();
    }
  }

  private static Map<String, DataPointSnapshot> getMetricValues(
      CoreContainer coreContainer, List<String> metricNames, String registryName, Labels labels) {
    Map<String, DataPointSnapshot> metrics = new HashMap<>();
    PrometheusMetricReader prometheusMetricReader =
        SolrMetricTestUtils.getPrometheusMetricReader(coreContainer, registryName);
    for (String metricName : metricNames) {
      if ("solr_core_authentication_plugin_request_times".equals(metricName)) {
        HistogramSnapshot.HistogramDataPointSnapshot metric =
            SolrMetricTestUtils.getHistogramDatapoint(prometheusMetricReader, metricName, labels);
        metrics.put(metricName, metric);
      } else {
        CounterSnapshot.CounterDataPointSnapshot metric =
            SolrMetricTestUtils.getCounterDatapoint(prometheusMetricReader, metricName, labels);
        metrics.put(metricName, metric);
      }
    }
    return metrics;
  }

  /**
   * Common test method to be able to check audit metrics
   *
   * @param className the class name to be used for composing prefix, e.g.
   *     "SECURITY./auditlogging/SolrLogAuditLoggerPlugin"
   */
  protected void assertAuditMetricsMinimums(
      MiniSolrCloudCluster cluster, String className, int count, int errors)
      throws InterruptedException {
    String prefix = "SECURITY./auditlogging." + className + ".";
    Map<String, Long> expectedCounts = new HashMap<>();
    expectedCounts.put("count", (long) count);

    Map<String, Long> counts = countSecurityMetrics(cluster, prefix, AUDIT_METRICS_KEYS);
    boolean success = isMetricsEqualOrLarger(AUDIT_METRICS_TO_COMPARE, expectedCounts, counts);
    if (!success) {
      log.info("First metrics count assert failed, pausing 2s before re-attempt");
      Thread.sleep(2000);
      counts = countSecurityMetrics(cluster, prefix, AUDIT_METRICS_KEYS);
      success = isMetricsEqualOrLarger(AUDIT_METRICS_TO_COMPARE, expectedCounts, counts);
    }

    assertTrue(
        "Expected metric minimums for prefix "
            + prefix
            + ": "
            + expectedCounts
            + ", but got: "
            + counts,
        success);
  }

  private boolean isMetricsEqualOrLarger(
      List<String> metricsToCompare,
      Map<String, Long> expectedCounts,
      Map<String, Long> actualCounts) {
    return metricsToCompare.stream()
        .allMatch(k -> actualCounts.get(k).intValue() >= expectedCounts.get(k).intValue());
  }

  // Have to sum the metrics from all three shards/nodes
  private long sumCount(String prefix, String key, List<Map<String, Metric>> metrics) {
    assertTrue(
        "Metric " + prefix + key + " does not exist", metrics.get(0).containsKey(prefix + key));
    if (AUTH_METRICS_METER_KEYS.contains(key))
      return metrics.stream().mapToLong(l -> ((Meter) l.get(prefix + key)).getCount()).sum();
    else if (AUTH_METRICS_TIMER_KEYS.contains(key))
      return (long)
          ((long) 1000
              * metrics.stream()
                  .mapToDouble(l -> ((Timer) l.get(prefix + key)).getMeanRate())
                  .average()
                  .orElse(0.0d));
    else return metrics.stream().mapToLong(l -> ((Counter) l.get(prefix + key)).getCount()).sum();
  }

  public static void verifySecurityStatus(
      HttpClient cl, String url, String objPath, Object expected, int count) throws Exception {
    verifySecurityStatus(cl, url, objPath, expected, count, (String) null);
  }

  public static void verifySecurityStatus(
      HttpClient cl,
      String url,
      String objPath,
      Object expected,
      int count,
      String user,
      String pwd)
      throws Exception {
    verifySecurityStatus(cl, url, objPath, expected, count, makeBasicAuthHeader(user, pwd));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected static void verifySecurityStatus(
      HttpClient cl, String url, String objPath, Object expected, int count, String authHeader)
      throws IOException, InterruptedException {
    boolean success = false;
    String s = null;
    List<String> hierarchy = StrUtils.splitSmart(objPath, '/');
    for (int i = 0; i < count; i++) {
      HttpGet get = new HttpGet(url);
      if (authHeader != null) setAuthorizationHeader(get, authHeader);
      HttpResponse rsp = cl.execute(get);
      s = EntityUtils.toString(rsp.getEntity());
      Map m = null;
      try {
        m = (Map) Utils.fromJSONString(s);
      } catch (Exception e) {
        fail("Invalid json " + s);
      }
      Utils.consumeFully(rsp.getEntity());
      Object actual = Utils.getObjectByPath(m, true, hierarchy);
      if (expected instanceof Predicate predicate) {
        if (predicate.test(actual)) {
          success = true;
          break;
        }
      } else if (Objects.equals(actual == null ? null : String.valueOf(actual), expected)) {
        success = true;
        break;
      }
      Thread.sleep(50);
    }
    assertTrue("No match for " + objPath + " = " + expected + ", full response = " + s, success);
  }

  protected static String makeBasicAuthHeader(String user, String pwd) {
    String userPass = user + ":" + pwd;
    return "Basic " + Base64.getEncoder().encodeToString(userPass.getBytes(UTF_8));
  }

  public static void setAuthorizationHeader(AbstractHttpMessage httpMsg, String headerString) {
    httpMsg.setHeader(new BasicHeader("Authorization", headerString));
    log.info("Added Authorization Header {}", headerString);
  }

  /**
   * This helper method can be used by tests to monitor the current state of either <code>
   * "authentication"</code> or <code>"authorization"</code> plugins in use each node of the current
   * cluster.
   *
   * <p>This can be useful in a {@link TimeOut#waitFor} loop to monitor a cluster and "wait for" A
   * change in security settings to affect all nodes by comparing the objects in the current Map
   * with the one in use prior to executing some test command. (providing a work around for the
   * security user experienence limitations identified in <a
   * href="https://issues.apache.org/jira/browse/SOLR-13464">SOLR-13464</a> )
   *
   * @param url A REST url (or any arbitrary String) ending in <code>"authentication"</code> or
   *     <code>"authorization"</code> used to specify the type of plugins to introspect
   * @return A Map from <code>nodeName</code> to auth plugin
   */
  public static Map<String, Object> getAuthPluginsInUseForCluster(String url) {
    Map<String, Object> plugins = new HashMap<>();
    if (url.endsWith("authentication")) {
      for (JettySolrRunner r : cluster.getJettySolrRunners()) {
        plugins.put(r.getNodeName(), r.getCoreContainer().getAuthenticationPlugin());
      }
    } else if (url.endsWith("authorization")) {
      for (JettySolrRunner r : cluster.getJettySolrRunners()) {
        plugins.put(r.getNodeName(), r.getCoreContainer().getAuthorizationPlugin());
      }
    } else {
      fail("Test helper method assumptions broken: " + url);
    }
    return plugins;
  }
}
