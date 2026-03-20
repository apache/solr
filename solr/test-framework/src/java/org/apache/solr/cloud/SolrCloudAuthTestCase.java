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
import org.apache.solr.client.solrj.apache.HttpClientUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.util.SolrMetricTestUtils;
import org.apache.solr.util.TimeOut;
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
          "solr_authentication_errors",
          "solr_authentication_requests",
          "solr_authentication_num_authenticated",
          "solr_authentication_num_pass_through",
          "solr_authentication_failures/wrong_credentials",
          "solr_authentication_failures/missing_credentials",
          "solr_authentication_request_times_nanoseconds");
  private static final List<String> AUTH_METRICS_METER_KEYS =
      Arrays.asList("solr_authentication_errors", "count");
  private static final List<String> AUTH_METRICS_TIMER_KEYS =
      Collections.singletonList("solr_authentication_request_times_nanoseconds");

  @SuppressWarnings({"rawtypes"})
  public static final Predicate NOT_NULL_PREDICATE = o -> o != null;

  @BeforeClass
  public static void enableMetrics() {
    System.setProperty("metricsEnabled", "true");
  }

  /** Used to check metric counts for PKI auth */
  protected void assertPkiAuthMetricsMinimums(
      int requests,
      int authenticated,
      int passThrough,
      int failWrongCredentials,
      int failMissingCredentials,
      int errors) {
    String handler = "/authentication/pki";
    String registryName = "solr.node";
    Labels labels =
        Labels.of(
            "otel_scope_name",
            "org.apache.solr",
            "category",
            "SECURITY",
            "handler",
            handler,
            "plugin_name",
            PKIAuthenticationPlugin.class.getSimpleName());
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
      Class<? extends AuthenticationPlugin> authPluginClass,
      int requests,
      int authenticated,
      int passThrough,
      int failWrongCredentials,
      int failMissingCredentials,
      int errors) {
    String handler = "/authentication";
    String registryName = "solr.node";
    Labels labels =
        Labels.of(
            "otel_scope_name",
            "org.apache.solr",
            "category",
            "SECURITY",
            "handler",
            handler,
            "plugin_name",
            authPluginClass.getSimpleName());
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
    expectedCounts.put("solr_authentication_requests", (long) requests);
    expectedCounts.put("solr_authentication_num_authenticated", (long) authenticated);
    expectedCounts.put("solr_authentication_num_pass_through", (long) passThrough);
    expectedCounts.put(
        "solr_authentication_failures/wrong_credentials", (long) failWrongCredentials);
    expectedCounts.put(
        "solr_authentication_failures/missing_credentials", (long) failMissingCredentials);
    expectedCounts.put("solr_authentication_errors", (long) errors);

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

    if (counts.get("solr_authentication_requests") > 0) {
      assertTrue(
          "requestTimes count not > 0",
          counts.get("solr_authentication_request_times_nanoseconds") > 0);
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
    if (AUTH_METRICS_METER_KEYS.contains(key)) {
      return metricsPerNode.stream()
          .mapToLong(
              nodeMap -> counterToLong((CounterSnapshot.CounterDataPointSnapshot) nodeMap.get(key)))
          .sum();
    } else if (AUTH_METRICS_TIMER_KEYS.contains(key)) {
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
      if ("solr_authentication_request_times_nanoseconds".equals(metricName)) {
        HistogramSnapshot.HistogramDataPointSnapshot metric =
            SolrMetricTestUtils.getHistogramDatapoint(prometheusMetricReader, metricName, labels);
        metrics.put(metricName, metric);
      } else if ("solr_authentication_failures/wrong_credentials".equals(metricName)) {
        // Fake metric name, actual metric will be in solr_authentication_failures with label type:
        // wrong_credentials
        Labels wrongCredsLabels = Labels.of("type", "wrong_credentials").merge(labels);
        CounterSnapshot.CounterDataPointSnapshot wrongCredsMetric =
            SolrMetricTestUtils.getCounterDatapoint(
                prometheusMetricReader, "solr_authentication_failures", wrongCredsLabels);

        metrics.put("solr_authentication_failures/wrong_credentials", wrongCredsMetric);
      } else if ("solr_authentication_failures/missing_credentials".equals(metricName)) {
        // Fake metric name, actual metric will be in solr_authentication_failures with label type:
        // missing_credentials
        Labels missingCredsLabels = Labels.of("type", "missing_credentials").merge(labels);
        CounterSnapshot.CounterDataPointSnapshot missingCredsMetric =
            SolrMetricTestUtils.getCounterDatapoint(
                prometheusMetricReader, "solr_authentication_failures", missingCredsLabels);
        metrics.put("solr_authentication_failures/missing_credentials", missingCredsMetric);
      } else {
        CounterSnapshot.CounterDataPointSnapshot metric =
            SolrMetricTestUtils.getCounterDatapoint(prometheusMetricReader, metricName, labels);
        metrics.put(metricName, metric);
      }
    }
    return metrics;
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
      HttpClientUtil.consumeFully(rsp.getEntity());
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
   * with the one in use prior to executing some test command. (providing a workaround for the
   * security user experience limitations identified in <a
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
