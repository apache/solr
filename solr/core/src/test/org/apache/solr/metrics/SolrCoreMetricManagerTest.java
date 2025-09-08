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
package org.apache.solr.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.reporters.MockMetricReporter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// NOCOMMIT: Need to fix up these tests to use the new SolrMetricTestUtils once we move off of
// Dropwizard
public class SolrCoreMetricManagerTest extends SolrTestCaseJ4 {
  private static final int MAX_ITERATIONS = 100;

  private SolrCoreMetricManager coreMetricManager;
  private SolrMetricManager metricManager;

  @Before
  public void beforeTest() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
    coreMetricManager = h.getCore().getCoreMetricManager();
    metricManager = h.getCore().getCoreContainer().getMetricManager();
  }

  @After
  public void afterTest() throws IOException {
    if (null != coreMetricManager) {
      coreMetricManager.close();
      assertTrue(metricManager.getReporters(coreMetricManager.getRegistryName()).isEmpty());
      deleteCore();
    }
  }

  @Test
  public void testRegisterMetrics() {
    Random random = random();

    String scope = SolrMetricTestUtils.getRandomScope(random);
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random);
    Map<String, Counter> metrics = SolrMetricTestUtils.getRandomMetrics(random);
    SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
    try {
      coreMetricManager.registerMetricProducer(scope, producer);
      assertNotNull(scope);
      assertNotNull(category);
      assertRegistered(scope, metrics, coreMetricManager);
    } catch (final IllegalArgumentException e) {
      assertTrue(
          "expected at least one null but got: scope=" + scope + ", category=" + category,
          (scope == null || category == null));
      assertRegistered(scope, new HashMap<>(), coreMetricManager);
    }
  }

  @Test
  public void testRegisterMetricsWithReplacements() {
    Random random = random();

    Map<String, Counter> registered = new HashMap<>();
    String scope = SolrMetricTestUtils.getRandomScope(random, true);
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random, true);

    int iterations = TestUtil.nextInt(random, 0, MAX_ITERATIONS);
    for (int i = 0; i < iterations; ++i) {
      Map<String, Counter> metrics =
          SolrMetricTestUtils.getRandomMetricsWithReplacements(random, registered);
      if (metrics.isEmpty()) {
        continue;
      }
      SolrMetricProducer producer = SolrMetricTestUtils.getProducerOf(category, scope, metrics);
      coreMetricManager.registerMetricProducer(scope, producer);
      registered.putAll(metrics);
      assertRegistered(scope, registered, coreMetricManager);
    }
  }

  @Test
  public void testLoadReporter() throws Exception {
    Random random = random();

    String className = MockMetricReporter.class.getName();
    String reporterName = TestUtil.randomUnicodeString(random);
    String taggedName = reporterName + "@" + coreMetricManager.getTag();

    Map<String, Object> attrs = new HashMap<>();
    attrs.put(FieldType.CLASS_NAME, className);
    attrs.put(CoreAdminParams.NAME, reporterName);

    boolean shouldDefineConfigurable = random.nextBoolean();
    String configurable = TestUtil.randomUnicodeString(random);
    if (shouldDefineConfigurable) attrs.put("configurable", configurable);

    boolean shouldDefinePlugin = random.nextBoolean();
    PluginInfo pluginInfo =
        shouldDefinePlugin ? new PluginInfo(TestUtil.randomUnicodeString(random), attrs) : null;

    try {
      metricManager.loadReporter(
          coreMetricManager.getRegistryName(),
          coreMetricManager.getCore(),
          pluginInfo,
          coreMetricManager.getTag());
      assertNotNull(pluginInfo);
      Map<String, SolrMetricReporter> reporters =
          metricManager.getReporters(coreMetricManager.getRegistryName());
      assertTrue(
          "reporters.size should be > 0, but was + " + reporters.size(), reporters.size() > 0);
      assertNotNull(
          "reporter " + reporterName + " not present among " + reporters,
          reporters.get(taggedName));
      assertTrue(
          "wrong reporter class: " + reporters.get(taggedName),
          reporters.get(taggedName) instanceof MockMetricReporter);
    } catch (IllegalArgumentException e) {
      assertTrue(pluginInfo == null || attrs.get("configurable") == null);
      assertNull(metricManager.getReporters(coreMetricManager.getRegistryName()).get(taggedName));
    }
  }

  private void assertRegistered(
      String scope, Map<String, Counter> newMetrics, SolrCoreMetricManager coreMetricManager) {
    if (scope == null || newMetrics == null) {
      return;
    }
    String filter = "." + scope + ".";
    MetricRegistry registry = metricManager.registry(coreMetricManager.getRegistryName());
    assertEquals(
        newMetrics.size(),
        registry.getMetrics().keySet().stream().filter(s -> s.contains(filter)).count());

    Map<String, Metric> registeredMetrics =
        registry.getMetrics().entrySet().stream()
            .filter(e -> e.getKey() != null && e.getKey().contains(filter))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    for (Map.Entry<String, Metric> entry : registeredMetrics.entrySet()) {
      String name = entry.getKey();
      Metric expectedMetric = entry.getValue();

      Metric actualMetric = registry.getMetrics().get(name);

      assertNotNull(actualMetric);
      assertEquals(expectedMetric, actualMetric);
    }
  }

  @Test
  public void testReregisterMetrics() {
    Random random = random();

    Map<String, Long> initialMetrics =
        SolrMetricTestUtils.getRandomPrometheusMetricsWithReplacements(random, new HashMap<>());
    var initialProducer = new SolrMetricTestUtils.TestSolrMetricProducer(coreName, initialMetrics);
    coreMetricManager.registerMetricProducer(
        SolrMetricTestUtils.getRandomScope(random, true), initialProducer);

    var labels = SolrMetricTestUtils.newStandaloneLabelsBuilder(h.getCore()).build();

    String randomMetricName = initialMetrics.entrySet().iterator().next().getKey();

    long actualValue =
        (long)
            SolrMetricTestUtils.getCounterDatapoint(h.getCore(), randomMetricName, labels)
                .getValue();
    long expectedValue = initialMetrics.get(randomMetricName);

    assertEquals(expectedValue, actualValue);

    // Change the metric value in OTEL
    initialProducer
        .getCounters()
        .get(randomMetricName)
        .add(10L, Attributes.of(AttributeKey.stringKey("core"), coreName));

    long newActualValue =
        (long)
            SolrMetricTestUtils.getCounterDatapoint(h.getCore(), randomMetricName, labels)
                .getValue();
    assertEquals(expectedValue + 10L, newActualValue);

    // Reregister the core metrics which should reset the metric value back to the initial value
    coreMetricManager.reregisterCoreMetrics();

    long reregisteredValue =
        (long)
            SolrMetricTestUtils.getCounterDatapoint(h.getCore(), randomMetricName, labels)
                .getValue();
    assertEquals(expectedValue, reregisteredValue);
  }

  @Test
  public void testNonCloudRegistryName() {
    String registryName = h.getCore().getCoreMetricManager().getRegistryName();
    String leaderRegistryName = h.getCore().getCoreMetricManager().getLeaderRegistryName();
    assertNotNull(registryName);
    assertEquals("solr.core.collection1", registryName);
    assertNull(leaderRegistryName);
  }
}
