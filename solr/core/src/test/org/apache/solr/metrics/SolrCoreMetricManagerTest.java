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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SolrCoreMetricManagerTest extends SolrTestCaseJ4 {
  private static final int MAX_ITERATIONS = 100;

  private SolrCoreMetricManager coreMetricManager;

  @Before
  public void beforeTest() throws Exception {
    initCore("solrconfig-basic.xml", "schema.xml");
    coreMetricManager = h.getCore().getCoreMetricManager();
  }

  @After
  public void afterTest() throws IOException {
    if (null != coreMetricManager) {
      coreMetricManager.close();
      deleteCore();
    }
  }

  @Test
  public void testRegisterMetrics() {
    Random random = random();
    SolrInfoBean.Category category = SolrMetricTestUtils.getRandomCategory(random);
    Map<String, Long> metrics =
        SolrMetricTestUtils.getRandomPrometheusMetricsWithReplacements(random, new HashMap<>());
    SolrMetricTestUtils.TestSolrMetricProducer producer =
        new SolrMetricTestUtils.TestSolrMetricProducer("coll", metrics);
    coreMetricManager.registerMetricProducer(producer, Attributes.empty());
    assertNotNull(category);
    assertRegistered(metrics, coreMetricManager);
  }

  private void assertRegistered(
      Map<String, Long> newMetrics, SolrCoreMetricManager coreMetricManager) {
    if (newMetrics == null) {
      return;
    }
    var reader =
        coreMetricManager
            .getSolrMetricsContext()
            .getMetricManager()
            .getPrometheusMetricReader(coreMetricManager.getRegistryName());

    // Check every metric that registered appears in the PrometheusMetricReader
    for (Map.Entry<String, Long> entry : newMetrics.entrySet()) {
      var metricSnapshots = reader.collect(name -> entry.getKey().equals(name));
      assertNotNull(metricSnapshots);
      var counterSnapshot = (CounterSnapshot) metricSnapshots.get(0);
      assertEquals(
          counterSnapshot.getDataPoints().getFirst().getValue(),
          newMetrics.get(counterSnapshot.getMetadata().getPrometheusName()),
          0.0);
    }
  }

  @Test
  public void testReregisterMetrics() {
    Random random = random();

    Map<String, Long> initialMetrics =
        SolrMetricTestUtils.getRandomPrometheusMetricsWithReplacements(random, new HashMap<>());
    var initialProducer = new SolrMetricTestUtils.TestSolrMetricProducer(coreName, initialMetrics);
    coreMetricManager.registerMetricProducer(initialProducer, Attributes.empty());

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
    assertNotNull(registryName);
    assertEquals("solr.core.collection1", registryName);
  }

  /** Check the metric registry specific to a core is removed once the core is unloaded. */
  @Test
  public void testNoRegistryAfterUnload() throws Exception {

    String coreRegistryName;

    CoreContainer cc = h.getCoreContainer();
    SolrMetricManager metricManager = cc.getMetricManager();

    SolrCore core = cc.create("to-unload", Map.of("configSet", "minimal"));
    coreRegistryName = core.getSolrMetricsContext().getRegistryName();

    assertNotNull("missing registry", metricManager.getPrometheusMetricReader(coreRegistryName));

    // Commit and wait for searcher to ensure the searcher is created. This is required to make sure
    // the core inner thread does not create it *after* we asked the container to unload the core.
    SolrParams params = new MapSolrParams(Map.of(UpdateParams.WAIT_SEARCHER, "true"));
    core.getUpdateHandler()
        .commit(new CommitUpdateCommand(new SolrQueryRequestBase(h.getCore(), params) {}, false));

    cc.unload("to-unload");

    // Make sure the core metric registry does not exist anymore once we fully removed the core
    // from the container
    assertFalse(metricManager.getPrometheusMetricReaders().containsKey(coreRegistryName));
  }
}
