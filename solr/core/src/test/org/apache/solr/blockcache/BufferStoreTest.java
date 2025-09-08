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
package org.apache.solr.blockcache;

import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferStoreTest extends SolrTestCase {
  private static final int blockSize = 1024;

  private Metrics metrics;
  private SolrMetricManager metricManager;
  private String registry;

  private Store store;

  @Before
  public void setup() {
    metrics = new Metrics();
    metricManager = new SolrMetricManager();
    registry = TestUtil.randomSimpleString(random(), 2, 10);
    String scope = TestUtil.randomSimpleString(random(), 2, 10);
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, registry, "foo");
    metrics.initializeMetrics(solrMetricsContext, Attributes.empty(), scope);
    BufferStore.initNewBuffer(blockSize, blockSize, metrics);
    store = BufferStore.instance(blockSize);
  }

  @After
  public void clearBufferStores() {
    BufferStore.clearBufferStores();
  }

  @Test
  public void testBufferTakePut() {
    byte[] b1 = store.takeBuffer(blockSize);

    assertGaugeMetricsChanged(false, false);

    byte[] b2 = store.takeBuffer(blockSize);
    byte[] b3 = store.takeBuffer(blockSize);

    assertRawMetricCounts(2, 0);
    assertGaugeMetricsChanged(true, false);

    store.putBuffer(b1);

    assertGaugeMetricsChanged(false, false);

    store.putBuffer(b2);
    store.putBuffer(b3);

    assertRawMetricCounts(0, 2);
    assertGaugeMetricsChanged(false, true);
  }

  private void assertRawMetricCounts(int allocated, int lost) {
    assertEquals(
        "Buffer allocation count is wrong.", allocated, metrics.shardBuffercacheAllocate.get());
    assertEquals("Lost buffer count is wrong", lost, metrics.shardBuffercacheLost.get());
  }

  /**
   * Stateful method to verify whether the amount of buffers allocated and lost since the last call
   * has changed.
   *
   * @param allocated whether buffers should have been allocated since the last call
   * @param lost whether buffers should have been lost since the last call
   */
  private void assertGaugeMetricsChanged(boolean allocated, boolean lost) {
    var gauge =
        metricManager.getPrometheusMetricReader(registry).collect().stream()
            .filter(ms -> ms.getMetadata().getPrometheusName().equals("solr_buffer_cache_stats"))
            .map(GaugeSnapshot.class::cast)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing gauge metric solr_buffer_cache_stats"));

    var actualAllocatedValue = getBufferCacheStatsValue(gauge, "allocations");
    var actualLostValue = getBufferCacheStatsValue(gauge, "lost");
    assertEquals(
        "Buffer allocation metric not updating correctly.", allocated, actualAllocatedValue > 0);
    assertEquals("Buffer allocation metric not updating correctly.", lost, actualLostValue > 0);
  }

  private Double getBufferCacheStatsValue(GaugeSnapshot gaugeSnapshot, String type) {
    return gaugeSnapshot.getDataPoints().stream()
        .filter(
            (dp) ->
                dp.getLabels()
                    .equals(
                        Labels.of(
                            "category",
                            "CACHE",
                            "otel_scope_name",
                            "org.apache.solr",
                            "type",
                            type)))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Missing type=" + type + " label on metric"))
        .getValue();
  }
}
