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
package org.apache.solr.search;

import static org.apache.solr.metrics.SolrMetricProducer.CATEGORY_ATTR;

import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import java.util.Optional;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSolrFieldCacheBean extends SolrTestCaseJ4 {

  private static final String ENTRIES_METRIC_NAME = "solr_core_field_cache_entries";
  private static final String SIZE_BYTES_METRIC_NAME = "solr_core_field_cache_size_bytes";
  private static final String DISABLE_ENTRY_LIST_PROPERTY = "disableSolrFieldCacheMBeanEntryList";
  private static final String DISABLE_ENTRY_LIST_JMX_PROPERTY =
      "disableSolrFieldCacheMBeanEntryListJmx";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-minimal.xml");
  }

  @Test
  public void testEntryList() {
    // Ensure entries to FieldCache
    assertU(adoc("id", "id0"));
    assertU(commit());
    assertQ(req("q", "*:*", "sort", "id asc"), "//*[@numFound='1']");

    // Test with entry list enabled
    assertEntryList(true);

    // Test again with entry list disabled
    System.setProperty("solr.metrics.fieldcache.entries.enabled", "false");
    try {
      assertEntryList(false);
    } finally {
      System.clearProperty("solr.metrics.fieldcache.entries.enabled");
    }

    // Test with entry list enabled again
    assertEntryList(true);

    // Test with entry list disabled for jmx
    System.setProperty("solr.metrics.fieldcache.entries.jmx.enabled", "false");
    try {
      assertEntryList(false);
    } finally {
      System.clearProperty("solr.metrics.fieldcache.entries.jmx.enabled");
    }
  }

  private void assertEntryList(boolean bytesMetricIncluded) {
    FieldCacheMetrics metrics = getFieldCacheMetrics();
    assertTrue(
        "Field cache entries count should be greater than 0",
        metrics.entries().get().getDataPoints().getFirst().getValue() > 0);

    if (bytesMetricIncluded) {
      assertTrue("Size bytes metric should be present", metrics.sizeBytes().isPresent());
    } else {
      assertTrue("Size bytes metric should not be present", metrics.sizeBytes().isEmpty());
    }
  }

  private FieldCacheMetrics getFieldCacheMetrics() {
    String registryName = TestUtil.randomSimpleString(random(), 1, 10);
    SolrMetricManager metricManager = h.getCoreContainer().getMetricManager();
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, registryName);

    try (SolrFieldCacheBean mbean = new SolrFieldCacheBean()) {
      mbean.initializeMetrics(
          solrMetricsContext, Attributes.of(CATEGORY_ATTR, SolrInfoBean.Category.CACHE.toString()));

      var metrics = metricManager.getPrometheusMetricReader(registryName).collect();

      var entryCount =
          metrics.stream()
              .filter(ms -> ENTRIES_METRIC_NAME.equals(ms.getMetadata().getPrometheusName()))
              .map(GaugeSnapshot.class::cast)
              .findFirst();

      var sizeBytes =
          metrics.stream()
              .filter(ms -> SIZE_BYTES_METRIC_NAME.equals(ms.getMetadata().getPrometheusName()))
              .map(GaugeSnapshot.class::cast)
              .findFirst();

      return new FieldCacheMetrics(entryCount, sizeBytes);
    }
  }

  private record FieldCacheMetrics(
      Optional<GaugeSnapshot> entries, Optional<GaugeSnapshot> sizeBytes) {}
}
