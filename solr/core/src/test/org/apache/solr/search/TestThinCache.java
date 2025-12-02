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
import static org.apache.solr.metrics.SolrMetricProducer.NAME_ATTR;

import io.opentelemetry.api.common.Attributes;
import io.prometheus.metrics.model.snapshots.Labels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrMetricTestUtils;
import org.apache.solr.util.TestHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Test for {@link ThinCache}. */
public class TestThinCache extends SolrTestCaseJ4 {

  @ClassRule public static EmbeddedSolrServerTestRule solrRule = new EmbeddedSolrServerTestRule();
  public static final String SOLR_NODE_LEVEL_CACHE_XML =
      "<solr>\n"
          + "  <caches>\n"
          + "    <cache name='myNodeLevelCache'\n"
          + "      size='10'\n"
          + "      initialSize='10'\n"
          + "      />\n"
          + "    <cache name='myNodeLevelCacheThin'\n"
          + "      class='solr.ThinCache$NodeLevelCache'\n"
          + "      size='10'\n"
          + "      initialSize='10'\n"
          + "      />\n"
          + "  </caches>\n"
          + "</solr>";

  @BeforeClass
  public static void setupSolrHome() throws Exception {
    Path home = createTempDir("home");
    Files.writeString(home.resolve("solr.xml"), SOLR_NODE_LEVEL_CACHE_XML);

    solrRule.startSolr(home);

    Path configSet = createTempDir("configSet");
    copyMinConf(configSet);
    // insert a special filterCache configuration
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "</config>",
                "<query>\n"
                    + "<filterCache\n"
                    + "      class=\"solr.ThinCache\"\n"
                    + "      parentCacheName=\"myNodeLevelCacheThin\"\n"
                    + "      size=\"5\"\n"
                    + "      initialSize=\"5\"/>\n"
                    + "</query></config>"));

    solrRule.newCollection().withConfigSet(configSet.toString()).create();

    // legacy; get rid of this someday!
    h = new TestHarness(solrRule.getCoreContainer());
    lrf = h.getRequestFactory("/select", 0, 20);
  }

  SolrMetricManager metricManager = new SolrMetricManager(null);
  String registry = TestUtil.randomSimpleString(random(), 2, 10);
  String scope = TestUtil.randomSimpleString(random(), 2, 10);

  @Test
  public void testSimple() {
    Object cacheScope = new Object();
    ThinCache.NodeLevelCache<Object, Integer, String> backing = new ThinCache.NodeLevelCache<>();
    ThinCache<Object, Integer, String> lfuCache = new ThinCache<>();
    String lfuCacheName = "lfu_cache";
    lfuCache.setBacking(cacheScope, backing);
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(metricManager, registry);
    lfuCache.initializeMetrics(
        solrMetricsContext,
        Attributes.of(
            CATEGORY_ATTR, SolrInfoBean.Category.CACHE.toString(), NAME_ATTR, lfuCacheName),
        "solr_node_cache");

    Object cacheScope2 = new Object();
    ThinCache<Object, Integer, String> newLFUCache = new ThinCache<>();
    String newLfuCacheName = "new_lfu_cache";
    newLFUCache.setBacking(cacheScope2, backing);
    newLFUCache.initializeMetrics(
        solrMetricsContext,
        Attributes.of(
            CATEGORY_ATTR, SolrInfoBean.Category.CACHE.toString(), NAME_ATTR, newLfuCacheName),
        "solr_node_cache");

    Map<String, String> params = new HashMap<>();
    params.put("size", "100");
    params.put("initialSize", "10");

    NoOpRegenerator regenerator = new NoOpRegenerator();
    backing.init(params, null, null);
    Object initObj =
        lfuCache.init(Collections.singletonMap("autowarmCount", "25"), null, regenerator);
    lfuCache.setState(SolrCache.State.LIVE);
    for (int i = 0; i < 101; i++) {
      lfuCache.put(i + 1, Integer.toString(i + 1));
    }
    assertEquals("15", lfuCache.get(15));
    assertEquals("75", lfuCache.get(75));
    assertNull(lfuCache.get(110));

    var hits = getNodeCacheLookups(metricManager, registry, lfuCacheName, "hit");
    var miss = getNodeCacheLookups(metricManager, registry, lfuCacheName, "miss");
    var inserts = getNodeCacheOp(metricManager, registry, lfuCacheName, "inserts");
    assertEquals(3L, hits + miss);
    assertEquals(2L, hits);
    assertEquals(101L, inserts);

    assertNull(lfuCache.get(1)); // first item put in should be the first out

    // Test autowarming
    newLFUCache.init(Collections.singletonMap("autowarmCount", "25"), initObj, regenerator);
    newLFUCache.warm(null, lfuCache);
    newLFUCache.setState(SolrCache.State.LIVE);

    newLFUCache.put(103, "103");
    assertEquals("15", newLFUCache.get(15));
    assertEquals("75", newLFUCache.get(75));
    assertNull(newLFUCache.get(50));

    var newhits = getNodeCacheLookups(metricManager, registry, newLfuCacheName, "hit");
    var newmiss = getNodeCacheLookups(metricManager, registry, newLfuCacheName, "miss");
    var newinserts = getNodeCacheOp(metricManager, registry, newLfuCacheName, "inserts");
    var evictions = getNodeCacheOp(metricManager, registry, newLfuCacheName, "evictions");
    assertEquals(7L, newhits + newmiss);
    assertEquals(4L, newhits);
    assertEquals(102L, newinserts);
    assertEquals(0L, evictions);
  }

  @Test
  public void testInitCore() throws Exception {
    String thinCacheName = "myNodeLevelCacheThin";
    String nodeCacheName = "myNodeLevelCache";
    for (int i = 0; i < 20; i++) {
      assertU(adoc("id", Integer.toString(i)));
    }
    assertU(commit());
    assertQ(req("q", "*:*", "fq", "id:0"));
    assertQ(req("q", "*:*", "fq", "id:0"));
    assertQ(req("q", "*:*", "fq", "id:1"));

    var metricManager = h.getCoreContainer().getMetricManager();
    assertEquals(
        3L,
        getNodeCacheLookups(metricManager, "solr.node", thinCacheName, "hit")
            + getNodeCacheLookups(
                metricManager, "solr.node", thinCacheName, "miss")); // total lookups
    assertEquals(1L, getNodeCacheLookups(metricManager, "solr.node", thinCacheName, "hit"));
    assertEquals(2L, getNodeCacheOp(metricManager, "solr.node", thinCacheName, "inserts"));

    assertEquals(2, getNodeCacheSize(metricManager, "solr.node", thinCacheName));

    // for the other node-level cache, simply check that metrics are accessible
    assertEquals(0, getNodeCacheSize(metricManager, "solr.node", nodeCacheName));
  }

  private long getNodeCacheOp(
      SolrMetricManager metricManager, String registry, String cacheName, String operation) {
    var reader = metricManager.getPrometheusMetricReader(registry);
    return (long)
        SolrMetricTestUtils.getCounterDatapoint(
                reader,
                "solr_node_cache_ops",
                Labels.builder()
                    .label("category", "CACHE")
                    .label("ops", operation)
                    .label("name", cacheName)
                    .label("otel_scope_name", "org.apache.solr")
                    .build())
            .getValue();
  }

  private long getNodeCacheLookups(
      SolrMetricManager metricManager, String registry, String cacheName, String result) {
    var reader = metricManager.getPrometheusMetricReader(registry);
    var builder =
        Labels.builder()
            .label("category", "CACHE")
            .label("name", cacheName)
            .label("otel_scope_name", "org.apache.solr");
    if (result != null) builder.label("result", result);

    return (long)
        SolrMetricTestUtils.getCounterDatapoint(reader, "solr_node_cache_lookups", builder.build())
            .getValue();
  }

  private long getNodeCacheSize(
      SolrMetricManager metricManager, String registry, String cacheName) {
    var reader = metricManager.getPrometheusMetricReader(registry);
    return (long)
        SolrMetricTestUtils.getGaugeDatapoint(
                reader,
                "solr_node_cache_size",
                Labels.builder()
                    .label("category", "CACHE")
                    .label("name", cacheName)
                    .label("otel_scope_name", "org.apache.solr")
                    .build())
            .getValue();
  }
}
