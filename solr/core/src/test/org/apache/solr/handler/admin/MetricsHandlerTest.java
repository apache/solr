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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.SettableGauge;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.prometheus.SolrPrometheusExporter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link MetricsHandler} */
public class MetricsHandlerTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {

    initCore("solrconfig-minimal.xml", "schema.xml");
    h.getCoreContainer().waitForLoadingCoresToFinish(30000);

    // manually register & seed some metrics in solr.jvm and solr.jetty for testing via handler
    // (use "solrtest_" prefix just in case the jvm or jetty adds a "foo" metric at some point)
    Counter c = h.getCoreContainer().getMetricManager().counter(null, "solr.jvm", "solrtest_foo");
    c.inc();
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "solrtest_foo");
    c.inc(2);
    // test escapes
    c = h.getCoreContainer().getMetricManager().counter(null, "solr.jetty", "solrtest_foo:bar");
    c.inc(3);

    // Manually register for Prometheus exporter tests
    registerGauge("solr.jvm", "gc.G1-Old-Generation.count");
    registerGauge("solr.jvm", "gc.G1-Old-Generation.time");
    registerGauge("solr.jvm", "memory.heap.committed");
    registerGauge("solr.jvm", "memory.pools.CodeHeap-'non-nmethods'.committed");
    registerGauge("solr.jvm", "threads.count");
    registerGauge("solr.jvm", "os.availableProcessors");
    registerGauge("solr.jvm", "buffers.direct.Count");
    registerGauge("solr.jvm", "buffers.direct.MemoryUsed");
    h.getCoreContainer()
        .getMetricManager()
        .meter(null, "solr.jetty", "org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses");
    h.getCoreContainer()
        .getMetricManager()
        .counter(
            null, "solr.jetty", "org.eclipse.jetty.server.handler.DefaultHandler.active-requests");
    h.getCoreContainer()
        .getMetricManager()
        .timer(null, "solr.jetty", "org.eclipse.jetty.server.handler.DefaultHandler.dispatches");
  }

  @AfterClass
  public static void cleanupMetrics() {
    if (null != h) {
      h.getCoreContainer().getMetricManager().registry("solr.jvm").remove("solrtest_foo");
      h.getCoreContainer().getMetricManager().registry("solr.jetty").remove("solrtest_foo");
      h.getCoreContainer().getMetricManager().registry("solr.jetty").remove("solrtest_foo:bar");
    }
  }

  @Test
  public void test() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json"),
        resp);
    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    NamedList<?> nl = (NamedList<?>) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof MapWriter);
    // response wasn't serialized, so we get here whatever MetricUtils produced instead of NamedList
    assertNotNull(((MapWriter) o)._get("count", null));
    assertEquals(0L, ((MapWriter) nl.get("SEARCHER.new.errors"))._get("count", null));
    assertNotNull(nl.get("INDEX.segments")); // int gauge
    assertTrue((int) ((MapWriter) nl.get("INDEX.segments"))._get("value", null) >= 0);
    assertNotNull(nl.get("INDEX.sizeInBytes")); // long gauge
    assertTrue((long) ((MapWriter) nl.get("INDEX.sizeInBytes"))._get("value", null) >= 0);
    nl = (NamedList<?>) values.get("solr.node");
    assertNotNull(nl.get("CONTAINER.cores.loaded")); // int gauge
    assertEquals(1, ((MapWriter) nl.get("CONTAINER.cores.loaded"))._get("value", null));
    assertNotNull(nl.get("ADMIN./admin/authorization.clientErrors")); // timer type
    Map<String, Object> map = new HashMap<>();
    ((MapWriter) nl.get("ADMIN./admin/authorization.clientErrors")).toMap(map);
    assertEquals(5, map.size());

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "jvm,jetty"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    // "collection" works too, because it's a prefix for "collection1"
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "registry",
            "solr.core.collection,solr.jvm"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.core.collection1"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    // "collection" works too, because it's a prefix for "collection1"
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "registry",
            "solr.core.collection",
            "registry",
            "solr.jvm"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.core.collection1"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "jvm,jetty"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "jvm",
            "group",
            "jetty"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(2, values.size());
    assertNotNull(values.get("solr.jetty"));
    assertNotNull(values.get("solr.jvm"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "node",
            "type",
            "counter"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(1, values.size());
    values = (NamedList<?>) values.get("solr.node");
    assertNotNull(values);
    assertNull(values.get("ADMIN./admin/authorization.errors")); // this is a timer node

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "prefix",
            "CONTAINER.cores,CONTAINER.threadPool"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(1, values.size());
    assertNotNull(values.get("solr.node"));
    values = (NamedList<?>) values.get("solr.node");
    assertEquals(27, values.size());
    assertNotNull(values.get("CONTAINER.cores.lazy")); // this is a gauge node
    assertNotNull(values.get("CONTAINER.threadPool.coreContainerWorkExecutor.completed"));
    assertNotNull(values.get("CONTAINER.threadPool.coreLoadExecutor.completed"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "prefix",
            "CONTAINER.cores",
            "regex",
            "C.*thread.*completed"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertNotNull(values.get("solr.node"));
    values = (NamedList<?>) values.get("solr.node");
    assertEquals(7, values.size());
    assertNotNull(values.get("CONTAINER.threadPool.coreContainerWorkExecutor.completed"));
    assertNotNull(values.get("CONTAINER.threadPool.coreLoadExecutor.completed"));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            "prefix",
            "CACHE.core.fieldCache",
            "property",
            "entries_count",
            MetricsHandler.COMPACT_PARAM,
            "true"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertNotNull(values.get("solr.core.collection1"));
    values = (NamedList<?>) values.get("solr.core.collection1");
    assertEquals(1, values.size());
    MapWriter writer = (MapWriter) values.get("CACHE.core.fieldCache");
    assertNotNull(writer);
    assertNotNull(writer._get("entries_count", null));

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "jvm",
            "prefix",
            "CONTAINER.cores"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    assertEquals(0, values.size());

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "json",
            "group",
            "node",
            "type",
            "timer",
            "prefix",
            "CONTAINER.cores"),
        resp);
    values = resp.getValues();
    assertNotNull(values.get("metrics"));
    SimpleOrderedMap<?> map1 = (SimpleOrderedMap<?>) values.get("metrics");
    assertEquals(0, map1.size());
    handler.close();
  }

  @Test
  public void testCompact() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true"),
        resp);
    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    NamedList<?> nl = (NamedList<?>) values.get("solr.core.collection1");
    assertNotNull(nl);
    Object o = nl.get("SEARCHER.new.errors");
    assertNotNull(o); // counter type
    assertTrue(o instanceof Number);
    handler.close();
  }

  @Test
  public void testPropertyFilter() throws Exception {
    assertQ(req("*:*"), "//result[@numFound='0']");

    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true",
            "group",
            "core",
            "prefix",
            "CACHE.searcher"),
        resp);
    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    NamedList<?> nl = (NamedList<?>) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertTrue(nl.size() > 0);
    nl.forEach(
        (k, v) -> {
          assertTrue(v instanceof MapWriter);
          Map<String, Object> map = new HashMap<>();
          ((MapWriter) v).toMap(map);
          assertTrue(map.size() > 2);
        });

    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true",
            "group",
            "core",
            "prefix",
            "CACHE.searcher",
            "property",
            "inserts",
            "property",
            "size"),
        resp);
    values = resp.getValues();
    values = (NamedList<?>) values.get("metrics");
    nl = (NamedList<?>) values.get("solr.core.collection1");
    assertNotNull(nl);
    assertTrue(nl.size() > 0);
    nl.forEach(
        (k, v) -> {
          assertTrue(v instanceof MapWriter);
          Map<String, Object> map = new HashMap<>();
          ((MapWriter) v).toMap(map);
          assertEquals("k=" + k + ", v=" + map, 2, map.size());
          assertNotNull(map.get("inserts"));
          assertNotNull(map.get("size"));
        });
    handler.close();
  }

  @Test
  public void testKeyMetrics() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    String key1 = "solr.core.collection1:CACHE.core.fieldCache";
    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            key1),
        resp);
    NamedList<?> values = resp.getValues();
    Object val = values.findRecursive("metrics", key1);
    assertNotNull(val);
    assertTrue(val instanceof MapWriter);
    assertTrue(((MapWriter) val)._size() >= 2);

    String key2 = "solr.core.collection1:CACHE.core.fieldCache:entries_count";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            key2),
        resp);
    val = resp.getValues()._get("metrics/" + key2, null);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    String key3 = "solr.jetty:solrtest_foo\\:bar";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            key3),
        resp);

    val = resp.getValues()._get("metrics/" + key3, null);
    assertNotNull(val);
    assertTrue(val instanceof Number);
    assertEquals(3, ((Number) val).intValue());

    // test multiple keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            key1,
            MetricsHandler.KEY_PARAM,
            key2,
            MetricsHandler.KEY_PARAM,
            key3),
        resp);

    val = resp.getValues()._get("metrics/" + key1, null);
    assertNotNull(val);
    val = resp.getValues()._get("metrics/" + key2, null);
    assertNotNull(val);
    val = resp.getValues()._get("metrics/" + key3, null);
    assertNotNull(val);

    String key4 = "solr.core.collection1:QUERY./select.requestTimes:1minRate";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            key4),
        resp);
    // the key contains a slash, need explicit list of path elements
    val = resp.getValues()._get(Arrays.asList("metrics", key4), null);
    assertNotNull(val);
    assertTrue(val instanceof Number);

    // test errors

    // invalid keys
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            "foo",
            MetricsHandler.KEY_PARAM,
            "foo:bar:baz:xyz"),
        resp);
    values = resp.getValues();
    NamedList<?> metrics = (NamedList<?>) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo"));
    assertNotNull(values.findRecursive("errors", "foo:bar:baz:xyz"));

    // unknown registry
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            "foo:bar:baz"),
        resp);
    values = resp.getValues();
    metrics = (NamedList<?>) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "foo:bar:baz"));

    // unknown metric
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.KEY_PARAM,
            "solr.jetty:unknown:baz"),
        resp);
    values = resp.getValues();
    metrics = (NamedList<?>) values.get("metrics");
    assertEquals(0, metrics.size());
    assertNotNull(values.findRecursive("errors", "solr.jetty:unknown:baz"));

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExprMetrics() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    String key1 = "solr\\.core\\..*:.*/select\\.request.*:.*Rate";
    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.EXPR_PARAM,
            key1),
        resp);
    // response structure is like in the case of non-key params
    Object val =
        resp.getValues()
            .findRecursive("metrics", "solr.core.collection1", "QUERY./select.requestTimes");
    assertNotNull(val);
    assertTrue(val instanceof MapWriter);
    Map<String, Object> map = new HashMap<>();
    ((MapWriter) val).toMap(map);
    assertEquals(map.toString(), 4, map.size()); // mean, 1, 5, 15
    assertNotNull(map.toString(), map.get("meanRate"));
    assertNotNull(map.toString(), map.get("1minRate"));
    assertNotNull(map.toString(), map.get("5minRate"));
    assertNotNull(map.toString(), map.get("15minRate"));
    assertEquals(map.toString(), ((Number) map.get("1minRate")).doubleValue(), 0.0, 0.0);
    map.clear();

    String key2 = "solr\\.core\\..*:.*/select\\.request.*";
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.EXPR_PARAM,
            key2),
        resp);
    // response structure is like in the case of non-key params
    val = resp.getValues().findRecursive("metrics", "solr.core.collection1");
    assertNotNull(val);
    Object v = ((SimpleOrderedMap<Object>) val).get("QUERY./select.requestTimes");
    assertNotNull(v);
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertEquals(map.toString(), 14, map.size());
    assertNotNull(map.toString(), map.get("1minRate"));
    assertEquals(map.toString(), ((Number) map.get("1minRate")).doubleValue(), 0.0, 0.0);
    map.clear();
    // select requests counter
    v = ((SimpleOrderedMap<Object>) val).get("QUERY./select.requests");
    assertNotNull(v);
    assertTrue(v instanceof Number);

    // test multiple expressions producing overlapping metrics - should be no dupes

    // this key matches also sub-metrics of /select, eg. /select[shard], ...
    String key3 = "solr\\.core\\..*:.*/select.*\\.requestTimes:count";
    resp = new SolrQueryResponse();
    // ORDER OF PARAMS MATTERS HERE! see the refguide
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.EXPR_PARAM,
            key2,
            MetricsHandler.EXPR_PARAM,
            key1,
            MetricsHandler.EXPR_PARAM,
            key3),
        resp);
    val = resp.getValues().findRecursive("metrics", "solr.core.collection1");
    assertNotNull(val);
    // for requestTimes only the full set of values from the first expr should be present
    assertNotNull(val);
    SimpleOrderedMap<Object> values = (SimpleOrderedMap<Object>) val;
    assertEquals(values.jsonStr(), 3, values.size());
    v = values.get("QUERY./select.requestTimes");
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertTrue(map.toString(), map.containsKey("count"));
    map.clear();
    v = values.get("QUERY./select[shard].requestTimes");
    assertTrue(v instanceof MapWriter);
    ((MapWriter) v).toMap(map);
    assertTrue(map.toString(), map.containsKey("count"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrometheusMetricsCore() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "prometheus"),
        resp);

    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    SolrPrometheusExporter exporter = (SolrPrometheusExporter) values.get("solr.core.collection1");
    assertNotNull(exporter);
    MetricSnapshots actualSnapshots = exporter.collect();
    assertNotNull(actualSnapshots);

    MetricSnapshot actualSnapshot =
        getMetricSnapshot(actualSnapshots, "solr_metrics_core_average_request_time");
    GaugeSnapshot.GaugeDataPointSnapshot actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot,
            Labels.of("category", "QUERY", "core", "collection1", "handler", "/select[shard]"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_requests");
    CounterSnapshot.CounterDataPointSnapshot actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot,
            Labels.of(
                "category",
                "QUERY",
                "core",
                "collection1",
                "handler",
                "/select[shard]",
                "type",
                "requests"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_cache");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot,
            Labels.of("cacheType", "fieldValueCache", "core", "collection1", "item", "hits"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_highlighter_requests");
    actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot,
            Labels.of("item", "default", "core", "collection1", "type", "SolrFragmenter"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_requests_time");
    actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot,
            Labels.of("category", "QUERY", "core", "collection1", "handler", "/select[shard]"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_searcher_documents");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("core", "collection1", "type", "numDocs"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_core_update_handler");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot,
            Labels.of(
                "category",
                "UPDATE",
                "core",
                "collection1",
                "type",
                "adds",
                "handler",
                "updateHandler"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot =
        getMetricSnapshot(actualSnapshots, "solr_metrics_core_average_searcher_warmup_time");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("core", "collection1", "type", "warmup"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrometheusMetricsNode() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "prometheus"),
        resp);

    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    SolrPrometheusExporter exporter = (SolrPrometheusExporter) values.get("solr.node");
    assertNotNull(exporter);
    MetricSnapshots actualSnapshots = exporter.collect();
    assertNotNull(actualSnapshots);

    MetricSnapshot actualSnapshot =
        getMetricSnapshot(actualSnapshots, "solr_metrics_node_requests");

    CounterSnapshot.CounterDataPointSnapshot actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot,
            Labels.of("category", "ADMIN", "handler", "/admin/info", "type", "requests"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_node_requests_time");
    actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot, Labels.of("category", "ADMIN", "handler", "/admin/info"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_node_thread_pool");
    actualCounterDataPoint =
        getCounterDatapointSnapshot(
            actualSnapshot,
            Labels.of(
                "category",
                "ADMIN",
                "executer",
                "parallelCoreAdminExecutor",
                "handler",
                "/admin/cores",
                "task",
                "completed"));
    assertEquals(0, actualCounterDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_node_connections");
    GaugeSnapshot.GaugeDataPointSnapshot actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot,
            Labels.of(
                "category",
                "UPDATE",
                "handler",
                "updateShardHandler",
                "item",
                "availableConnections"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_node_core_root_fs_bytes");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("category", "CONTAINER", "item", "totalSpace"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_node_cores");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("category", "CONTAINER", "item", "lazy"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrometheusMetricsJvm() throws Exception {
    // Some JVM metrics are non-deterministic due to testing environment such as
    // availableProcessors. We confirm snapshot exists and is nonNull instead.
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "prometheus"),
        resp);

    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    SolrPrometheusExporter exporter = (SolrPrometheusExporter) values.get("solr.jvm");
    assertNotNull(exporter);
    MetricSnapshots actualSnapshots = exporter.collect();
    assertNotNull(actualSnapshots);

    MetricSnapshot actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_gc");
    GaugeSnapshot.GaugeDataPointSnapshot actualGaugeDataPoint =
        getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "G1-Old-Generation"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_threads");
    actualGaugeDataPoint = getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "count"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_memory_pools_bytes");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("item", "committed", "space", "CodeHeap-'non-nmethods'"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_os");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "availableProcessors"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_buffers");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "Count", "pool", "direct"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_heap");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "committed", "memory", "heap"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_buffers_bytes");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(
            actualSnapshot, Labels.of("item", "MemoryUsed", "pool", "direct"));
    assertNotNull(actualGaugeDataPoint);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jvm_gc_seconds");
    actualGaugeDataPoint =
        getGaugeDatapointSnapshot(actualSnapshot, Labels.of("item", "G1-Old-Generation"));
    assertEquals(0, actualGaugeDataPoint.getValue(), 0);

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrometheusMetricsJetty() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            MetricsHandler.COMPACT_PARAM,
            "false",
            CommonParams.WT,
            "prometheus"),
        resp);

    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    SolrPrometheusExporter exporter = (SolrPrometheusExporter) values.get("solr.jetty");
    assertNotNull(exporter);
    MetricSnapshots actualSnapshots = exporter.collect();
    assertNotNull(actualSnapshots);

    MetricSnapshot actualSnapshot =
        getMetricSnapshot(actualSnapshots, "solr_metrics_jetty_response");
    CounterSnapshot.CounterDataPointSnapshot actualCounterDatapoint =
        getCounterDatapointSnapshot(actualSnapshot, Labels.of("status", "2xx"));
    assertEquals(0, actualCounterDatapoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jetty_requests");
    actualCounterDatapoint =
        getCounterDatapointSnapshot(actualSnapshot, Labels.of("method", "active"));
    assertEquals(0, actualCounterDatapoint.getValue(), 0);

    actualSnapshot = getMetricSnapshot(actualSnapshots, "solr_metrics_jetty_dispatches");
    actualCounterDatapoint = getCounterDatapointSnapshot(actualSnapshot, Labels.of());
    assertEquals(0, actualCounterDatapoint.getValue(), 0);

    handler.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrometheusMetricsFilter() throws Exception {
    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "prometheus",
            "group",
            "core",
            "type",
            "counter",
            "prefix",
            "QUERY"),
        resp);

    NamedList<?> values = resp.getValues();
    assertNotNull(values.get("metrics"));
    values = (NamedList<?>) values.get("metrics");
    SolrPrometheusExporter exporter = (SolrPrometheusExporter) values.get("solr.core.collection1");
    assertNotNull(exporter);
    MetricSnapshots actualSnapshots = exporter.collect();
    assertNotNull(actualSnapshots);

    actualSnapshots.forEach(
        (k) -> {
          k.getDataPoints()
              .forEach(
                  (datapoint) -> {
                    assertTrue(datapoint instanceof CounterSnapshot.CounterDataPointSnapshot);
                    assertEquals("QUERY", datapoint.getLabels().get("category"));
                  });
        });

    handler.close();
  }

  @Test
  public void testMetricsUnload() throws Exception {

    SolrCore core = h.getCoreContainer().getCore("collection1");
    // .getRequestHandlers().put("/dumphandler", new DumpRequestHandler());
    RefreshablePluginHolder pluginHolder = null;
    try {
      PluginInfo info =
          new PluginInfo(
              SolrRequestHandler.TYPE,
              Map.of("name", "/dumphandler", "class", DumpRequestHandler.class.getName()));
      DumpRequestHandler requestHandler = new DumpRequestHandler();
      requestHandler.gaugevals = Map.of("d_k1", "v1", "d_k2", "v2");
      pluginHolder = new RefreshablePluginHolder(info, requestHandler);
      core.getRequestHandlers().put("/dumphandler", pluginHolder);

    } finally {
      core.close();
    }

    MetricsHandler handler = new MetricsHandler(h.getCoreContainer());

    SolrQueryResponse resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true",
            "key",
            "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertEquals(
        "v1",
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k1"),
                null));
    assertEquals(
        "v2",
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k2"),
                null));
    pluginHolder.closeHandler();
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true",
            "key",
            "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertNull(
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k1"),
                null));
    assertNull(
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k2"),
                null));

    DumpRequestHandler requestHandler = new DumpRequestHandler();
    requestHandler.gaugevals = Map.of("d_k1", "v1.1", "d_k2", "v2.1");
    pluginHolder.reset(requestHandler);
    resp = new SolrQueryResponse();
    handler.handleRequestBody(
        req(
            CommonParams.QT,
            "/admin/metrics",
            CommonParams.WT,
            "json",
            MetricsHandler.COMPACT_PARAM,
            "true",
            "key",
            "solr.core.collection1:QUERY./dumphandler.dumphandlergauge"),
        resp);

    assertEquals(
        "v1.1",
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k1"),
                null));
    assertEquals(
        "v2.1",
        resp.getValues()
            ._getStr(
                Arrays.asList(
                    "metrics", "solr.core.collection1:QUERY./dumphandler.dumphandlergauge", "d_k2"),
                null));

    handler.close();
  }

  public static void registerGauge(String registry, String metricName) {
    Gauge<Number> metric =
        new SettableGauge<>() {
          @Override
          public void setValue(Number value) {}

          @Override
          public Number getValue() {
            return 0;
          }
        };
    h.getCoreContainer()
        .getMetricManager()
        .registerGauge(
            null,
            registry,
            metric,
            "",
            SolrMetricManager.ResolutionStrategy.IGNORE,
            metricName,
            "");
  }

  private MetricSnapshot getMetricSnapshot(MetricSnapshots snapshots, String metricName) {
    return snapshots.stream()
        .filter(ss -> ss.getMetadata().getPrometheusName().equals(metricName))
        .findAny()
        .get();
  }

  private GaugeSnapshot.GaugeDataPointSnapshot getGaugeDatapointSnapshot(
      MetricSnapshot snapshot, Labels labels) {
    return (GaugeSnapshot.GaugeDataPointSnapshot)
        snapshot.getDataPoints().stream()
            .filter(ss -> ss.getLabels().hasSameValues(labels))
            .findAny()
            .get();
  }

  private CounterSnapshot.CounterDataPointSnapshot getCounterDatapointSnapshot(
      MetricSnapshot snapshot, Labels labels) {
    return (CounterSnapshot.CounterDataPointSnapshot)
        snapshot.getDataPoints().stream()
            .filter(ss -> ss.getLabels().hasSameValues(labels))
            .findAny()
            .get();
  }

  static class RefreshablePluginHolder extends PluginBag.PluginHolder<SolrRequestHandler> {

    private DumpRequestHandler rh;
    private SolrMetricsContext metricsInfo;

    public RefreshablePluginHolder(PluginInfo info, DumpRequestHandler rh) {
      super(info);
      this.rh = rh;
    }

    @Override
    public boolean isLoaded() {
      return true;
    }

    void closeHandler() throws Exception {
      this.metricsInfo = rh.getSolrMetricsContext();
      //      if(metricsInfo.tag.contains(String.valueOf(rh.hashCode()))){
      //        //this created a new child metrics
      //        metricsInfo = metricsInfo.getParent();
      //      }
      this.rh.close();
    }

    void reset(DumpRequestHandler rh) {
      this.rh = rh;
      if (metricsInfo != null) this.rh.initializeMetrics(metricsInfo, "/dumphandler");
    }

    @Override
    public SolrRequestHandler get() {
      return rh;
    }
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
    public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
      super.initializeMetrics(parentContext, scope);
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
