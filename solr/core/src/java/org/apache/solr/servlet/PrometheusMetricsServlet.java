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
package org.apache.solr.servlet;

import static org.apache.solr.util.circuitbreaker.CircuitBreakerRegistry.getTimesTrippedMetrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.ServletOutputStream;
import javax.servlet.UnavailableException;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.storage.CompressingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FullStory: a simple servlet to produce a few prometheus metrics. This servlet exists for
 * backwards compatibility and will be removed in favor of the native prometheus-exporter.
 */
public final class PrometheusMetricsServlet extends BaseSolrServlet {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // values less than this threshold are considered invalid; mark the invalid values instead of
  // failing the call.
  private static final Integer INVALID_NUMBER = -1;

  private final List<MetricsApiCaller> callers = getCallers();

  private List<MetricsApiCaller> getCallers() {
    return List.of(
        new GarbageCollectorMetricsApiCaller(),
        new MemoryMetricsApiCaller(),
        new OsMetricsApiCaller(),
        new ThreadMetricsApiCaller(),
        new StatusCodeMetricsApiCaller(),
        new AggregateMetricsApiCaller(),
        new CoresMetricsApiCaller());
  }

  private final Map<String, PrometheusMetricType> cacheMetricTypes =
      Map.of(
          "bytesUsed", PrometheusMetricType.GAUGE,
          "lookups", PrometheusMetricType.COUNTER,
          "hits", PrometheusMetricType.COUNTER,
          "puts", PrometheusMetricType.COUNTER,
          "evictions", PrometheusMetricType.COUNTER);

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, UnavailableException {
    List<PrometheusMetric> metrics = new ArrayList<>();
    ResultContext resultContext = new ResultContext(metrics);

    AtomicInteger qTime = new AtomicInteger();
    // callers should be invoked sequentially in the same thread there could be dependencies among
    // them
    for (MetricsApiCaller caller : callers) {
      caller.call(qTime, resultContext, request);
    }
    getCompressingDirectoryPoolMetrics(metrics);
    getCircuitBreakerMetrics(metrics);
    getSharedCacheMetrics(metrics, getSolrDispatchFilter(request).getCores(), cacheMetricTypes);
    metrics.add(
        new PrometheusMetric(
            "metrics_qtime", PrometheusMetricType.GAUGE, "QTime for calling metrics api", qTime));
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    PrintWriter printWriter =
        new PrintWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8));
    for (PrometheusMetric metric : metrics) {
      metric.write(printWriter);
    }
    printWriter.flush();
  }

  private static final String BASE_POOL_DESCRIPTION =
      "global CompressingDirectory direct ByteBuffers ";

  private void getCompressingDirectoryPoolMetrics(List<PrometheusMetric> metrics) {
    Map<Integer, long[]> poolStats = CompressingDirectory.getPoolStats();
    for (Map.Entry<Integer, long[]> e : poolStats.entrySet()) {
      int blockSize = e.getKey();
      long[] v = e.getValue();
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_created",
              PrometheusMetricType.COUNTER,
              BASE_POOL_DESCRIPTION + "created",
              v[0]));
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_size",
              PrometheusMetricType.GAUGE,
              BASE_POOL_DESCRIPTION + "pool size",
              v[1]));
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_hits",
              PrometheusMetricType.COUNTER,
              BASE_POOL_DESCRIPTION + "reused",
              v[2]));
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_discarded",
              PrometheusMetricType.COUNTER,
              BASE_POOL_DESCRIPTION + "discarded (pool overflow)",
              v[3]));
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_outstanding",
              PrometheusMetricType.GAUGE,
              BASE_POOL_DESCRIPTION + "outstanding (in use)",
              v[4]));
      metrics.add(
          new PrometheusMetric(
              "direct_pool_" + blockSize + "_outstanding_max",
              PrometheusMetricType.GAUGE,
              BASE_POOL_DESCRIPTION + "outstanding (in use) high water mark",
              v[5]));
    }
    poolStats = CompressingDirectory.getInitPoolStats();
    for (Map.Entry<Integer, long[]> e : poolStats.entrySet()) {
      int blockSize = e.getKey();
      long[] v = e.getValue();
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_created",
              PrometheusMetricType.COUNTER,
              "init " + BASE_POOL_DESCRIPTION + "created",
              v[0]));
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_size",
              PrometheusMetricType.GAUGE,
              "init " + BASE_POOL_DESCRIPTION + "pool size",
              v[1]));
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_hits",
              PrometheusMetricType.COUNTER,
              "init " + BASE_POOL_DESCRIPTION + "reused",
              v[2]));
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_discarded",
              PrometheusMetricType.COUNTER,
              "init " + BASE_POOL_DESCRIPTION + "discarded (pool overflow)",
              v[3]));
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_outstanding",
              PrometheusMetricType.GAUGE,
              "init " + BASE_POOL_DESCRIPTION + "outstanding (in use)",
              v[4]));
      metrics.add(
          new PrometheusMetric(
              "direct_init_pool_" + blockSize + "_outstanding_max",
              PrometheusMetricType.GAUGE,
              "init " + BASE_POOL_DESCRIPTION + "outstanding (in use) high water mark",
              v[5]));
    }
  }

  private void getCircuitBreakerMetrics(List<PrometheusMetric> metrics) {
    getTimesTrippedMetrics()
        .forEach(
            (k, v) -> {
              metrics.add(
                  new PrometheusMetric(
                      "times_tripped" + k,
                      PrometheusMetricType.COUNTER,
                      "number of times circuit has been tripped",
                      v));
            });
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static void getSharedCacheMetrics(
      List<PrometheusMetric> results,
      CoreContainer cores,
      Map<String, PrometheusMetricType> types) {
    Object value =
        Optional.of(cores)
            .map(CoreContainer::getZkController)
            .map(ZkController::getSolrCloudManager)
            .map(SolrCloudManager::getObjectCache)
            .map(
                cache ->
                    cache.get(
                        "fs-shared-caches")) // see AbstractSharedCache.statsSupplier in plugin.
            .filter(Supplier.class::isInstance)
            .map(Supplier.class::cast)
            .map(Supplier::get)
            .orElse(null);
    if (value == null) {
      return;
    }
    Map<String, NamedList<Number>> cacheStats = (Map<String, NamedList<Number>>) value;
    for (Map.Entry<String, NamedList<Number>> cacheStat : cacheStats.entrySet()) {
      String cache = cacheStat.getKey().toLowerCase(Locale.ROOT);
      for (Map.Entry<String, Number> stat : cacheStat.getValue()) {
        String name = stat.getKey();
        PrometheusMetricType type = types.get(name);
        if (type != null) {
          results.add(
              new PrometheusMetric(
                  String.format(Locale.ROOT, "cache_%s_%s", cache, name),
                  type,
                  String.format(
                      Locale.ROOT, "%s %s for cache %s", name, type.getDisplayName(), cache),
                  stat.getValue()));
        }
      }
    }
  }

  static class GarbageCollectorMetricsApiCaller extends MetricsByPrefixApiCaller {

    GarbageCollectorMetricsApiCaller() {
      super("jvm", "gc.G1-,memory.pools.G1-", "");
    }

    /*
    "metrics":{
      "solr.jvm":{
        "gc.G1-Old-Generation.count":0,
        "gc.G1-Old-Generation.time":0,
        "gc.G1-Young-Generation.count":7,
        "gc.G1-Young-Generation.time":75,
        "memory.pools.G1-Eden-Space.committed":374341632,
        "memory.pools.G1-Eden-Space.init":113246208,
        "memory.pools.G1-Eden-Space.max":-1,
        "memory.pools.G1-Eden-Space.usage":0.025210084033613446,
        "memory.pools.G1-Eden-Space.used":9437184,
        "memory.pools.G1-Eden-Space.used-after-gc":0,
        "memory.pools.G1-Old-Gen.committed":1752170496,
        "memory.pools.G1-Old-Gen.init":2034237440,
        "memory.pools.G1-Old-Gen.max":2147483648,
        "memory.pools.G1-Old-Gen.usage":0.010585308074951172,
        "memory.pools.G1-Old-Gen.used":22731776,
        "memory.pools.G1-Old-Gen.used-after-gc":0,
        "memory.pools.G1-Survivor-Space.committed":20971520,
        "memory.pools.G1-Survivor-Space.init":0,
        "memory.pools.G1-Survivor-Space.max":-1,
        "memory.pools.G1-Survivor-Space.usage":1.0,
        "memory.pools.G1-Survivor-Space.used":20971520,
        "memory.pools.G1-Survivor-Space.used-after-gc":20971520}}}
       */
    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode parent = metrics.path("solr.jvm");
      results.add(
          new PrometheusMetric(
              "collection_count_g1_young_generation",
              PrometheusMetricType.COUNTER,
              "the number of GC invocations for G1 Young Generation",
              getNumber(parent, "gc.G1-Young-Generation.count")));
      results.add(
          new PrometheusMetric(
              "collection_time_g1_young_generation",
              PrometheusMetricType.COUNTER,
              "the total number of milliseconds of time spent in gc for G1 Young Generation",
              getNumber(parent, "gc.G1-Young-Generation.time")));
      results.add(
          new PrometheusMetric(
              "collection_count_g1_old_generation",
              PrometheusMetricType.COUNTER,
              "the number of GC invocations for G1 Old Generation",
              getNumber(parent, "gc.G1-Old-Generation.count")));
      results.add(
          new PrometheusMetric(
              "collection_time_g1_old_generation",
              PrometheusMetricType.COUNTER,
              "the total number of milliseconds of time spent in gc for G1 Old Generation",
              getNumber(parent, "gc.G1-Old-Generation.time")));
      results.add(
          new PrometheusMetric(
              "committed_g1_young_eden",
              PrometheusMetricType.GAUGE,
              "committed bytes for G1 Young Generation eden space",
              getNumber(parent, "memory.pools.G1-Eden-Space.committed")));
      results.add(
          new PrometheusMetric(
              "used_g1_young_eden",
              PrometheusMetricType.GAUGE,
              "used bytes for G1 Young Generation eden space",
              getNumber(parent, "memory.pools.G1-Eden-Space.used")));
      results.add(
          new PrometheusMetric(
              "committed_g1_young_survivor",
              PrometheusMetricType.GAUGE,
              "committed bytes for G1 Young Generation survivor space",
              getNumber(parent, "memory.pools.G1-Survivor-Space.committed")));
      results.add(
          new PrometheusMetric(
              "used_g1_young_survivor",
              PrometheusMetricType.GAUGE,
              "used bytes for G1 Young Generation survivor space",
              getNumber(parent, "memory.pools.G1-Survivor-Space.used")));
      results.add(
          new PrometheusMetric(
              "committed_g1_old",
              PrometheusMetricType.GAUGE,
              "committed bytes for G1 Old Generation",
              getNumber(parent, "memory.pools.G1-Old-Gen.committed")));
      results.add(
          new PrometheusMetric(
              "used_g1_old",
              PrometheusMetricType.GAUGE,
              "used bytes for G1 Old Generation",
              getNumber(parent, "memory.pools.G1-Old-Gen.used")));
    }
  }

  static class MemoryMetricsApiCaller extends MetricsByPrefixApiCaller {

    MemoryMetricsApiCaller() {
      super("jvm", "memory.heap.,memory.non-heap.", "");
    }

    /*
    "metrics":{
      "solr.jvm":{
        "memory.heap.committed":2147483648,
        "memory.heap.init":2147483648,
        "memory.heap.max":2147483648,
        "memory.heap.usage":0.1012108325958252,
        "memory.heap.used":217348608,
        "memory.non-heap.committed":96886784,
        "memory.non-heap.init":7667712,
        "memory.non-heap.max":-1,
        "memory.non-heap.usage":-9.313556E7,
        "memory.non-heap.used":93135560}}}
       */
    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode parent = metrics.path("solr.jvm");
      results.add(
          new PrometheusMetric(
              "committed_memory_heap",
              PrometheusMetricType.GAUGE,
              "amount of memory in bytes that is committed for the Java virtual machine to use in the heap",
              getNumber(parent, "memory.heap.committed")));
      results.add(
          new PrometheusMetric(
              "used_memory_heap",
              PrometheusMetricType.GAUGE,
              "amount of used memory in bytes in the heap",
              getNumber(parent, "memory.heap.used")));
      results.add(
          new PrometheusMetric(
              "committed_memory_nonheap",
              PrometheusMetricType.GAUGE,
              "amount of memory in bytes that is committed for the Java virtual machine to use in the nonheap",
              getNumber(parent, "memory.non-heap.committed")));
      results.add(
          new PrometheusMetric(
              "used_memory_nonheap",
              PrometheusMetricType.GAUGE,
              "amount of used memory in bytes in the nonheap",
              getNumber(parent, "memory.non-heap.used")));
    }
  }

  static class OsMetricsApiCaller extends MetricsByPrefixApiCaller {

    OsMetricsApiCaller() {
      super("jvm", "os.", "");
    }

    /*
    "metrics":{
      "solr.jvm":{
        "os.arch":"x86_64",
        "os.availableProcessors":12,
        "os.committedVirtualMemorySize":10852392960,
        "os.freePhysicalMemorySize":1097367552,
        "os.freeSwapSpaceSize":1423704064,
        "os.maxFileDescriptorCount":10000,
        "os.name":"Mac OS X",
        "os.openFileDescriptorCount":188,
        "os.processCpuLoad":1.391126878589777E-4,
        "os.processCpuTime":141437037000,
        "os.systemCpuLoad":0.09213820553771189,
        "os.systemLoadAverage":1.67724609375,
        "os.totalPhysicalMemorySize":17179869184,
        "os.totalSwapSpaceSize":9663676416,
        "os.version":"10.15.7"}}}
       */
    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode parent = metrics.path("solr.jvm");
      results.add(
          new PrometheusMetric(
              "open_file_descriptors",
              PrometheusMetricType.GAUGE,
              "the number of open file descriptors on the filesystem",
              getNumber(parent, "os.openFileDescriptorCount")));
      results.add(
          new PrometheusMetric(
              "max_file_descriptors",
              PrometheusMetricType.GAUGE,
              "the number of max file descriptors on the filesystem",
              getNumber(parent, "os.maxFileDescriptorCount")));
    }
  }

  static class ThreadMetricsApiCaller extends MetricsByPrefixApiCaller {

    ThreadMetricsApiCaller() {
      super("jvm", "threads.", "");
    }

    /*
    "metrics":{
      "solr.jvm":{
        "threads.blocked.count":0,
        "threads.count":2019,
        "threads.daemon.count":11,
        "threads.deadlock.count":0,
        "threads.deadlocks":[],
        "threads.new.count":0,
        "threads.runnable.count":16,
        "threads.terminated.count":0,
        "threads.timed_waiting.count":247,
        "threads.waiting.count":1756}}}
       */
    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode parent = metrics.path("solr.jvm");
      results.add(
          new PrometheusMetric(
              "threads_count",
              PrometheusMetricType.GAUGE,
              "number of threads",
              getNumber(parent, "threads.count")));
      results.add(
          new PrometheusMetric(
              "threads_blocked_count",
              PrometheusMetricType.GAUGE,
              "number of blocked threads",
              getNumber(parent, "threads.blocked.count")));
      results.add(
          new PrometheusMetric(
              "threads_deadlock_count",
              PrometheusMetricType.GAUGE,
              "number of deadlock threads",
              getNumber(parent, "threads.deadlock.count")));
      results.add(
          new PrometheusMetric(
              "threads_runnable_count",
              PrometheusMetricType.GAUGE,
              "number of runnable threads",
              getNumber(parent, "threads.runnable.count")));
      results.add(
          new PrometheusMetric(
              "threads_terminated_count",
              PrometheusMetricType.GAUGE,
              "number of terminated threads",
              getNumber(parent, "threads.terminated.count")));
      results.add(
          new PrometheusMetric(
              "threads_timed_waiting_count",
              PrometheusMetricType.GAUGE,
              "number of timed waiting threads",
              getNumber(parent, "threads.timed_waiting.count")));
      results.add(
          new PrometheusMetric(
              "threads_waiting_count",
              PrometheusMetricType.GAUGE,
              "number of waiting threads",
              getNumber(parent, "threads.waiting.count")));
    }
  }

  static class StatusCodeMetricsApiCaller extends MetricsByPrefixApiCaller {

    StatusCodeMetricsApiCaller() {
      super("jetty", "org.eclipse.jetty.server.handler.DefaultHandler.", "count");
    }

    /*
    "metrics":{
      "solr.jetty":{
        "org.eclipse.jetty.server.handler.DefaultHandler.1xx-responses":{"count":0},
        "org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses":{"count":8816245},
        "org.eclipse.jetty.server.handler.DefaultHandler.3xx-responses":{"count":0},
        "org.eclipse.jetty.server.handler.DefaultHandler.4xx-responses":{"count":1692},
        "org.eclipse.jetty.server.handler.DefaultHandler.5xx-responses":{"count":2066},
        "org.eclipse.jetty.server.handler.DefaultHandler.active-dispatches":0,
        "org.eclipse.jetty.server.handler.DefaultHandler.active-requests":0,
        ...
       */
    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode parent = metrics.path("solr.jetty");
      results.add(
          new PrometheusMetric(
              "status_codes_2xx",
              PrometheusMetricType.COUNTER,
              "cumulative number of responses with 2xx status codes",
              getNumber(
                  parent,
                  "org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses",
                  property)));
      results.add(
          new PrometheusMetric(
              "status_codes_4xx",
              PrometheusMetricType.COUNTER,
              "cumulative number of responses with 4xx status codes",
              getNumber(
                  parent,
                  "org.eclipse.jetty.server.handler.DefaultHandler.4xx-responses",
                  property)));
      results.add(
          new PrometheusMetric(
              "status_codes_5xx",
              PrometheusMetricType.COUNTER,
              "cumulative number of responses with 5xx status codes",
              getNumber(
                  parent,
                  "org.eclipse.jetty.server.handler.DefaultHandler.5xx-responses",
                  property)));
    }
  }

  enum CoreMetric {
    MAJOR_MERGE(
        "INDEX.merge.major", "merges_major", "cumulative number of major merges across cores"),
    MAJOR_MERGE_RUNNING_DOCS(
        "INDEX.merge.major.running.docs",
        "merges_major_current_docs",
        "current number of docs in major merges across cores",
        null,
        PrometheusMetricType.GAUGE),
    MINOR_MERGE(
        "INDEX.merge.minor", "merges_minor", "cumulative number of minor merges across cores"),
    MINOR_MERGE_RUNNING_DOCS(
        "INDEX.merge.minor.running.docs",
        "merges_minor_current_docs",
        "current number of docs in minor merges across cores",
        null,
        PrometheusMetricType.GAUGE),
    GET(
        "QUERY./get.requestTimes",
        "top_level_requests_get",
        "cumulative number of top-level gets across cores"),
    GET_DURATION_P50(
        "QUERY./get.requestTimes",
        "top_level_requests_get_duration_p50",
        "top-level gets p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    GET_DURATION_P95(
        "QUERY./get.requestTimes",
        "top_level_requests_get_duration_p95",
        "top-level gets p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    GET_DURATION_P99(
        "QUERY./get.requestTimes",
        "top_level_requests_get_duration_p99",
        "top-level gets p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    GET_SUBSHARD(
        "QUERY./get[shard].requestTimes",
        "sub_shard_requests_get",
        "cumulative number of sub (spawned by re-distributing a top-level req) gets across cores"),
    GET_SUBSHARD_DURATION_P50(
        "QUERY./get[shard].requestTimes",
        "sub_shard_requests_get_duration_p50",
        "sub shard gets p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    GET_SUBSHARD_DURATION_P95(
        "QUERY./get[shard].requestTimes",
        "sub_shard_requests_get_duration_p95",
        "sub shard gets p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    GET_SUBSHARD_DURATION_P99(
        "QUERY./get[shard].requestTimes",
        "sub_shard_requests_get_duration_p99",
        "sub shard gets p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    SELECT(
        "QUERY./select.requestTimes",
        "top_level_requests_select",
        "cumulative number of top-level selects across cores"),
    SELECT_DURATION_P50(
        "QUERY./select.requestTimes",
        "top_level_requests_select_duration_p50",
        "top-level selects p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    SELECT_DURATION_P95(
        "QUERY./select.requestTimes",
        "top_level_requests_select_duration_p95",
        "top-level selects p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    SELECT_DURATION_P99(
        "QUERY./select.requestTimes",
        "top_level_requests_select_duration_p99",
        "top-level selects p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    SELECT_SUBSHARD(
        "QUERY./select[shard].requestTimes",
        "sub_shard_requests_select",
        "cumulative number of sub (spawned by re-distributing a top-level req) selects across cores"),
    SELECT_SUBSHARD_DURATION_P50(
        "QUERY./select[shard].requestTimes",
        "sub_shard_requests_select_duration_p50",
        "sub shard selects p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    SELECT_SUBSHARD_DURATION_P95(
        "QUERY./select[shard].requestTimes",
        "sub_shard_requests_select_duration_p95",
        "sub shard selects p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    SELECT_SUBSHARD_DURATION_P99(
        "QUERY./select[shard].requestTimes",
        "sub_shard_requests_select_duration_p99",
        "sub shard selects p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    UPDATE(
        "UPDATE./update.requestTimes",
        "distributed_requests_update",
        "cumulative number of distributed updates across cores"),
    UPDATE_DURATION_P50(
        "UPDATE./update.requestTimes",
        "distributed_requests_update_duration_p50",
        "distributed updates p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    UPDATE_DURATION_P95(
        "UPDATE./update.requestTimes",
        "distributed_requests_update_duration_p95",
        "distributed updates p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    UPDATE_DURATION_P99(
        "UPDATE./update.requestTimes",
        "distributed_requests_update_duration_p99",
        "distributed updates p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    LOCAL_UPDATE(
        "UPDATE./update[local].requestTimes",
        "local_requests_update",
        "cumulative number of local updates across cores"),
    LOCAL_UPDATE_DURATION_P50(
        "UPDATE./update[local].requestTimes",
        "local_requests_update_duration_p50",
        "local updates p50 duration",
        "median_ms",
        PrometheusMetricType.GAUGE),
    LOCAL_UPDATE_DURATION_P95(
        "UPDATE./update[local].requestTimes",
        "local_requests_update_duration_p95",
        "local updates p95 duration",
        "p95_ms",
        PrometheusMetricType.GAUGE),
    LOCAL_UPDATE_DURATION_P99(
        "UPDATE./update[local].requestTimes",
        "local_requests_update_duration_p99",
        "local updates p99 duration",
        "p99_ms",
        PrometheusMetricType.GAUGE),
    AUTOCOMMIT(
        "UPDATE.updateHandler.autoCommits",
        "auto_commits_hard",
        "cumulative number of hard auto commits across cores"),
    SOFT_AUTOCOMMIT(
        "UPDATE.updateHandler.softAutoCommits",
        "auto_commits_soft",
        "cumulative number of soft auto commits across cores"),
    COMMITS("UPDATE.updateHandler.commits", "commits", "cumulative number of commits across cores"),
    CUMULATIVE_DEL_BY_ID(
        "UPDATE.updateHandler.cumulativeDeletesById",
        "deletes_by_id",
        "cumulative number of deletes by id across cores"),
    CUMULATIVE_DEL_BY_Q(
        "UPDATE.updateHandler.cumulativeDeletesByQuery",
        "deletes_by_query",
        "cumulative number of deletes by query across cores"),
    CUMULATIVE_DOC_ADDS(
        "UPDATE.updateHandler.cumulativeAdds",
        "doc_adds",
        "cumulative number of docs added across cores"),
    CUMULATIVE_ERRS(
        "UPDATE.updateHandler.cumulativeErrors",
        "update_errors",
        "cumulative number of errors during updates across cores"),
    MERGES("UPDATE.updateHandler.merges", "merges", "cumulative number of merges across cores"),
    OPTIMIZE(
        "UPDATE.updateHandler.optimizes",
        "optimizes",
        "cumulative number of optimizes across cores"),

    SPLITS("UPDATE.updateHandler.optimizes", "splits", "cumulative number of splits across cores"),

    EXPUNGE_DEL(
        "UPDATE.updateHandler.expungeDeletes",
        "expunge_deletes",
        "cumulative number of expungeDeletes across cores");
    final String key, metricName, desc, property;
    private final PrometheusMetricType metricType;
    private static final Map<String, CoreMetric> lookup = new HashMap<>();

    static {
      for (CoreMetric e : CoreMetric.values()) {
        lookup.put(e.key, e);
      }
    }

    CoreMetric(String key, String metricName, String desc) {
      this(key, metricName, desc, "count", PrometheusMetricType.COUNTER);
    }

    CoreMetric(
        String key,
        String metricName,
        String desc,
        String property,
        PrometheusMetricType metricType) {
      this.key = key;
      this.metricName = metricName;
      this.desc = desc;
      this.property = property;
      this.metricType = metricType;
    }

    PrometheusMetric createPrometheusMetric(Number value) {
      return createPrometheusMetric(value, null);
    }

    PrometheusMetric createPrometheusMetric(Number value, String descriptionSuffix) {
      return new PrometheusMetric(
          metricName,
          metricType,
          desc + (descriptionSuffix != null ? descriptionSuffix : ""),
          value.longValue());
    }
  }

  /**
   * A caller that fetch metrics from both groups "solr.node" (node aggregated metrics) and "core"
   * (per core metrics) and match it to all the values in enum CoreMetric. The goal is to provide
   * node level metrics on the CoreMetric values.
   *
   * <p>It first iterates on the "solr.node" metrics, if a core metric is not found there, then it
   * will look it up per core and sum them up as the node metrics.
   */
  static class AggregateMetricsApiCaller extends MetricsByPrefixApiCaller {
    /*
    "metrics":{
    "solr.node":{ //node aggregated metrics
    "QUERY./select.requestTimes":{"count":2},
    "QUERY./select[shard].requestTimes":{"count":0},
    "UPDATE./update.requestTimes":{"count":2},
    "UPDATE./update[local].requestTimes":{"count":0}
    ...
    },
    "solr.core.loadtest.shard1_1.replica_n8":{ //pre core metrics
    "QUERY./select.requestTimes":{"count":1},
    "QUERY./select[shard].requestTimes":{"count":0},
    "UPDATE./update.requestTimes":{"count":1},
    "UPDATE./update[local].requestTimes":{"count":0}
    ...
    },
    "solr.core.loadtest.shard2_1.replica_n10":{
    "QUERY./select.requestTimes":{"count":0},
    "QUERY./select[shard].requestTimes":{"count":0},
    "UPDATE./update.requestTimes":{"count":1},
    "UPDATE./update[local].requestTimes":{"count":0}
    ...
    },
    ...
    */

    AggregateMetricsApiCaller() {
      super("solr.node", buildPrefix(), buildProperty());
    }

    private static String buildPrefix() {
      return String.join(
          ",", Arrays.stream(CoreMetric.values()).map(m -> m.key).toArray(String[]::new));
    }

    private static String buildProperty() {
      return String.join(
          ",",
          Arrays.stream(CoreMetric.values())
              .filter(m -> m.property != null)
              .map(m -> m.property)
              .collect(Collectors.toSet()));
    }

    @Override
    protected void handle(ResultContext resultContext, JsonNode metricsNode) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      JsonNode nodeMetricNode = metricsNode.get("solr.node");

      if (nodeMetricNode != null) {
        resultContext.missingCoreMetrics = new ArrayList<>(); // explicitly set missing core metrics
        for (CoreMetric metric : CoreMetric.values()) {
          Number value =
              metric.property != null
                  ? getNumber(nodeMetricNode, metric.key, metric.property)
                  : getNumber(nodeMetricNode, metric.key);
          if (!INVALID_NUMBER.equals(value)) {
            results.add(metric.createPrometheusMetric(value, "[node aggregated]"));
          } else {
            resultContext.missingCoreMetrics.add(metric);
          }
        }
      } else {
        log.warn(
            "Cannot find the solr.node metrics, going to fall back to getting metrics from all cores");
      }
    }
  }

  /**
   * Collector that get metrics from all the cores and then sum those metrics by CoreMetric key.
   *
   * <p>This runs after AggregateMetricsApiCaller and pick up whatever is missing from it by reading
   * missingCoreMetricsView.
   *
   * <p>Therefore, this has dependency on AggregateMetricsApiCaller and should not be executed
   * concurrently with it.
   */
  static class CoresMetricsApiCaller extends MetricsApiCaller {
    @Override
    protected String buildQueryString(ResultContext resultContext) {
      List<String> prefixes = new ArrayList<>();
      List<String> properties = new ArrayList<>();

      for (CoreMetric targetMetric : getTargetCoreMetrics(resultContext)) {
        prefixes.add(targetMetric.key);
        if (targetMetric.property != null) {
          properties.add(targetMetric.property);
        }
      }

      String propertyClause =
          String.join(
              "&property=",
              properties.stream()
                  .map(p -> URLEncoder.encode(p, StandardCharsets.UTF_8))
                  .collect(Collectors.toSet()));
      return String.format(
          Locale.ROOT,
          "wt=json&indent=false&compact=true&group=%s&prefix=%s%s",
          "core",
          URLEncoder.encode(String.join(",", prefixes), StandardCharsets.UTF_8),
          propertyClause);
    }

    private List<CoreMetric> getTargetCoreMetrics(ResultContext resultContext) {
      List<CoreMetric> targetCoreMetrics = resultContext.missingCoreMetrics;
      // if not explicitly defined by other callers, then just get everything
      if (targetCoreMetrics == null) {
        targetCoreMetrics = Arrays.asList(CoreMetric.values());
      }
      return targetCoreMetrics;
    }

    /*
    "metrics":{
      "solr.core.loadtest.shard1_1.replica_n8":{
        "INDEX.merge.errors":0,
        "INDEX.merge.major":{"count":0},
        "INDEX.merge.major.running":0,
        "INDEX.merge.major.running.docs":0,
        "INDEX.merge.major.running.segments":0,
        "INDEX.merge.minor":{"count":0},
        "INDEX.merge.minor.running":0,
        "INDEX.merge.minor.running.docs":0,
        "INDEX.merge.minor.running.segments":0,
        "QUERY./get.requestTimes":{"count":0},
        "QUERY./get[shard].requestTimes":{"count":0},
        "QUERY./select.requestTimes":{"count":2},
        "QUERY./select[shard].requestTimes":{"count":0},
        "UPDATE./update.requestTimes":{"count":0},
        "UPDATE./update[local].requestTimes":{"count":0},
        "UPDATE.updateHandler.autoCommits":0,
        "UPDATE.updateHandler.commits":{"count":14877},
        "UPDATE.updateHandler.cumulativeDeletesById":{"count":0},
        "UPDATE.updateHandler.cumulativeDeletesByQuery":{"count":0},
        "UPDATE.updateHandler.softAutoCommits":0},
      ...
     */

    @Override
    protected void handle(ResultContext resultContext, JsonNode metrics) throws IOException {
      List<PrometheusMetric> results = resultContext.resultMetrics;
      Map<CoreMetric, Long> accumulative = new LinkedHashMap<>();
      for (CoreMetric missingCoreMetric : getTargetCoreMetrics(resultContext)) {
        for (JsonNode coreMetricNode : metrics) {
          Number val =
              missingCoreMetric.property != null
                  ? getNumber(coreMetricNode, missingCoreMetric.key, missingCoreMetric.property)
                  : getNumber(coreMetricNode, missingCoreMetric.key);
          if (!val.equals(INVALID_NUMBER)) {
            accumulative.put(
                missingCoreMetric,
                accumulative.getOrDefault(missingCoreMetric, 0L) + val.longValue());
          }
        }
      }

      for (Map.Entry<CoreMetric, Long> coreMetricEntry : accumulative.entrySet()) {
        CoreMetric coreMetric = coreMetricEntry.getKey();
        Long accumulativeVal = coreMetricEntry.getValue();
        results.add(coreMetric.createPrometheusMetric(accumulativeVal));
      }
    }
  }

  enum PrometheusMetricType {
    COUNTER("counter"),
    GAUGE("gauge");

    private final String displayName;

    PrometheusMetricType(String displayName) {
      this.displayName = displayName;
    }

    String getDisplayName() {
      return displayName;
    }
  }

  static class PrometheusMetric {

    private final String name;
    private final String type;
    private final String description;
    private final Number value;

    PrometheusMetric(String name, PrometheusMetricType type, String description, Number value) {
      this.name = normalize(name);
      this.type = type.getDisplayName();
      this.description = description;
      this.value = value;
    }

    void write(PrintWriter writer) throws IOException {
      writer.append("# HELP ").append(name).append(' ').append(description).println();
      writer.append("# TYPE ").append(name).append(' ').append(type).println();
      writer.append(name).append(' ').append(value.toString()).println();
    }

    static String normalize(String name) {
      StringBuilder builder = new StringBuilder();
      boolean modified = false;
      for (int i = 0; i < name.length(); i++) {
        char ch = name.charAt(i);
        if (ch == ' ') {
          builder.append('_');
          modified = true;
        } else if (ch == '-') {
          modified = true;
        } else if (Character.isUpperCase(ch)) {
          builder.append('_').append(Character.toLowerCase(ch));
          modified = true;
        } else {
          builder.append(ch);
        }
      }
      return modified ? builder.toString() : name;
    }
  }

  static Number getNumber(JsonNode node, String... names) throws IOException {
    JsonNode originalNode = node;
    for (String name : names) {
      node = node.path(name);
    }
    if (node.isMissingNode()) {
      return INVALID_NUMBER;
    } else if (node.isNumber()) {
      return node.numberValue();
    } else {
      log.warn("node {} does not have a number at the path {}.", originalNode, names);
      return INVALID_NUMBER;
    }
  }

  static SolrDispatchFilter getSolrDispatchFilter(HttpServletRequest request) throws IOException {
    Object value = request.getAttribute(HttpSolrCall.class.getName());
    if (!(value instanceof HttpSolrCall)) {
      throw new IOException(
          String.format(
              Locale.ROOT, "request attribute %s does not exist.", HttpSolrCall.class.getName()));
    }
    return ((HttpSolrCall) value).solrDispatchFilter;
  }

  abstract static class MetricsApiCaller {

    // use HttpSolrCall to simulate a call to the metrics api.
    void call(AtomicInteger qTime, ResultContext resultContext, HttpServletRequest originalRequest)
        throws IOException, UnavailableException {
      SolrDispatchFilter filter = getSolrDispatchFilter(originalRequest);
      CoreContainer cores = filter.getCores();
      HttpServletRequest request =
          new MetricsApiRequest(originalRequest, buildQueryString(resultContext));
      MetricsApiResponse response = new MetricsApiResponse();
      SolrDispatchFilter.Action action =
          new HttpSolrCall(filter, cores, request, response, false).call();
      if (action != SolrDispatchFilter.Action.RETURN) {
        throw new IOException(
            String.format(
                Locale.ROOT,
                "metrics api call returns %s; expected %s.",
                action,
                SolrDispatchFilter.Action.RETURN));
      }
      handleResponse(qTime, resultContext, response.getJsonNode());
    }

    void handleResponse(AtomicInteger qTime, ResultContext resultContext, JsonNode response)
        throws IOException {
      JsonNode header = response.path("responseHeader");
      int status = getNumber(header, "status").intValue();
      if (status != 0) {
        throw new IOException(
            String.format(Locale.ROOT, "metrics api response status is %d; expected 0.", status));
      }
      qTime.addAndGet(getNumber(header, "QTime").intValue());
      handle(resultContext, response.path("metrics"));
    }

    abstract void handle(ResultContext resultContext, JsonNode metrics) throws IOException;

    abstract String buildQueryString(ResultContext resultContext);
  }

  private abstract static class MetricsByPrefixApiCaller extends MetricsApiCaller {
    protected final String group;
    protected final String prefix;
    protected final String[] properties;
    protected final String property; // for backward compatibility

    MetricsByPrefixApiCaller(String group, String prefix, String... properties) {
      this.group = group;
      this.prefix = prefix;
      this.properties = properties;
      this.property = properties.length > 0 ? properties[0] : null;
    }

    @Override
    protected String buildQueryString(ResultContext resultContext) {
      String propertyClause =
          String.join(
              "&property=",
              Arrays.stream(properties)
                  .map(p -> URLEncoder.encode(p, StandardCharsets.UTF_8))
                  .collect(Collectors.toSet()));
      return String.format(
          Locale.ROOT,
          "wt=json&indent=false&compact=true&group=%s&prefix=%s%s",
          URLEncoder.encode(group, StandardCharsets.UTF_8),
          URLEncoder.encode(prefix, StandardCharsets.UTF_8),
          propertyClause);
    }
  }

  // represents a request to e.g.,
  // /solr/admin/metrics?wt=json&indent=false&compact=true&group=solr.jvm&prefix=memory.pools.
  // see ServletUtils.getPathAfterContext() for setting getServletPath() and getPathInfo().
  static class MetricsApiRequest extends HttpServletRequestWrapper {

    private final String queryString;
    private final Map<String, Object> attributes = new HashMap<>();

    MetricsApiRequest(HttpServletRequest request, String queryString) throws IOException {
      super(request);
      this.queryString = queryString;
    }

    @Override
    public String getServletPath() {
      return CommonParams.METRICS_PATH;
    }

    @Override
    public String getPathInfo() {
      return null;
    }

    @Override
    public String getQueryString() {
      return queryString;
    }

    @Override
    public Object getAttribute(String name) {
      Object value = attributes.get(name);
      if (value == null) {
        value = super.getAttribute(name);
      }
      return value;
    }

    @Override
    public Enumeration<String> getAttributeNames() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setAttribute(String name, Object value) {
      attributes.put(name, value);
    }

    @Override
    public void removeAttribute(String name) {
      throw new UnsupportedOperationException();
    }
  }

  static class ByteArrayServletOutputStream extends ServletOutputStream {

    private ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Override
    public void write(int b) throws IOException {
      output.write(b);
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener writeListener) {}

    public byte[] getBytes() {
      return output.toByteArray();
    }
  }
  ;

  static class MetricsApiResponse implements HttpServletResponse {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private int statusCode = 0;
    private ByteArrayServletOutputStream body = new ByteArrayServletOutputStream();

    @Override
    public void setStatus(int code) {
      statusCode = code;
    }

    @Override
    public void setStatus(int code, String s) {
      statusCode = code;
    }

    @Override
    public void sendError(int code, String s) throws IOException {
      statusCode = code;
    }

    @Override
    public void sendError(int code) throws IOException {
      statusCode = code;
    }

    @Override
    public int getStatus() {
      return statusCode;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
      return body;
    }

    public JsonNode getJsonNode() throws IOException {
      if (statusCode != 0 && statusCode / 100 != 2) {
        throw new IOException(
            String.format(Locale.ROOT, "metrics api failed with status code %s.", statusCode));
      }
      return OBJECT_MAPPER.readTree(body.getBytes());
    }

    @Override
    public String encodeURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectURL(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String encodeRedirectUrl(String s) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendRedirect(String s) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addCookie(Cookie cookie) {}

    @Override
    public void setDateHeader(String s, long l) {}

    @Override
    public void addDateHeader(String s, long l) {}

    @Override
    public void setHeader(String s, String s1) {}

    @Override
    public void addHeader(String s, String s1) {}

    @Override
    public void setIntHeader(String s, int i) {}

    @Override
    public void addIntHeader(String s, int i) {}

    @Override
    public void setCharacterEncoding(String s) {}

    @Override
    public void setContentLength(int i) {}

    @Override
    public void setContentLengthLong(long l) {}

    @Override
    public void setContentType(String s) {}

    @Override
    public void setBufferSize(int i) {}

    @Override
    public void flushBuffer() throws IOException {}

    @Override
    public void resetBuffer() {}

    @Override
    public void reset() {}

    @Override
    public void setLocale(Locale locale) {}

    @Override
    public boolean containsHeader(String s) {
      return false;
    }

    @Override
    public String getHeader(String s) {
      return null;
    }

    @Override
    public Collection<String> getHeaders(String s) {
      return Collections.emptyList();
    }

    @Override
    public Collection<String> getHeaderNames() {
      return Collections.emptyList();
    }

    @Override
    public String getCharacterEncoding() {
      return StandardCharsets.UTF_8.name();
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getBufferSize() {
      return 0;
    }

    @Override
    public boolean isCommitted() {
      return false;
    }

    @Override
    public Locale getLocale() {
      return Locale.ROOT;
    }
  }

  /**
   * Context that carries the metrics results as well as information that needs to be propagated in
   * the MetricsApiCaller call chain
   */
  static class ResultContext {
    final List<PrometheusMetric> resultMetrics;
    List<CoreMetric> missingCoreMetrics;

    ResultContext(List<PrometheusMetric> resultMetrics) {
      this.resultMetrics = resultMetrics;
    }
  }
}
