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
package org.apache.solr.util.stats;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot.HistogramDataPointSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.SolrMetricTestUtils;
import org.junit.Before;
import org.junit.Test;

public class OtelInstrumentedExecutorServiceTest extends SolrTestCase {
  public static final int PARALLELISM = 10;
  public static long EXEC_TIMEOUT = 1;
  public static TimeUnit EXEC_TIMEOUT_UNITS = TimeUnit.SECONDS;
  public static final String REGISTRY_NAME = "solr-test-otel-registry";
  public static final double DELTA = 2.4e-07;

  public static SolrMetricsContext metricsContext;

  @Before
  public void setUpMetrics() {
    metricsContext = new SolrMetricsContext(new SolrMetricManager(null), REGISTRY_NAME);
  }

  @Test
  public void taskCount() throws InterruptedException {
    try (var exec = testExecutor("taskCount", newFixedThreadPoolExecutor())) {
      final int numTasks = 225;
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(EXEC_TIMEOUT, EXEC_TIMEOUT_UNITS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot tasksRunning =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_tasks_running");
      CounterSnapshot taskCounters =
          SolrMetricTestUtils.getMetricSnapshot(
              CounterSnapshot.class, metrics, "solr_node_executor_tasks");

      GaugeDataPointSnapshot runningTasks = tasksRunning.getDataPoints().getFirst();
      CounterDataPointSnapshot submittedTasks = getCounterData(taskCounters, "submitted");
      CounterDataPointSnapshot completedTasks = getCounterData(taskCounters, "completed");

      assertEquals(0.0, runningTasks.getValue(), DELTA);
      assertEquals(numTasks, submittedTasks.getValue(), DELTA);
      assertEquals(numTasks, completedTasks.getValue(), DELTA);
    }
  }

  @Test
  public void taskRandomCount() throws InterruptedException {
    try (var exec = testExecutor("taskRandomCount", newFixedThreadPoolExecutor())) {
      final int numTasks = randomIntBetween(1, 500);
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(EXEC_TIMEOUT, EXEC_TIMEOUT_UNITS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot tasksRunning =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_tasks_running");
      CounterSnapshot taskCounters =
          SolrMetricTestUtils.getMetricSnapshot(
              CounterSnapshot.class, metrics, "solr_node_executor_tasks");

      GaugeDataPointSnapshot runningTasks = tasksRunning.getDataPoints().getFirst();
      CounterDataPointSnapshot submittedTasks = getCounterData(taskCounters, "submitted");
      CounterDataPointSnapshot completedTasks = getCounterData(taskCounters, "completed");

      assertEquals(0.0, runningTasks.getValue(), DELTA);
      assertEquals(numTasks, submittedTasks.getValue(), DELTA);
      assertEquals(numTasks, completedTasks.getValue(), DELTA);
    }
  }

  @Test
  public void taskTimers() throws InterruptedException {
    try (var exec = testExecutor("taskTimers", newFixedThreadPoolExecutor())) {
      final long durationMs = 200;
      final double durationDeltaMs = 10.0;
      exec.submit(
          () -> {
            try {
              Thread.sleep(durationMs);
            } catch (InterruptedException e) {
            }
          });
      exec.awaitTermination(EXEC_TIMEOUT, EXEC_TIMEOUT_UNITS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      HistogramSnapshot taskTimers =
          metrics.stream()
              .filter(
                  m ->
                      m.getMetadata()
                          .getPrometheusName()
                          .startsWith("solr_node_executor_task_times"))
              .findFirst()
              .map(HistogramSnapshot.class::cast)
              .get();

      HistogramDataPointSnapshot idleTimer =
          taskTimers.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("idle"))
              .findFirst()
              .get();
      HistogramDataPointSnapshot durationTimer =
          taskTimers.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("duration"))
              .findFirst()
              .get();

      assertTrue(TimeUnit.SECONDS.toMillis(5) > idleTimer.getSum());
      assertEquals(durationMs, durationTimer.getSum(), durationDeltaMs);
    }
  }

  @Test
  public void threadPoolTasks() throws InterruptedException {
    try (var exec = testExecutor("threadPoolTasks", newFixedThreadPoolExecutor())) {
      final int numTasks = 225;
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(EXEC_TIMEOUT, EXEC_TIMEOUT_UNITS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot sizeGauges =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_thread_pool_size");
      GaugeSnapshot taskGauges =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_thread_pool_tasks");

      GaugeDataPointSnapshot poolSize = getGaugeData(sizeGauges, "size");
      GaugeDataPointSnapshot corePoolSize = getGaugeData(sizeGauges, "core");
      GaugeDataPointSnapshot maxPoolSize = getGaugeData(sizeGauges, "max");

      GaugeDataPointSnapshot activeTasks = getGaugeData(taskGauges, "active");
      GaugeDataPointSnapshot completedTasks = getGaugeData(taskGauges, "completed");
      GaugeDataPointSnapshot queuedTasks = getGaugeData(taskGauges, "queued");
      GaugeDataPointSnapshot poolCapacity = getGaugeData(taskGauges, "capacity");

      assertEquals(PARALLELISM, poolSize.getValue(), DELTA);
      assertEquals(PARALLELISM, corePoolSize.getValue(), DELTA);
      assertEquals(PARALLELISM, maxPoolSize.getValue(), DELTA);

      assertEquals(0.0, activeTasks.getValue(), DELTA);
      assertEquals(numTasks, completedTasks.getValue(), DELTA);
      assertEquals(0.0, queuedTasks.getValue(), DELTA);
      assertEquals(Integer.MAX_VALUE, poolCapacity.getValue(), DELTA);
    }
  }

  private static ExecutorService newFixedThreadPoolExecutor() {
    return ExecutorUtil.newMDCAwareFixedThreadPool(PARALLELISM, new SolrNamedThreadFactory("test"));
  }

  @Test
  public void forkJoinPoolTasks() throws InterruptedException {
    try (var exec = testExecutor("forkJoinPoolTasks", Executors.newWorkStealingPool(PARALLELISM))) {
      final int numTasks = 225;
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(EXEC_TIMEOUT, EXEC_TIMEOUT_UNITS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot taskGauges =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_fork_join_pool_tasks");
      GaugeSnapshot threadGauges =
          SolrMetricTestUtils.getMetricSnapshot(
              GaugeSnapshot.class, metrics, "solr_node_executor_fork_join_pool_threads");
      GaugeDataPointSnapshot stolenTasks = getGaugeData(taskGauges, "stolen");
      GaugeDataPointSnapshot queuedTasks = getGaugeData(taskGauges, "queued");

      GaugeDataPointSnapshot activeThreads = getGaugeData(threadGauges, "active");
      GaugeDataPointSnapshot runningThreads = getGaugeData(threadGauges, "running");

      assertNotNull(stolenTasks.getValue());
      assertEquals(0.0, queuedTasks.getValue(), DELTA);

      assertEquals(0.0, activeThreads.getValue(), DELTA);
      assertEquals(0.0, runningThreads.getValue(), DELTA);
    }
  }

  private static ExecutorService testExecutor(String name, ExecutorService exec) {
    return metricsContext.instrumentedExecutorService(
        exec, "solr_node_executor", name, SolrInfoBean.Category.ADMIN);
  }

  private static CounterDataPointSnapshot getCounterData(CounterSnapshot snapshot, String type) {
    return snapshot.getDataPoints().stream()
        .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals(type))
        .findFirst()
        .get();
  }

  private static GaugeDataPointSnapshot getGaugeData(GaugeSnapshot snapshot, String type) {
    return snapshot.getDataPoints().stream()
        .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals(type))
        .findFirst()
        .get();
  }
}
