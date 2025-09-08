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
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.Before;
import org.junit.Test;

public class OtelInstrumentedExecutorServiceTest extends SolrTestCase {
  public static final int NUM_THREADS = 10;
  public static final double DELTA = 1E-8;
  public static final String REGISTRY_NAME = "solr-test-otel-registry";
  public static final String TAG_NAME = "solr-test-otel-tag";

  public static SolrMetricsContext metricsContext;

  @Before
  public void setUpMetrics() {
    metricsContext = new SolrMetricsContext(new SolrMetricManager(), REGISTRY_NAME, TAG_NAME);
  }

  @Test
  public void taskCount() throws InterruptedException {
    try (var exec = testExecutor("taskCount", Executors.newFixedThreadPool(NUM_THREADS))) {
      final int numTasks = 2025;
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(5, TimeUnit.SECONDS); // Wait for task completion

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot tasksRunning =
          metrics.stream()
              .filter(
                  m -> m.getMetadata().getPrometheusName().equals("solr_executor_tasks_running"))
              .findFirst()
              .map(GaugeSnapshot.class::cast)
              .get();
      CounterSnapshot taskCounters =
          metrics.stream()
              .filter(m -> m.getMetadata().getPrometheusName().equals("solr_executor_tasks"))
              .findFirst()
              .map(CounterSnapshot.class::cast)
              .get();

      GaugeDataPointSnapshot runningTasks = tasksRunning.getDataPoints().getFirst();
      CounterDataPointSnapshot submittedTasks =
          taskCounters.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("submitted"))
              .findFirst()
              .get();
      CounterDataPointSnapshot completedTasks =
          taskCounters.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("completed"))
              .findFirst()
              .get();

      assertEquals(0, runningTasks.getValue(), DELTA);
      assertEquals(numTasks, submittedTasks.getValue(), DELTA);
      assertEquals(numTasks, completedTasks.getValue(), DELTA);
    }
  }

  @Test
  public void executorTaskRandomCount() throws InterruptedException {
    try (var exec = testExecutor("taskRandomCount", Executors.newFixedThreadPool(NUM_THREADS))) {
      final int numTasks = randomIntBetween(1, 5000);
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(5, TimeUnit.SECONDS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      GaugeSnapshot tasksRunning =
          metrics.stream()
              .filter(
                  m -> m.getMetadata().getPrometheusName().equals("solr_executor_tasks_running"))
              .findFirst()
              .map(GaugeSnapshot.class::cast)
              .get();
      CounterSnapshot taskCounters =
          metrics.stream()
              .filter(m -> m.getMetadata().getPrometheusName().equals("solr_executor_tasks"))
              .findFirst()
              .map(CounterSnapshot.class::cast)
              .get();

      GaugeDataPointSnapshot runningTasks = tasksRunning.getDataPoints().getFirst();
      CounterDataPointSnapshot submittedTasks =
          taskCounters.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("submitted"))
              .findFirst()
              .get();
      CounterDataPointSnapshot completedTasks =
          taskCounters.getDataPoints().stream()
              .filter(data -> data.getLabels().get(TYPE_ATTR.toString()).equals("completed"))
              .findFirst()
              .get();

      assertEquals(0, runningTasks.getValue(), DELTA);
      assertEquals(numTasks, submittedTasks.getValue(), DELTA);
      assertEquals(numTasks, completedTasks.getValue(), DELTA);
    }
  }

  @Test
  public void executorTaskTimers() throws InterruptedException {
    try (var exec = testExecutor("taskTimers", Executors.newFixedThreadPool(NUM_THREADS))) {
      final long durationMs = 300;
      final double durationDeltaMs = 10.0;
      exec.submit(
          () -> {
            try {
              Thread.sleep(durationMs);
            } catch (InterruptedException e) {
            }
          });
      exec.awaitTermination(5, TimeUnit.SECONDS);

      MetricSnapshots metrics =
          metricsContext.getMetricManager().getPrometheusMetricReader(REGISTRY_NAME).collect();
      HistogramSnapshot taskTimers =
          metrics.stream()
              .filter(
                  m -> m.getMetadata().getPrometheusName().startsWith("solr_executor_task_times"))
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
    try (var exec = testExecutor("threadPoolTasks", Executors.newFixedThreadPool(NUM_THREADS))) {
      final int numTasks = randomIntBetween(1, 5000);
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(5, TimeUnit.SECONDS);

      // TODO: test thread pool metrics
    }
  }

  @Test
  public void forkJoinPoolTasks() throws InterruptedException {
    try (var exec = testExecutor("forkJoinPoolTasks", Executors.newWorkStealingPool(NUM_THREADS))) {
      final int numTasks = randomIntBetween(1, 5000);
      for (int i = 0; i < numTasks; ++i) {
        exec.submit(() -> {});
      }
      exec.awaitTermination(5, TimeUnit.SECONDS);

      // TODO: test fork join pool metrics
    }
  }

  private ExecutorService testExecutor(String name, ExecutorService exec) {
    return MetricUtils.instrumentedExecutorService(
        exec, metricsContext, SolrInfoBean.Category.ADMIN, name);
  }
}
