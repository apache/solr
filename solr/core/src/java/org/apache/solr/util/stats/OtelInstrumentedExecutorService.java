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

import static org.apache.solr.metrics.SolrMetricProducer.CATEGORY_ATTR;
import static org.apache.solr.metrics.SolrMetricProducer.NAME_ATTR;
import static org.apache.solr.metrics.SolrMetricProducer.TYPE_ATTR;

import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.metrics.otel.instruments.AttributedLongCounter;
import org.apache.solr.metrics.otel.instruments.AttributedLongTimer;
import org.apache.solr.metrics.otel.instruments.AttributedLongTimer.MetricTimer;
import org.apache.solr.metrics.otel.instruments.AttributedLongUpDownCounter;

/**
 * OTEL instrumentation wrapper around {@link ExecutorService}. Based on
 * com.codahale.metrics.InstrumentedExecutorService.
 */
public class OtelInstrumentedExecutorService implements ExecutorService {

  private final ExecutorService delegate;
  private final String executorName;
  private final List<AutoCloseable> observableMetrics;
  private final AttributedLongCounter submitted;
  private final AttributedLongUpDownCounter running;
  private final AttributedLongCounter completed;
  private final AttributedLongTimer idle;
  private final AttributedLongTimer duration;

  public OtelInstrumentedExecutorService(
      ExecutorService delegate,
      SolrMetricsContext ctx,
      String metricPrefix,
      String executorName,
      SolrInfoBean.Category category) {
    this.delegate = delegate;
    this.executorName = executorName;
    this.observableMetrics = new ArrayList<>();

    Attributes attrs =
        Attributes.builder()
            .put(CATEGORY_ATTR, category.toString())
            .put(NAME_ATTR, executorName)
            .build();

    // Each metric type needs a separate name to avoid obscuring other types
    var executorTaskCounter =
        ctx.longCounter(metricPrefix + "_tasks", "Number of ExecutorService tasks");
    this.submitted =
        new AttributedLongCounter(
            executorTaskCounter, attrs.toBuilder().put(TYPE_ATTR, "submitted").build());
    this.completed =
        new AttributedLongCounter(
            executorTaskCounter, attrs.toBuilder().put(TYPE_ATTR, "completed").build());
    this.running =
        new AttributedLongUpDownCounter(
            ctx.longUpDownCounter(
                metricPrefix + "_tasks_running", "Number of running ExecutorService tasks"),
            attrs);
    var executorTaskTimer =
        ctx.longHistogram(
            metricPrefix + "_task_times", "Timing of ExecutorService tasks", OtelUnit.MILLISECONDS);
    this.idle =
        new AttributedLongTimer(
            executorTaskTimer, attrs.toBuilder().put(TYPE_ATTR, "idle").build());
    this.duration =
        new AttributedLongTimer(
            executorTaskTimer, attrs.toBuilder().put(TYPE_ATTR, "duration").build());

    if (delegate instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor threadPool = (ThreadPoolExecutor) delegate;
      observableMetrics.add(
          ctx.observableLongGauge(
              metricPrefix + "_thread_pool_size",
              "Thread pool size",
              measurement -> {
                measurement.record(
                    threadPool.getPoolSize(), attrs.toBuilder().put(TYPE_ATTR, "size").build());
                measurement.record(
                    threadPool.getCorePoolSize(), attrs.toBuilder().put(TYPE_ATTR, "core").build());
                measurement.record(
                    threadPool.getMaximumPoolSize(),
                    attrs.toBuilder().put(TYPE_ATTR, "max").build());
              }));

      final BlockingQueue<Runnable> taskQueue = threadPool.getQueue();
      observableMetrics.add(
          ctx.observableLongGauge(
              metricPrefix + "_thread_pool_tasks",
              "Thread pool task counts",
              measurement -> {
                measurement.record(
                    threadPool.getActiveCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "active").build());
                measurement.record(
                    threadPool.getCompletedTaskCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "completed").build());
                measurement.record(
                    taskQueue.size(), attrs.toBuilder().put(TYPE_ATTR, "queued").build());
                measurement.record(
                    taskQueue.remainingCapacity(),
                    attrs.toBuilder().put(TYPE_ATTR, "capacity").build());
              }));
    } else if (delegate instanceof ForkJoinPool) {
      ForkJoinPool forkJoinPool = (ForkJoinPool) delegate;
      observableMetrics.add(
          ctx.observableLongGauge(
              metricPrefix + "_fork_join_pool_tasks",
              "Fork join pool task counts",
              measurement -> {
                measurement.record(
                    forkJoinPool.getStealCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "stolen").build());
                measurement.record(
                    forkJoinPool.getQueuedTaskCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "queued").build());
              }));
      observableMetrics.add(
          ctx.observableLongGauge(
              metricPrefix + "_fork_join_pool_threads",
              "Fork join pool thread counts",
              measurement -> {
                measurement.record(
                    forkJoinPool.getActiveThreadCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "active").build());
                measurement.record(
                    forkJoinPool.getRunningThreadCount(),
                    attrs.toBuilder().put(TYPE_ATTR, "running").build());
              }));
    }
  }

  @Override
  public void execute(Runnable task) {
    submitted.inc();
    delegate.execute(new InstrumentedRunnable(task));
  }

  @Override
  public Future<?> submit(Runnable task) {
    submitted.inc();
    return delegate.submit(new InstrumentedRunnable(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    submitted.inc();
    return delegate.submit(new InstrumentedRunnable(task), result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    submitted.inc();
    return delegate.submit(new InstrumentedCallable<>(task));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    submitted.add(Long.valueOf(tasks.size()));
    return delegate.invokeAll(instrument(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    submitted.add(Long.valueOf(tasks.size()));
    return delegate.invokeAll(instrument(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    submitted.add(Long.valueOf(tasks.size()));
    return delegate.invokeAny(instrument(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    submitted.add(Long.valueOf(tasks.size()));
    return delegate.invokeAny(instrument(tasks), timeout, unit);
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
    IOUtils.closeQuietly(observableMetrics);
  }

  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> tasks = delegate.shutdownNow();
    IOUtils.closeQuietly(observableMetrics);
    return tasks;
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  private <T> List<InstrumentedCallable<T>> instrument(Collection<? extends Callable<T>> tasks) {
    List<InstrumentedCallable<T>> instrumented = new ArrayList<>(tasks.size());
    for (Callable<T> task : tasks) {
      instrumented.add(new InstrumentedCallable<>(task));
    }
    return instrumented;
  }

  private class InstrumentedRunnable implements Runnable {
    private final Runnable task;
    private final MetricTimer idleTimer;

    InstrumentedRunnable(Runnable task) {
      this.task = task;
      this.idleTimer = idle.start();
    }

    @Override
    public void run() {
      idleTimer.stop();
      running.inc();

      MetricTimer durationTimer = duration.start();
      try {
        task.run();
      } finally {
        durationTimer.stop();
        running.dec();
        completed.inc();
      }
    }
  }

  private class InstrumentedCallable<T> implements Callable<T> {
    private final Callable<T> task;
    private final MetricTimer idleTimer;

    InstrumentedCallable(Callable<T> task) {
      this.task = task;
      this.idleTimer = idle.start();
    }

    @Override
    public T call() throws Exception {
      idleTimer.stop();
      running.inc();

      MetricTimer durationTimer = duration.start();
      try {
        return task.call();
      } finally {
        durationTimer.stop();
        running.dec();
        completed.inc();
      }
    }
  }
}
