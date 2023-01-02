package com.flipkart.solr.ltr.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;

public class Parallelizer {
  private final ExecutorService executorService;

  public Parallelizer(int nThreads, MetricRegistry metricRegistry, String name) {
    this.executorService = Executors.newFixedThreadPool(nThreads);
    new InstrumentedExecutorService(executorService, metricRegistry, name+"Parallelizer");
  }

  public void parallelConsume(int size, int parallelizationLevel, Consumer<Integer> consumer) {
    if (size < parallelizationLevel) {
      parallelizationLevel = size;
    }

    int subSize = size/parallelizationLevel + (size%parallelizationLevel == 0 ? 0 : 1);

    List<Callable<Boolean>> tasks = new ArrayList<>();
    for (int i=0; i<parallelizationLevel; i++) {
      int startOffsetInclusive = i*subSize;
      if (startOffsetInclusive >= size) break;
      int endOffSetExclusive = Math.min((i + 1) * subSize, size);

      tasks.add(new RangeConsumer(startOffsetInclusive, endOffSetExclusive, consumer));
    }

    try {
      executorService.invokeAll(tasks);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static class RangeConsumer implements Callable<Boolean> {
    private final int startOffsetInclusive;
    private final int endOffSetExclusive;
    private final Consumer<Integer> consumer;

    private RangeConsumer(int startOffsetInclusive, int endOffSetExclusive, Consumer<Integer> consumer) {
      this.startOffsetInclusive = startOffsetInclusive;
      this.endOffSetExclusive = endOffSetExclusive;
      this.consumer = consumer;
    }

    @Override
    public Boolean call() {
      for (int offset = startOffsetInclusive; offset <endOffSetExclusive; offset++) {
        consumer.accept(offset);
      }
      return true;
    }
  }
}