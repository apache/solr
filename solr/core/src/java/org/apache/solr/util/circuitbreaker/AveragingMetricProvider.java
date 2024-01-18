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
package org.apache.solr.util.circuitbreaker;

import com.google.common.util.concurrent.AtomicDouble;
import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.logging.CircularList;

/** Averages the metric value over a period of time */
public class AveragingMetricProvider implements Closeable {
  private final CircularList<Double> samplesRingBuffer;
  private ScheduledExecutorService executor;
  private final AtomicDouble currentAverageValue = new AtomicDouble(-1);

  /**
   * Creates an instance with an executor that runs every sampleInterval seconds and averages over
   * numSamples samples.
   *
   * @param metricProvider metric provider that will provide a value
   * @param numSamples number of samples to calculate average for
   * @param sampleInterval interval between each sample
   */
  public AveragingMetricProvider(
      MetricProvider metricProvider, int numSamples, long sampleInterval) {
    this.samplesRingBuffer = new CircularList<>(numSamples);
    executor =
        Executors.newSingleThreadScheduledExecutor(
            new SolrNamedThreadFactory(
                "AveragingMetricProvider-" + metricProvider.getClass().getSimpleName()));
    executor.scheduleWithFixedDelay(
        () -> {
          samplesRingBuffer.add(metricProvider.getMetricValue());
          currentAverageValue.set(
              samplesRingBuffer.toList().stream()
                  .mapToDouble(Double::doubleValue)
                  .average()
                  .orElse(-1));
        },
        0,
        sampleInterval,
        TimeUnit.SECONDS);
  }

  /**
   * Return current average. This is a cached value, so calling this method will not incur any
   * calculations
   */
  public double getMetricValue() {
    return currentAverageValue.get();
  }

  @Override
  public void close() {
    ExecutorUtil.shutdownAndAwaitTermination(executor);
  }

  /** Interface to provide the metric value. */
  public interface MetricProvider {
    double getMetricValue();
  }
}
