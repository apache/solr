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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the current JVM heap usage and triggers if a moving heap usage average over 30 seconds
 * exceeds the defined percentage of the maximum heap size allocated to the JVM. Once the average
 * memory usage goes below the threshold, it will start allowing queries again.
 *
 * <p>The memory threshold is defined as a percentage of the maximum memory allocated -- see
 * memThreshold in <code>solrconfig.xml</code>.
 */
public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
  // One shared provider / executor for all instances of this class
  private static RefCounted<AveragingMetricProvider> averagingMetricProvider;

  private long heapMemoryThreshold;

  private static final ThreadLocal<Long> seenMemory = ThreadLocal.withInitial(() -> 0L);
  private static final ThreadLocal<Long> allowedMemory = ThreadLocal.withInitial(() -> 0L);

  /** Creates an instance which averages over 6 samples during last 30 seconds. */
  public MemoryCircuitBreaker() {
    this(6, 5);
  }

  /**
   * Constructor that allows override of sample interval for which the memory usage is fetched. This
   * is provided for testing, not intended for general use because the average metric provider
   * implementation is the same for all instances of the class.
   *
   * @param numSamples number of samples to calculate average for
   * @param sampleInterval interval between each sample
   */
  protected MemoryCircuitBreaker(int numSamples, int sampleInterval) {
    super();
    synchronized (MemoryCircuitBreaker.class) {
      if (averagingMetricProvider == null || averagingMetricProvider.getRefcount() == 0) {
        averagingMetricProvider =
            new RefCounted<>(
                new AveragingMetricProvider(
                    () -> MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed(),
                    numSamples,
                    sampleInterval)) {
              @Override
              protected void close() {
                get().close();
              }
            };
      }
      averagingMetricProvider.incref();
    }
  }

  public MemoryCircuitBreaker setThreshold(double thresholdValueInPercentage) {
    long currentMaxHeap = MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();

    if (currentMaxHeap <= 0) {
      throw new IllegalArgumentException("Invalid JVM state for the max heap usage");
    }

    double thresholdInFraction = thresholdValueInPercentage / (double) 100;
    heapMemoryThreshold = (long) (currentMaxHeap * thresholdInFraction);

    if (heapMemoryThreshold <= 0) {
      throw new IllegalStateException("Memory limit cannot be less than or equal to zero");
    }
    return this;
  }

  @Override
  public boolean isTripped() {

    long localAllowedMemory = getCurrentMemoryThreshold();
    long localSeenMemory = getAvgMemoryUsage();

    allowedMemory.set(localAllowedMemory);

    seenMemory.set(localSeenMemory);

    return (localSeenMemory >= localAllowedMemory);
  }

  protected long getAvgMemoryUsage() {
    return (long) averagingMetricProvider.get().getMetricValue();
  }

  @Override
  public String getErrorMessage() {
    return "Memory Circuit Breaker triggered as JVM heap usage values are greater than allocated threshold. "
        + "Seen JVM heap memory usage "
        + seenMemory.get()
        + " and allocated threshold "
        + allowedMemory.get();
  }

  private long getCurrentMemoryThreshold() {
    return heapMemoryThreshold;
  }

  @Override
  public void close() throws IOException {
    synchronized (MemoryCircuitBreaker.class) {
      if (averagingMetricProvider != null && averagingMetricProvider.getRefcount() > 0) {
        averagingMetricProvider.decref();
      }
    }
  }
}
