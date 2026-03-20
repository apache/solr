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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * Utility class for detecting stalls in request processing.
 *
 * <p>This class is used by {@link ConcurrentUpdateBaseSolrClient} to detect when request processing
 * has stalled, which can happen if the server is unresponsive or if there's a problem with the
 * connection.
 */
public class StallDetection {
  private final LongAdder processedCount;
  private volatile long lastProcessedCount;
  private final AtomicLong startStallTime = new AtomicLong(-1);
  private final long stallTimeMillis;
  private final IntSupplier queueSizeSupplier;
  private final TimeSource timeSource;

  /**
   * Creates a new StallDetection instance.
   *
   * @param stallTimeMillis The time in milliseconds after which to consider processing stalled
   * @param queueSizeSupplier A supplier that returns the current queue size
   */
  public StallDetection(long stallTimeMillis, IntSupplier queueSizeSupplier) {
    this(stallTimeMillis, queueSizeSupplier, TimeSource.SYSTEM);
  }

  /**
   * Creates a new StallDetection instance with a custom time source.
   *
   * @param stallTimeMillis The time in milliseconds after which to consider processing stalled
   * @param queueSizeSupplier A supplier that returns the current queue size
   * @param timeSource The time source to use for time measurements
   */
  public StallDetection(
      long stallTimeMillis, IntSupplier queueSizeSupplier, TimeSource timeSource) {
    if (stallTimeMillis < 0) {
      throw new IllegalArgumentException("stallTimeMillis must be non-negative");
    }
    this.stallTimeMillis = stallTimeMillis;
    this.queueSizeSupplier = queueSizeSupplier;
    this.processedCount = new LongAdder();
    this.lastProcessedCount = 0;
    this.timeSource = timeSource;
  }

  /**
   * Increments the processed count.
   *
   * <p>This should be called whenever a request is successfully processed.
   */
  public void incrementProcessedCount() {
    processedCount.increment();
  }

  /**
   * Checks if request processing has stalled.
   *
   * <p>This method should be called periodically to check if request processing has stalled. If the
   * queue is not empty and the processed count hasn't changed since the last check, a timer is
   * started. If the timer exceeds the stall time, an IOException is thrown.
   *
   * <p>This method will never throw an exception if the queue is empty.
   *
   * @throws IOException if request processing has stalled
   */
  public void stallCheck() throws IOException {
    int currentQueueSize = queueSizeSupplier.getAsInt();
    if (currentQueueSize == 0) {
      // If the queue is empty, we're not stalled
      startStallTime.set(-1);
      return;
    }

    long processed = processedCount.sum();
    if (processed > lastProcessedCount) {
      // there's still some progress in processing the queue - not stalled
      lastProcessedCount = processed;
      startStallTime.set(-1); // Reset timer when we see progress
    } else {
      long currentStartStallTime = startStallTime.get();
      long currentTime = timeSource.nanoTime();

      // Start the timer if it hasn't been started yet
      if (currentStartStallTime == -1) {
        startStallTime.compareAndSet(-1, currentTime);
        return; // First detection, give it time before throwing exception
      }

      // Calculate elapsed time since stall was first detected
      long timeElapsed = TimeUnit.NANOSECONDS.toMillis(currentTime - currentStartStallTime);

      if (timeElapsed > stallTimeMillis) {
        // Double-check that the queue is still not empty before throwing
        int latestQueueSize = queueSizeSupplier.getAsInt();
        if (latestQueueSize > 0) {
          throw new IOException(
              "Request processing has stalled for "
                  + timeElapsed
                  + "ms with "
                  + latestQueueSize
                  + " remaining elements in the queue.");
        } else {
          // Queue is now empty, reset the timer
          startStallTime.set(-1);
        }
      }
    }
  }

  /**
   * Gets the current processed count.
   *
   * @return the current processed count
   */
  public long getProcessedCount() {
    return processedCount.sum();
  }

  /**
   * Gets the stall time in milliseconds.
   *
   * @return the stall time in milliseconds
   */
  public long getStallTimeMillis() {
    return stallTimeMillis;
  }

  /**
   * Resets the stall timer.
   *
   * <p>This can be useful if you know that processing hasn't actually stalled even though the
   * processed count hasn't changed.
   */
  public void resetStallTimer() {
    startStallTime.set(-1);
  }
}
