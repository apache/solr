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

package org.apache.solr.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class DelegateRegistryTimerTest extends SolrTestCase {

  MetricRegistry.MetricSupplier<Timer> timerSupplier =
      new MetricSuppliers.DefaultTimerSupplier(null);

  @Test
  public void update() {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    delegateRegistryTimer.update(Duration.ofNanos(100));
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(100.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(100.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(100.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(100.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(100.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(100.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());

    delegateRegistryTimer.update(Duration.ofNanos(200));
    delegateRegistryTimer.update(Duration.ofNanos(300));
    assertEquals(3, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(200.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(200.0, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(200.0, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(200.0, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(3, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(200.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(200.0, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());
  }

  @Test
  public void testUpdate() {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    delegateRegistryTimer.update(100, TimeUnit.NANOSECONDS);
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(100, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(100, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(100, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(100, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(100, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(100, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(100L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());

    delegateRegistryTimer.update(200, TimeUnit.NANOSECONDS);
    delegateRegistryTimer.update(300, TimeUnit.NANOSECONDS);
    assertEquals(3, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(200, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(), 0.0);
    assertEquals(200, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMax());
    assertEquals(200, delegateRegistryTimer.getSnapshot().getMean(), 0.0);
    assertEquals(200, delegateRegistryTimer.getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getSnapshot().getMax());
    assertEquals(3, delegateRegistryTimer.getDelegateTimer().getCount());
    assertEquals(200, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(), 0.0);
    assertEquals(200, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMedian(), 0.0);
    assertEquals(300L, delegateRegistryTimer.getDelegateTimer().getSnapshot().getMax());
  }

  @Test
  public void timeContext() throws InterruptedException {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    Timer.Context time = delegateRegistryTimer.time();
    Thread.sleep(100);
    time.close();
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > 100000);
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > 100000);
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(),
        0.0);
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getSnapshot().getMean(),
        0.0);
  }

  @Test
  public void timeSupplier() {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    AtomicLong timeTaken = new AtomicLong();
    Long supplierResult =
        delegateRegistryTimer.timeSupplier(
            () -> {
              timeTaken.getAndSet(System.nanoTime());
              for (int i = 0; i < 100; i++) {
                // Just loop
              }
              timeTaken.getAndSet(System.nanoTime() - timeTaken.get());
              return 1L;
            });
    assertEquals(Long.valueOf(1L), supplierResult);
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > timeTaken.get());
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > timeTaken.get());
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(),
        0.0);
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getSnapshot().getMean(),
        0.0);
  }

  @Test
  public void testTimeCallable() throws Exception {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    AtomicLong timeTaken = new AtomicLong();
    Long callableResult =
        delegateRegistryTimer.time(
            () -> {
              timeTaken.getAndSet(System.nanoTime());
              for (int i = 0; i < 100; i++) {
                // Just loop
              }
              timeTaken.getAndSet(System.nanoTime() - timeTaken.get());
              return 1L;
            });
    assertEquals(Long.valueOf(1L), callableResult);
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > timeTaken.get());
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > timeTaken.get());
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(),
        0.0);
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getSnapshot().getMean(),
        0.0);
  }

  @Test
  public void testTimeRunnable() {
    DelegateRegistryTimer delegateRegistryTimer =
        new DelegateRegistryTimer(
            Clock.defaultClock(), timerSupplier.newMetric(), timerSupplier.newMetric());
    AtomicLong timeTaken = new AtomicLong();
    delegateRegistryTimer.time(
        () -> {
          timeTaken.getAndSet(System.nanoTime());
          for (int i = 0; i < 100; i++) {
            // Just loop
          }
          timeTaken.getAndSet(System.nanoTime() - timeTaken.get());
        });
    assertEquals(1, delegateRegistryTimer.getPrimaryTimer().getCount());
    assertEquals(1, delegateRegistryTimer.getDelegateTimer().getCount());
    assertTrue(delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean() > timeTaken.get());
    assertTrue(delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean() > timeTaken.get());
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getDelegateTimer().getSnapshot().getMean(),
        0.0);
    assertEquals(
        delegateRegistryTimer.getPrimaryTimer().getSnapshot().getMean(),
        delegateRegistryTimer.getSnapshot().getMean(),
        0.0);
  }
}
