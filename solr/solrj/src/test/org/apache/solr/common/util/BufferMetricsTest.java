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
package org.apache.solr.common.util;

import java.util.concurrent.atomic.AtomicLong;

import org.agrona.MutableDirectBuffer;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Unit tests for {@link BufferMetrics} counter correctness. Uses before/after deltas rather than
 * absolute values, because the registry is a process-wide singleton shared across the JVM (and
 * pre-wired with the {@link ExpandableBuffers} pool suppliers) — deltas keep the tests independent
 * of execution order.
 *
 * @see org.apache.solr.common.util.BufferMetrics
 */
public class BufferMetricsTest extends SolrTestCase {

  @Test
  public void testHandleByteAccounting() {
    BufferMetrics m = BufferMetrics.getInstance();
    long bytesBefore = m.getActiveHandleBytes();
    long countBefore = m.getActiveHandleCount();

    m.handleAcquired(4096);
    m.handleAcquired(1024);
    assertEquals(bytesBefore + 5120, m.getActiveHandleBytes());
    assertEquals(countBefore + 2, m.getActiveHandleCount());

    m.handleReleased(4096);
    assertEquals(bytesBefore + 1024, m.getActiveHandleBytes());
    assertEquals(countBefore + 1, m.getActiveHandleCount());

    m.handleReleased(1024);
    assertEquals(bytesBefore, m.getActiveHandleBytes());
    assertEquals(countBefore, m.getActiveHandleCount());
  }

  @Test
  public void testDoubleReleaseCounter() {
    BufferMetrics m = BufferMetrics.getInstance();
    long before = m.getDoubleReleaseDetected();
    m.incrementDoubleReleaseDetected();
    m.incrementDoubleReleaseDetected();
    assertEquals(before + 2, m.getDoubleReleaseDetected());
  }

  @Test
  public void testMmapCapacityAndLogicalSizeKeptSeparate() {
    BufferMetrics m = BufferMetrics.getInstance();
    long capBefore = m.getMmapCapacityBytes();
    long logBefore = m.getMmapLogicalSizeBytes();
    long directBefore = m.getDirectRetainedBytes();
    long heapBefore = m.getHeapAgronaRetainedBytes();

    m.addMmapCapacity(1_000_000);
    m.addMmapLogicalSize(250_000);
    try {
      // Capacity and logical size are independent figures.
      assertEquals(capBefore + 1_000_000, m.getMmapCapacityBytes());
      assertEquals(logBefore + 250_000, m.getMmapLogicalSizeBytes());

      // mmap capacity must NEVER be folded into heap or direct totals.
      assertEquals("mmap capacity must not affect direct retained", directBefore, m.getDirectRetainedBytes());
      assertEquals("mmap capacity must not affect heap retained", heapBefore, m.getHeapAgronaRetainedBytes());

      // Category snapshot reports mmap under its own tag, equal to capacity (not logical size).
      assertEquals(
          Long.valueOf(m.getMmapCapacityBytes()),
          m.retainedByCategory().get(BufferMetrics.Category.MMAP_TLOG));
    } finally {
      m.addMmapCapacity(-1_000_000);
      m.addMmapLogicalSize(-250_000);
    }
  }

  @Test
  public void testActiveReadersCounter() {
    BufferMetrics m = BufferMetrics.getInstance();
    long before = m.getActiveReaders();
    m.incrementActiveReaders();
    m.incrementActiveReaders();
    assertEquals(before + 2, m.getActiveReaders());
    m.decrementActiveReaders();
    assertEquals(before + 1, m.getActiveReaders());
    m.decrementActiveReaders();
    assertEquals(before, m.getActiveReaders());
  }

  @Test
  public void testRemapAndForcedCloseCounters() {
    BufferMetrics m = BufferMetrics.getInstance();
    long remapBefore = m.getRemapCount();
    long forcedBefore = m.getForcedCloseCount();
    m.incrementRemapCount();
    m.incrementForcedCloseCount();
    m.incrementForcedCloseCount();
    assertEquals(remapBefore + 1, m.getRemapCount());
    assertEquals(forcedBefore + 2, m.getForcedCloseCount());
  }

  @Test
  public void testRetainedSupplierSummation() {
    BufferMetrics m = BufferMetrics.getInstance();
    long before = m.getDirectRetainedBytes();
    AtomicLong gauge = new AtomicLong(0);
    m.registerDirectRetainedSupplier(gauge::get);
    assertEquals(before, m.getDirectRetainedBytes());
    gauge.set(8192);
    assertEquals(before + 8192, m.getDirectRetainedBytes());
    gauge.set(0); // leave the registered supplier contributing zero (cannot unregister)
    assertEquals(before, m.getDirectRetainedBytes());
  }

  @Test
  public void testPoolRetainedAndAllocatedAccounting() {
    // Exercise AbstractByteBufferPool's retained (gauge) vs allocated (cumulative) counters directly
    // on a fresh pool, so the numbers are not perturbed by other JVM activity.
    ArrayByteBufferPool pool = new ArrayByteBufferPool();

    assertEquals(0, pool.getDirectAllocated());
    assertEquals(0, pool.getDirectMemory());

    // Pool miss: a fresh 2048-byte direct buffer is minted -> allocated bumps, retained stays 0
    // (it is in use, not idle in the pool).
    MutableDirectBuffer buf = pool.acquire(2048, true);
    assertEquals(2048, buf.capacity());
    assertEquals("allocated is cumulative on a pool miss", 2048, pool.getDirectAllocated());
    assertEquals("retained is 0 while the buffer is checked out", 0, pool.getDirectMemory());

    // Release: the buffer becomes retained (idle) in the pool.
    pool.release(buf);
    assertEquals(2048, pool.getDirectMemory());
    assertEquals("allocated does not move on release", 2048, pool.getDirectAllocated());

    // Re-acquire same size: served from the pool (a hit) -> retained drops, allocated unchanged.
    MutableDirectBuffer reused = pool.acquire(2048, true);
    assertEquals(0, pool.getDirectMemory());
    assertEquals("no new allocation on a pool hit", 2048, pool.getDirectAllocated());
    pool.release(reused);
  }

  @Test
  public void testHeapAllocatedTrackedSeparatelyFromDirect() {
    ArrayByteBufferPool pool = new ArrayByteBufferPool();
    MutableDirectBuffer heap = pool.acquire(2048, false);
    assertEquals("heap allocation counted as heap", 2048, pool.getHeapAllocated());
    assertEquals("heap allocation must not count as direct", 0, pool.getDirectAllocated());
    pool.release(heap);
    assertEquals(2048, pool.getHeapMemory());
    assertEquals(0, pool.getDirectMemory());
  }
}
