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

import org.agrona.MutableDirectBuffer;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Unit tests for {@link PooledBufferHandle} — the single-buffer, single-owner, exactly-once-release
 * AutoCloseable wrapper (invariants #1 and #2). Tests run with {@code -ea}, so a double-close is
 * expected to throw.
 *
 * @see org.apache.solr.common.util.PooledBufferHandle
 */
public class PooledBufferHandleTest extends SolrTestCase {

  private static ByteBufferPool pool() {
    return ExpandableBuffers.getInstance();
  }

  @Test
  public void testSingleReleaseAndBufferAccess() {
    PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 4096, true, "test-single");
    assertFalse(h.isReleased());
    MutableDirectBuffer b = h.buffer();
    assertNotNull(b);
    b.putInt(0, 12345);
    assertEquals(12345, b.getInt(0));
    h.close();
    assertTrue(h.isReleased());
  }

  @Test
  public void testUseAfterReleaseThrows() {
    PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 1024, true, "test-uar");
    h.close();
    try {
      h.buffer();
      fail("buffer() after release must throw IllegalStateException");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testDoubleReleaseDetectedAndThrows() {
    BufferMetrics metrics = BufferMetrics.getInstance();
    long before = metrics.getDoubleReleaseDetected();
    PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 1024, true, "test-double");
    h.close();
    // Tests run with assertions enabled, so the second close must throw.
    try {
      h.close();
      fail("double-close must throw with assertions enabled");
    } catch (IllegalStateException expected) {
      // expected
    }
    assertEquals("double-release counter must increment exactly once", before + 1, metrics.getDoubleReleaseDetected());
    assertTrue(h.isReleased());
  }

  @Test
  public void testTryWithResourcesReleasesOnce() {
    BufferMetrics metrics = BufferMetrics.getInstance();
    long activeBefore = metrics.getActiveHandleCount();
    PooledBufferHandle captured;
    try (PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 2048, true, "test-twr")) {
      captured = h;
      assertEquals(activeBefore + 1, metrics.getActiveHandleCount());
      h.buffer().putByte(0, (byte) 7);
    }
    assertTrue(captured.isReleased());
    assertEquals(activeBefore, metrics.getActiveHandleCount());
  }

  @Test
  public void testDefaultSizeSentinelMapsTo8192() {
    // size == -1 must flow through to the pool's 8192 default (ArrayByteBufferPool.acquire).
    try (PooledBufferHandle h = PooledBufferHandle.acquire(pool(), -1, true, "test-sentinel")) {
      assertEquals(8192, h.capacity());
    }
  }

  @Test
  public void testAcquireCapacityBucketRounding() {
    // Default-config pool rounds up to the capacity factor (1024). A 1500-byte request -> 2048.
    try (PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 1500, true, "test-bucket")) {
      assertEquals(2048, h.capacity());
    }
  }

  @Test
  public void testSubMinCapacityBranch() {
    // A pool with minCapacity > 0 returns a buffer of exactly the requested size for sub-minCapacity
    // requests (ArrayByteBufferPool.acquire: size < _minCapacity ? size : rounded).
    ArrayByteBufferPool minPool = new ArrayByteBufferPool(1024, 1024, 64 * 1024);
    try (PooledBufferHandle small = PooledBufferHandle.acquire(minPool, 100, true, "test-submin")) {
      assertEquals("sub-minCapacity request returns exact size", 100, small.capacity());
    }
    try (PooledBufferHandle big = PooledBufferHandle.acquire(minPool, 2000, true, "test-submin-big")) {
      assertEquals("at/above minCapacity rounds to factor", 2048, big.capacity());
    }
  }

  @Test
  public void testActiveHandleByteAccounting() {
    BufferMetrics metrics = BufferMetrics.getInstance();
    long bytesBefore = metrics.getActiveHandleBytes();
    long countBefore = metrics.getActiveHandleCount();
    PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 4096, true, "test-acct");
    assertEquals(bytesBefore + h.capacity(), metrics.getActiveHandleBytes());
    assertEquals(countBefore + 1, metrics.getActiveHandleCount());
    h.close();
    assertEquals(bytesBefore, metrics.getActiveHandleBytes());
    assertEquals(countBefore, metrics.getActiveHandleCount());
  }

  @Test
  public void testPoisonZeroOnRelease() {
    boolean prior = PooledBufferHandle.isPoisonEnabled();
    PooledBufferHandle.setPoisonForTest(true);
    try {
      PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 1024, true, "test-poison");
      MutableDirectBuffer raw = h.buffer(); // capture the raw ref before release
      raw.putInt(0, 0x7FABCDEF);
      assertEquals(0x7FABCDEF, raw.getInt(0));
      h.close();
      // After poisoned release the contents must be zeroed (read via the captured raw ref, since the
      // handle's buffer() now correctly refuses use-after-release).
      assertEquals("buffer must be zeroed on poisoned release", 0, raw.getInt(0));
    } finally {
      PooledBufferHandle.setPoisonForTest(prior);
    }
  }

  @Test
  public void testPoisonModeDoubleCloseThrowsWithoutAsserts() {
    // Even if -ea were off, poison mode forces a double-close to throw. Under -ea it throws anyway;
    // this asserts the poison branch specifically and that the counter still moves.
    boolean prior = PooledBufferHandle.isPoisonEnabled();
    PooledBufferHandle.setPoisonForTest(true);
    BufferMetrics metrics = BufferMetrics.getInstance();
    long before = metrics.getDoubleReleaseDetected();
    try {
      PooledBufferHandle h = PooledBufferHandle.acquire(pool(), 1024, true, "test-poison-dc");
      h.close();
      try {
        h.close();
        fail("double-close must throw in poison mode");
      } catch (IllegalStateException expected) {
        // expected
      }
      assertEquals(before + 1, metrics.getDoubleReleaseDetected());
    } finally {
      PooledBufferHandle.setPoisonForTest(prior);
    }
  }
}
