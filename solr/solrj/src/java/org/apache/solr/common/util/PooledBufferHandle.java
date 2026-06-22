/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.MutableDirectBuffer;

/**
 * A thin, additive {@link AutoCloseable} wrapper that gives a <b>single</b> pooled buffer exactly
 * one owner and exactly one release site, enforced in code rather than by comment.
 *
 * <p><b>Single-buffer, not a multi-buffer accumulator.</b> Do not confuse this with
 * {@link ByteBufferPool.Lease}: {@code Lease} is a Jetty-style multi-buffer accumulator that
 * collects several buffers and recycles them together; {@code PooledBufferHandle} owns exactly one
 * physical buffer and releases it once. The two are intentionally distinct — the word "lease" is
 * <em>not</em> overloaded onto this type.
 *
 * <p><b>Adoption rule (single-owner invariant #9 / #1).</b> This handle is <em>opt-in and
 * additive</em>. A physical buffer is owned by <em>either</em> a legacy {@code AtomicBoolean} CAS
 * guard (e.g. {@code SolrHttpRequest.freeBuffer}) <em>or</em> a {@code PooledBufferHandle}, never
 * both. Do NOT retrofit already-guarded single-owner sites; doing so would create a double-owner
 * hazard. New adoption is limited to sites that today lack a single idempotent owner (the
 * {@code responseBuffer} request-attribute defect targeted by LANE-QRESP).
 *
 * <p><b>Lifecycle guarantees:</b>
 * <ul>
 *   <li>{@link #buffer()} throws {@link IllegalStateException} after release (invariant #2,
 *       no use-after-release).</li>
 *   <li>{@link #close()} releases the buffer back to the pool <em>exactly once</em> via
 *       {@code released.compareAndSet(false,true)} (invariant #1, no double-release). A second
 *       {@code close()} is a no-op for the pool but increments
 *       {@link BufferMetrics#incrementDoubleReleaseDetected()} and, when assertions are enabled or
 *       poison mode is on, throws so the bug surfaces loudly in tests.</li>
 *   <li>Optional poison/zero-on-release mode ({@value #POISON_PROP}) zero-fills the buffer on
 *       release so any straggling reader sees zeros instead of recycled bytes.</li>
 * </ul>
 *
 * <p>This class is not thread-safe for concurrent {@code buffer()} use by multiple owners — the
 * point is that there is exactly one owner — but its release guard is correct under a concurrent
 * double-close race.
 */
public final class PooledBufferHandle implements AutoCloseable {

  /** System property enabling zero-on-release poisoning (test-only). Default false. */
  public static final String POISON_PROP = "solr.buffer.lease.poison";

  // Test-only toggle: zero-fill on release so use-after-release fails loudly, and make a
  // double-close throw even without -ea. Initialized from the system property; settable for tests.
  // Read as a volatile boolean (negligible cost) so tests can flip it without class-reload games.
  private static volatile boolean poison = Boolean.getBoolean(POISON_PROP);

  private static final boolean ASSERTS_ENABLED;

  static {
    boolean ea = false;
    assert ea = true;
    ASSERTS_ENABLED = ea;
  }

  private final ByteBufferPool pool;
  private final MutableDirectBuffer buffer;
  private final String tag;
  private final int capacity;
  private final AtomicBoolean released = new AtomicBoolean(false);

  private PooledBufferHandle(ByteBufferPool pool, MutableDirectBuffer buffer, String tag, int capacity) {
    this.pool = pool;
    this.buffer = buffer;
    this.tag = tag;
    this.capacity = capacity;
  }

  /**
   * Acquire a single pooled buffer and wrap it in an owning handle.
   *
   * <p>The {@code size == -1} default-size sentinel is preserved end-to-end: it is passed straight
   * through to {@link ByteBufferPool#acquire(int, boolean)}, which (for {@link ArrayByteBufferPool})
   * maps -1 to the 8192-byte default (see {@code ArrayByteBufferPool.acquire}). Allocation behavior
   * is unchanged; this only adds ownership + accounting.
   *
   * @param pool the pool to acquire from and, on {@link #close()}, release back to
   * @param size requested size, or {@code -1} for the pool default
   * @param direct whether the buffer must be off-heap direct
   * @param tag a short category/owner tag for diagnostics (may be null)
   * @return an owning handle over exactly one buffer
   */
  public static PooledBufferHandle acquire(ByteBufferPool pool, int size, boolean direct, String tag) {
    MutableDirectBuffer buffer = pool.acquire(size, direct);
    int capacity = buffer.capacity();
    BufferMetrics.getInstance().handleAcquired(capacity);
    return new PooledBufferHandle(pool, buffer, tag, capacity);
  }

  /**
   * @return the wrapped buffer.
   * @throws IllegalStateException if this handle has already been released (invariant #2).
   */
  public MutableDirectBuffer buffer() {
    if (released.get()) {
      throw new IllegalStateException("Use-after-release of pooled buffer handle" + tagSuffix());
    }
    return buffer;
  }

  /** @return the capacity of the wrapped buffer in bytes. */
  public int capacity() {
    return capacity;
  }

  /** @return the diagnostic tag supplied at acquisition (may be null). */
  public String tag() {
    return tag;
  }

  /** @return true once this handle has been released. */
  public boolean isReleased() {
    return released.get();
  }

  /** @return whether poison/zero-on-release mode is currently enabled. */
  public static boolean isPoisonEnabled() {
    return poison;
  }

  /**
   * Test-only: enable/disable poison (zero-on-release) mode. When on, {@link #close()} zero-fills the
   * buffer before returning it to the pool and a double-close throws even without {@code -ea}.
   */
  public static void setPoisonForTest(boolean enabled) {
    poison = enabled;
  }

  /**
   * Release the buffer back to the pool exactly once (invariant #1). Idempotent for the pool: a
   * second call never re-pools the buffer. A second call is still <em>recorded</em> as a
   * double-release in {@link BufferMetrics} and, when assertions or poison mode are enabled, throws.
   *
   * @throws IllegalStateException on a double-close when assertions are enabled or poison mode is on.
   */
  @Override
  public void close() {
    if (released.compareAndSet(false, true)) {
      BufferMetrics.getInstance().handleReleased(capacity);
      if (poison) {
        // Zero the contents so any straggling reader of a released buffer sees zeros, not
        // recycled bytes — turns a latent use-after-release into a loud, deterministic failure.
        buffer.setMemory(0, capacity, (byte) 0);
      }
      pool.release(buffer);
    } else {
      BufferMetrics.getInstance().incrementDoubleReleaseDetected();
      if (ASSERTS_ENABLED || poison) {
        throw new IllegalStateException("Double-release of pooled buffer handle" + tagSuffix());
      }
    }
  }

  private String tagSuffix() {
    return tag == null ? "" : " [" + tag + "]";
  }

  @Override
  public String toString() {
    return "PooledBufferHandle{tag=" + tag + ", capacity=" + capacity + ", released=" + released.get() + '}';
  }
}
