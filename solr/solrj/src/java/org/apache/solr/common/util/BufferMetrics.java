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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Central, process-wide registry of buffer-memory accounting for the Agrona / direct / off-heap
 * buffer subsystem (solr-ref Wave 1 foundation).
 *
 * <p>This is a deliberately small, dependency-free singleton so that it can be referenced from any
 * layer (solrj pool code, the dispatch filter, the memory-mapped {@code TransactionLog}, etc.)
 * without pulling in SolrMetrics or JMX. A separate adapter can later expose these gauges to
 * SolrMetricManager / JMX; nothing here depends on those frameworks.
 *
 * <p><b>Accounting taxonomy (categories never conflated).</b> The registry tracks several distinct
 * kinds of memory and <em>never</em> mixes them — in particular memory-mapped tlog capacity is a
 * mapped-file address-space figure, NOT JVM heap or pooled-direct memory, and is reported under its
 * own {@link Category#MMAP_TLOG} counters:
 * <ul>
 *   <li>{@link Category#POOLED_DIRECT} — pooled off-heap direct buffers ({@link ArrayByteBufferPool}
 *       direct buckets). Two figures: cumulative <em>allocated</em> (monotonic; counts genuinely new
 *       {@code allocateDirect} buffers minted on a pool miss) and currently <em>retained</em>
 *       (gauge; bytes sitting idle in the pool, sourced from
 *       {@link AbstractByteBufferPool#getDirectMemory()}).</li>
 *   <li>{@link Category#HEAP_AGRONA} — pooled on-heap Agrona buffers (the pool's indirect buckets);
 *       same allocated/retained split, retained sourced from
 *       {@link AbstractByteBufferPool#getHeapMemory()}.</li>
 *   <li>{@link Category#MMAP_TLOG} — memory-mapped {@code TransactionLog} buffers: mapped capacity
 *       (address space), logical size (bytes actually written), remap count, active reader count,
 *       and forced-close count. <b>Mapped capacity is never added to any heap/direct total.</b></li>
 *   <li>{@link Category#JETTY} — buffers owned by the embedded Jetty container's own pools; tracked
 *       as a tag for categorization only (Solr does not own their lifecycle).</li>
 *   <li>{@link Category#ORDINARY_HEAP} — plain {@code byte[]} / heap {@link java.nio.ByteBuffer}
 *       allocations; a tag for completeness, not actively summed.</li>
 * </ul>
 *
 * <p>Beyond the per-category figures the registry also tracks <em>active single-buffer handles</em>
 * ({@link PooledBufferHandle}) — the live (acquired-but-not-yet-released) handle byte total and
 * count — and a global {@code doubleReleaseDetected} counter incremented whenever a release-guard
 * (a {@link PooledBufferHandle#close()} double-close, or a legacy {@code AtomicBoolean} CAS no-op)
 * catches a second release of the same physical buffer (invariant #1).
 *
 * <p><b>Thread safety:</b> all mutators are lock-free ({@link AtomicLong}); supplier registration
 * uses a copy-on-write list. Gauge reads are point-in-time snapshots.
 */
public final class BufferMetrics {

  /** Mutually-exclusive memory categories. Capacity figures are never moved between categories. */
  public enum Category {
    /** Pooled off-heap direct buffers (Agrona {@code ExpandableDirectByteBuffer} via the pool). */
    POOLED_DIRECT,
    /** Pooled on-heap Agrona buffers (the pool's indirect buckets). */
    HEAP_AGRONA,
    /** Memory-mapped tlog capacity/logical-size (mapped file address space, NOT heap/direct). */
    MMAP_TLOG,
    /** Buffers owned by the embedded Jetty container's pools (tag only). */
    JETTY,
    /** Ordinary {@code byte[]} / heap {@link java.nio.ByteBuffer} (tag only). */
    ORDINARY_HEAP
  }

  private static final BufferMetrics INSTANCE = new BufferMetrics();

  /** @return the process-wide singleton registry. */
  public static BufferMetrics getInstance() {
    return INSTANCE;
  }

  // --- pooled-direct ---
  private final AtomicLong directAllocated = new AtomicLong();
  private final CopyOnWriteArrayList<LongSupplier> directRetainedSuppliers = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<LongSupplier> directAllocatedSuppliers = new CopyOnWriteArrayList<>();

  // --- heap-Agrona ---
  private final AtomicLong heapAgronaAllocated = new AtomicLong();
  private final CopyOnWriteArrayList<LongSupplier> heapAgronaRetainedSuppliers = new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<LongSupplier> heapAgronaAllocatedSuppliers = new CopyOnWriteArrayList<>();

  // --- active single-buffer handles (PooledBufferHandle) ---
  private final AtomicLong activeHandleBytes = new AtomicLong();
  private final AtomicLong activeHandleCount = new AtomicLong();

  // --- mmap tlog (capacity NEVER added to heap/direct totals) ---
  private final AtomicLong mmapCapacityBytes = new AtomicLong();
  private final AtomicLong mmapLogicalSizeBytes = new AtomicLong();
  private final AtomicLong mmapRemapCount = new AtomicLong();
  private final AtomicLong mmapActiveReaders = new AtomicLong();
  private final AtomicLong mmapForcedCloseCount = new AtomicLong();

  // --- release-guard violations ---
  private final AtomicLong doubleReleaseDetected = new AtomicLong();

  private BufferMetrics() {}

  // =====================================================================================
  // Pooled direct
  // =====================================================================================

  /** Record {@code bytes} of genuinely new direct allocation (pool miss minting a new buffer). */
  public void recordDirectAllocated(long bytes) {
    directAllocated.addAndGet(bytes);
  }

  /**
   * Register a gauge supplier for currently-retained pooled-direct bytes (e.g.
   * {@link AbstractByteBufferPool#getDirectMemory()}). Multiple suppliers are summed.
   */
  public void registerDirectRetainedSupplier(LongSupplier supplier) {
    directRetainedSuppliers.add(supplier);
  }

  /** Register a gauge supplier for cumulative pooled-direct allocated bytes. Suppliers are summed. */
  public void registerDirectAllocatedSupplier(LongSupplier supplier) {
    directAllocatedSuppliers.add(supplier);
  }

  /** @return cumulative direct bytes allocated (local counter + registered suppliers). */
  public long getDirectAllocatedBytes() {
    return directAllocated.get() + sum(directAllocatedSuppliers);
  }

  /** @return direct bytes currently retained (idle) in the pool(s). */
  public long getDirectRetainedBytes() {
    return sum(directRetainedSuppliers);
  }

  // =====================================================================================
  // Pooled heap-Agrona
  // =====================================================================================

  /** Record {@code bytes} of genuinely new pooled heap-Agrona allocation (pool miss). */
  public void recordHeapAgronaAllocated(long bytes) {
    heapAgronaAllocated.addAndGet(bytes);
  }

  /** Register a gauge supplier for retained pooled heap-Agrona bytes. Suppliers are summed. */
  public void registerHeapAgronaRetainedSupplier(LongSupplier supplier) {
    heapAgronaRetainedSuppliers.add(supplier);
  }

  /** Register a gauge supplier for cumulative pooled heap-Agrona allocated bytes. */
  public void registerHeapAgronaAllocatedSupplier(LongSupplier supplier) {
    heapAgronaAllocatedSuppliers.add(supplier);
  }

  /** @return cumulative pooled heap-Agrona bytes allocated. */
  public long getHeapAgronaAllocatedBytes() {
    return heapAgronaAllocated.get() + sum(heapAgronaAllocatedSuppliers);
  }

  /** @return pooled heap-Agrona bytes currently retained (idle) in the pool(s). */
  public long getHeapAgronaRetainedBytes() {
    return sum(heapAgronaRetainedSuppliers);
  }

  // =====================================================================================
  // Active single-buffer handles (PooledBufferHandle)
  // =====================================================================================

  /** Account a handle acquisition of {@code bytes} (live handle byte total + count both rise). */
  public void handleAcquired(long bytes) {
    activeHandleBytes.addAndGet(bytes);
    activeHandleCount.incrementAndGet();
  }

  /** Account a handle release of {@code bytes} (live handle byte total + count both fall). */
  public void handleReleased(long bytes) {
    activeHandleBytes.addAndGet(-bytes);
    activeHandleCount.decrementAndGet();
  }

  /** @return bytes held by currently-live (acquired, not yet released) {@link PooledBufferHandle}s. */
  public long getActiveHandleBytes() {
    return activeHandleBytes.get();
  }

  /** @return number of currently-live {@link PooledBufferHandle}s. */
  public long getActiveHandleCount() {
    return activeHandleCount.get();
  }

  // =====================================================================================
  // Memory-mapped tlog (capacity is mapped address space — NEVER heap/direct)
  // =====================================================================================

  /** Adjust mapped tlog capacity by {@code deltaBytes} (positive on map/grow, negative on unmap). */
  public void addMmapCapacity(long deltaBytes) {
    mmapCapacityBytes.addAndGet(deltaBytes);
  }

  /** @return total memory-mapped tlog capacity (address space) across all live tlogs. */
  public long getMmapCapacityBytes() {
    return mmapCapacityBytes.get();
  }

  /** Adjust mapped tlog logical size (bytes actually written) by {@code deltaBytes}. */
  public void addMmapLogicalSize(long deltaBytes) {
    mmapLogicalSizeBytes.addAndGet(deltaBytes);
  }

  /** @return total logical (written) bytes across all live mapped tlogs. */
  public long getMmapLogicalSizeBytes() {
    return mmapLogicalSizeBytes.get();
  }

  /** Increment the count of tlog remap (resize) events. */
  public void incrementRemapCount() {
    mmapRemapCount.incrementAndGet();
  }

  /** @return cumulative number of tlog remap (resize) events. */
  public long getRemapCount() {
    return mmapRemapCount.get();
  }

  /** Increment the live tlog reader/replayer count (call at reader incref sites). */
  public void incrementActiveReaders() {
    mmapActiveReaders.incrementAndGet();
  }

  /** Decrement the live tlog reader/replayer count (call at reader decref sites). */
  public void decrementActiveReaders() {
    mmapActiveReaders.decrementAndGet();
  }

  /**
   * @return number of live tlog readers/replayers. This is a dedicated counter, NOT the tlog
   *     {@code refcount} (which starts at 1 for the structural self-reference and would over-count).
   */
  public long getActiveReaders() {
    return mmapActiveReaders.get();
  }

  /** Increment the count of forced (drain-then-force) tlog unmaps. */
  public void incrementForcedCloseCount() {
    mmapForcedCloseCount.incrementAndGet();
  }

  /** @return cumulative number of forced tlog unmaps (the {@code forceClose} escape hatch). */
  public long getForcedCloseCount() {
    return mmapForcedCloseCount.get();
  }

  // =====================================================================================
  // Release-guard violations (invariant #1)
  // =====================================================================================

  /**
   * Record that a second release of an already-released physical buffer was caught by a guard
   * (a {@link PooledBufferHandle#close()} double-close, or a legacy CAS no-op).
   */
  public void incrementDoubleReleaseDetected() {
    doubleReleaseDetected.incrementAndGet();
  }

  /** @return number of double-release attempts caught by release guards. */
  public long getDoubleReleaseDetected() {
    return doubleReleaseDetected.get();
  }

  // =====================================================================================
  // Snapshots
  // =====================================================================================

  /**
   * @return a point-in-time map of currently-retained bytes per category. {@code MMAP_TLOG} reports
   *     mapped capacity (kept distinct from heap/direct); tag-only categories report 0.
   */
  public Map<Category, Long> retainedByCategory() {
    EnumMap<Category, Long> m = new EnumMap<>(Category.class);
    m.put(Category.POOLED_DIRECT, getDirectRetainedBytes());
    m.put(Category.HEAP_AGRONA, getHeapAgronaRetainedBytes());
    m.put(Category.MMAP_TLOG, getMmapCapacityBytes());
    m.put(Category.JETTY, 0L);
    m.put(Category.ORDINARY_HEAP, 0L);
    return m;
  }

  /**
   * Reset all locally-held counters and clear registered suppliers. <b>Test-only.</b> Does not
   * affect the underlying pools' own accounting.
   */
  public void reset() {
    directAllocated.set(0);
    heapAgronaAllocated.set(0);
    activeHandleBytes.set(0);
    activeHandleCount.set(0);
    mmapCapacityBytes.set(0);
    mmapLogicalSizeBytes.set(0);
    mmapRemapCount.set(0);
    mmapActiveReaders.set(0);
    mmapForcedCloseCount.set(0);
    doubleReleaseDetected.set(0);
    directRetainedSuppliers.clear();
    directAllocatedSuppliers.clear();
    heapAgronaRetainedSuppliers.clear();
    heapAgronaAllocatedSuppliers.clear();
  }

  private static long sum(CopyOnWriteArrayList<LongSupplier> suppliers) {
    long total = 0;
    for (LongSupplier s : suppliers) {
      total += s.getAsLong();
    }
    return total;
  }
}
