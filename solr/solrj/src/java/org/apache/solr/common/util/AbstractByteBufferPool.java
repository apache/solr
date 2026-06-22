//
// ========================================================================
// Copyright (c) 1995-2021 Mort Bay Consulting Pty Ltd and others.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License v. 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// ========================================================================
//

package org.apache.solr.common.util;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.agrona.MutableDirectBuffer;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.annotation.ManagedObject;
import org.eclipse.jetty.util.annotation.ManagedOperation;

@ManagedObject
abstract class AbstractByteBufferPool implements ByteBufferPool
{
    private final int _factor;
    private final int _maxQueueLength;
    private final long _maxHeapMemory;
    private final AtomicLong _heapMemory = new AtomicLong();
    private final long _maxDirectMemory;
    private final AtomicLong _directMemory = new AtomicLong();
    // Cumulative (monotonic) bytes ever freshly allocated on a pool miss — distinct from the
    // retained gauges above, which track bytes currently idle in the pool. Fed into BufferMetrics.
    private final AtomicLong _directAllocated = new AtomicLong();
    private final AtomicLong _heapAllocated = new AtomicLong();

    protected AbstractByteBufferPool(int factor, int maxQueueLength, long maxHeapMemory, long maxDirectMemory)
    {
        _factor = factor <= 0 ? 1024 : factor;
        _maxQueueLength = maxQueueLength;
        _maxHeapMemory = maxHeapMemory;
        _maxDirectMemory = maxDirectMemory;
    }

    protected int getCapacityFactor()
    {
        return _factor;
    }

    protected int getMaxQueueLength()
    {
        return _maxQueueLength;
    }

    protected void decrementMemory(MutableDirectBuffer buffer)
    {
        updateMemory(buffer, false);
    }

    protected void incrementMemory(MutableDirectBuffer buffer)
    {
        updateMemory(buffer, true);
    }

    private void updateMemory(MutableDirectBuffer buffer, boolean addOrSub)
    {
        AtomicLong memory = buffer.byteBuffer() != null ? _directMemory : _heapMemory;
        int capacity = buffer.capacity();
        memory.addAndGet(addOrSub ? capacity : -capacity);
    }

    protected void releaseExcessMemory(boolean direct, Consumer<Boolean> clearFn)
    {
        long maxMemory = direct ? _maxDirectMemory : _maxHeapMemory;
        if (maxMemory > 0)
        {
            while (getMemory(direct) > maxMemory)
            {
                clearFn.accept(direct);
            }
        }
    }

    /**
     * Record a genuinely-new allocation (pool miss minting a fresh buffer). Bumps the cumulative
     * allocated counter for the buffer's category (direct vs heap). Retained accounting is handled
     * separately by {@link #incrementMemory}/{@link #decrementMemory}.
     */
    protected void recordAllocated(MutableDirectBuffer buffer)
    {
        AtomicLong allocated = buffer.byteBuffer() != null ? _directAllocated : _heapAllocated;
        allocated.addAndGet(buffer.capacity());
    }

    @ManagedAttribute("The bytes retained by direct ByteBuffers")
    public long getDirectMemory()
    {
        return getMemory(true);
    }

    @ManagedAttribute("The bytes retained by heap ByteBuffers")
    public long getHeapMemory()
    {
        return getMemory(false);
    }

    @ManagedAttribute("Cumulative bytes freshly allocated as direct ByteBuffers (pool misses)")
    public long getDirectAllocated()
    {
        return _directAllocated.get();
    }

    @ManagedAttribute("Cumulative bytes freshly allocated as heap ByteBuffers (pool misses)")
    public long getHeapAllocated()
    {
        return _heapAllocated.get();
    }

    public long getMemory(boolean direct)
    {
        AtomicLong memory = direct ? _directMemory : _heapMemory;
        return memory.get();
    }

    @ManagedOperation(value = "Clears this ByteBufferPool", impact = "ACTION")
    public void clear()
    {
        _heapMemory.set(0);
        _directMemory.set(0);
    }
}
