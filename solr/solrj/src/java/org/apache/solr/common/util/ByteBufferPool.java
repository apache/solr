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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * <p>A {@link ByteBuffer} pool.</p>
 * <p>Acquired buffers may be {@link #release(MutableDirectBuffer) released} but they do not need to;
 * if they are released, they may be recycled and reused, otherwise they will be garbage
 * collected as usual.</p>
 */
public interface ByteBufferPool
{
    /**
     * <p>Requests a {@link ByteBuffer} of the given size.</p>
     * <p>The returned buffer may have a bigger capacity than the size being
     * requested but it will have the limit set to the given size.</p>
     *
     * @param size the size of the buffer
     * @param direct whether the buffer must be direct or not
     * @return the requested buffer
     * @see #release(MutableDirectBuffer)
     */
    public MutableDirectBuffer acquire(int size, boolean direct);

    /**
     * <p>Returns a {@link ByteBuffer}, usually obtained with {@link #acquire(int, boolean)}
     * (but not necessarily), making it available for recycling and reuse.</p>
     *
     * @param buffer the buffer to return
     * @see #acquire(int, boolean)
     */
    void release(MutableDirectBuffer buffer);

    /**
     * <p>Removes a {@link ByteBuffer} that was previously obtained with {@link #acquire(int, boolean)}.</p>
     * <p>The buffer will not be available for further reuse.</p>
     *
     * @param buffer the buffer to remove
     * @see #acquire(int, boolean)
     * @see #release(MutableDirectBuffer)
     */
    default void remove(MutableDirectBuffer buffer)
    {
    }

    /**
     * <p>Creates a new ByteBuffer of the given capacity and the given directness.</p>
     *
     * @param capacity the ByteBuffer capacity
     * @param direct the ByteBuffer directness
     * @return a newly allocated ByteBuffer
     */
    default MutableDirectBuffer newByteBuffer(int capacity, boolean direct)
    {
        if (direct) {
            return new ExpandableDirectByteBuffer(capacity);
        }
        return new ExpandableArrayBuffer(capacity);
    }

    public static class Lease
    {
        private final ByteBufferPool byteBufferPool;
        private final List<MutableDirectBuffer> buffers;
        private final List<Boolean> recycles;

        public Lease(ByteBufferPool byteBufferPool)
        {
            this.byteBufferPool = byteBufferPool;
            this.buffers = new ArrayList<>();
            this.recycles = new ArrayList<>();
        }

        public MutableDirectBuffer acquire(int capacity, boolean direct)
        {
            MutableDirectBuffer buffer = byteBufferPool.acquire(capacity, direct);
            buffer.byteBuffer().position(buffer.wrapAdjustment());
            buffer.byteBuffer().limit(buffer.byteBuffer().capacity() + buffer.wrapAdjustment());

            return buffer;
        }

        public void append(MutableDirectBuffer buffer, boolean recycle)
        {
            buffers.add(buffer);
            recycles.add(recycle);
        }

        public void insert(int index, MutableDirectBuffer buffer, boolean recycle)
        {
            buffers.add(index, buffer);
            recycles.add(index, recycle);
        }

        public List<MutableDirectBuffer> getByteBuffers()
        {
            return buffers;
        }



        public int getSize()
        {
            return buffers.size();
        }

        public void recycle()
        {
            for (int i = 0; i < buffers.size(); ++i)
            {
                MutableDirectBuffer buffer = buffers.get(i);
                if (recycles.get(i))
                    release(buffer);
            }
            buffers.clear();
            recycles.clear();
        }

        public void release(MutableDirectBuffer buffer)
        {
            byteBufferPool.release(buffer);
        }
    }

    public static class Bucket
    {
        private final Queue<MutableDirectBuffer> _queue = new LinkedTransferQueue<>();
        private final int _capacity;
        private final int _maxSize;
        private final AtomicInteger _size;
        private final AtomicLong _lastUpdate = new AtomicLong(System.nanoTime());

        public Bucket(int capacity, int maxSize)
        {
            _capacity = capacity;
            _maxSize = maxSize;
            _size = maxSize > 0 ? new AtomicInteger() : null;
        }

        public MutableDirectBuffer acquire()
        {
            MutableDirectBuffer buffer = queuePoll();
            if (buffer == null)
                return null;
            if (_size != null)
                _size.decrementAndGet();
            return buffer;
        }

        public void release(MutableDirectBuffer buffer)
        {
            _lastUpdate.setOpaque(System.nanoTime());

            if (buffer.byteBuffer() != null) {
                buffer.byteBuffer().position(buffer.wrapAdjustment());
                buffer.byteBuffer().limit(buffer.byteBuffer().capacity() + buffer.wrapAdjustment());
            }

            if (_size == null)
                queueOffer(buffer);
            else if (_size.incrementAndGet() <= _maxSize)
                queueOffer(buffer);
            else
                _size.decrementAndGet();
        }

        protected static void decrementMemory(MutableDirectBuffer buffer)
        {

           // org.agrona.BufferUtil.free(buffer);
        }

        public void clear()
        {
            clear(Bucket::decrementMemory);
        }

        void clear(Consumer<MutableDirectBuffer> memoryFn)
        {
            int size = _size == null ? 0 : _size.get() - 1;
            while (size >= 0)
            {
                MutableDirectBuffer buffer = queuePoll();
                if (buffer == null)
                    break;
                if (memoryFn != null)
                    memoryFn.accept(buffer);
                if (_size != null)
                {
                    _size.decrementAndGet();
                    --size;
                }
            }
        }

        private void queueOffer(MutableDirectBuffer buffer)
        {
            _queue.offer(buffer);
        }

        private MutableDirectBuffer queuePoll()
        {
            return _queue.poll();
        }

        boolean isEmpty()
        {
            return _queue.isEmpty();
        }

        int size()
        {
            return _queue.size();
        }

        long getLastUpdate()
        {
            return _lastUpdate.getOpaque();
        }

        @Override
        public String toString()
        {
            return String.format("%s@%x{%d/%d@%d}", getClass().getSimpleName(), hashCode(), size(), _maxSize, _capacity);
        }
    }
}
