package org.apache.solr.common.util;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.eclipse.jetty.io.MappedByteBufferPool;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.util.ProcessorUtils;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.annotation.ManagedObject;
import org.eclipse.jetty.util.annotation.ManagedOperation;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A ByteBuffer pool where ByteBuffers are held in queues that are held in a Map.</p>
 * <p>Given a capacity {@code factor} of 1024, the Map entry with key {@code 1} holds a
 * queue of ByteBuffers each of capacity 1024, the Map entry with key {@code 2} holds a
 * queue of ByteBuffers each of capacity 2048, and so on.</p>
 */
@ManagedObject
public class NonBlockingMappedByteBufferPool implements org.eclipse.jetty.io.ByteBufferPool
{
    private static final Logger LOG = LoggerFactory.getLogger(MappedByteBufferPool.class);

    private final ConcurrentMap<Integer, Bucket> _directBuffers = new NonBlockingHashMap<>(64);
    private final ConcurrentMap<Integer, Bucket> _heapBuffers = new NonBlockingHashMap<>(64);
    private final Function<Integer, Bucket> _newBucket;

    private final int _factor;
    private final int _maxQueueLength;
    private final long _maxHeapMemory;
    private final AtomicLong _heapMemory = new AtomicLong();
    private final long _maxDirectMemory;
    private final AtomicLong _directMemory = new AtomicLong();

    /**
     * Creates a new MappedByteBufferPool with a default configuration.
     */
    public NonBlockingMappedByteBufferPool()
    {
        this(-1);
    }

    /**
     * Creates a new MappedByteBufferPool with the given capacity factor.
     *
     * @param factor the capacity factor
     */
    public NonBlockingMappedByteBufferPool(int factor)
    {
        this(factor, -1);
    }

    /**
     * Creates a new MappedByteBufferPool with the given configuration.
     *
     * @param factor the capacity factor
     * @param maxQueueLength the maximum ByteBuffer queue length
     */
    public NonBlockingMappedByteBufferPool(int factor, int maxQueueLength)
    {
        this(factor, maxQueueLength, null);
    }

    /**
     * Creates a new MappedByteBufferPool with the given configuration.
     *
     * @param factor the capacity factor
     * @param maxQueueLength the maximum ByteBuffer queue length
     * @param newBucket the function that creates a Bucket
     */
    public NonBlockingMappedByteBufferPool(int factor, int maxQueueLength, Function<Integer, Bucket> newBucket)
    {
        this(factor, maxQueueLength, newBucket, -1, -1);
    }

    /**
     * Creates a new MappedByteBufferPool with the given configuration.
     *
     * @param factor the capacity factor
     * @param maxQueueLength the maximum ByteBuffer queue length
     * @param newBucket the function that creates a Bucket
     * @param maxHeapMemory the max heap memory in bytes
     * @param maxDirectMemory the max direct memory in bytes
     */
    public NonBlockingMappedByteBufferPool(int factor, int maxQueueLength, Function<Integer, Bucket> newBucket, long maxHeapMemory, long maxDirectMemory)
    {

        _factor = factor <= 0 ? 2048 : factor;
        _maxQueueLength = maxQueueLength;
        _maxHeapMemory = maxHeapMemory;
        _maxDirectMemory = maxDirectMemory;
        _newBucket = newBucket != null ? newBucket : this::newBucket;
    }

    private Bucket newBucket(int key)
    {
        return new Bucket(key * _factor, _maxQueueLength);
    }

    @Override
    public ByteBuffer acquire(int size, boolean direct)
    {
        int b = bucketFor(size);
        int capacity = b * _factor;
        ConcurrentMap<Integer, Bucket> buffers = bucketsFor(direct);
        Bucket bucket = buffers.get(b);
        if (bucket == null)
            return newByteBuffer(capacity, direct);
        ByteBuffer buffer = bucket.acquire();
        if (buffer == null)
            return newByteBuffer(capacity, direct);
        decrementMemory(buffer);
        return buffer;
    }

    @Override
    public void release(ByteBuffer buffer)
    {
        if (buffer == null)
            return; // nothing to do

        int capacity = buffer.capacity();
        // Validate that this buffer is from this pool.
        if ((capacity % _factor) != 0)
        {
            if (LOG.isDebugEnabled())
                LOG.debug("ByteBuffer {} does not belong to this pool, discarding it", BufferUtil.toDetailString(buffer));
            return;
        }

        int b = bucketFor(capacity);
        boolean direct = buffer.isDirect();
        ConcurrentMap<Integer, Bucket> buckets = bucketsFor(direct);
        Bucket bucket = buckets.computeIfAbsent(b, _newBucket);
        bucket.release(buffer);
        incrementMemory(buffer);
        releaseExcessMemory(direct, this::clearOldestBucket);
    }

    public void clear()
    {
        _heapMemory.set(0);
        _directMemory.set(0);
        _directBuffers.values().forEach(Bucket::clear);
        _directBuffers.clear();
        _heapBuffers.values().forEach(Bucket::clear);
        _heapBuffers.clear();
    }

    private void clearOldestBucket(boolean direct)
    {
        long oldest = Long.MAX_VALUE;
        int index = -1;
        ConcurrentMap<Integer, Bucket> buckets = bucketsFor(direct);
        for (Map.Entry<Integer, Bucket> entry : buckets.entrySet())
        {
            Bucket bucket = entry.getValue();
            long lastUpdate = bucket.getLastUpdate();
            if (lastUpdate < oldest)
            {
                oldest = lastUpdate;
                index = entry.getKey();
            }
        }
        if (index >= 0)
        {
            Bucket bucket = buckets.remove(index);
            // The same bucket may be concurrently
            // removed, so we need this null guard.
            if (bucket != null)
                bucket.clear(this::decrementMemory);
        }
    }

    private int bucketFor(int size)
    {
        int factor = _factor;
        int bucket = size / factor;
        if (bucket * factor != size)
            ++bucket;
        return bucket;
    }
    protected int getCapacityFactor()
    {
        return _factor;
    }

    protected int getMaxQueueLength()
    {
        return _maxQueueLength;
    }

    protected void decrementMemory(ByteBuffer buffer)
    {
        updateMemory(buffer, false);
    }

    protected void incrementMemory(ByteBuffer buffer)
    {
        updateMemory(buffer, true);
    }

    private void updateMemory(ByteBuffer buffer, boolean addOrSub)
    {
        AtomicLong memory = buffer.isDirect() ? _directMemory : _heapMemory;
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

    public long getMemory(boolean direct)
    {
        AtomicLong memory = direct ? _directMemory : _heapMemory;
        return memory.get();
    }


    @ManagedAttribute("The number of pooled direct ByteBuffers")
    public long getDirectByteBufferCount()
    {
        return getByteBufferCount(true);
    }

    @ManagedAttribute("The number of pooled heap ByteBuffers")
    public long getHeapByteBufferCount()
    {
        return getByteBufferCount(false);
    }

    private long getByteBufferCount(boolean direct)
    {
        return bucketsFor(direct).values().stream()
            .mapToLong(Bucket::size)
            .sum();
    }

    // Package local for testing
    ConcurrentMap<Integer, Bucket> bucketsFor(boolean direct)
    {
        return direct ? _directBuffers : _heapBuffers;
    }

    public static class Tagged extends MappedByteBufferPool
    {
        private final AtomicInteger tag = new AtomicInteger();

        @Override
        public ByteBuffer newByteBuffer(int capacity, boolean direct)
        {
            ByteBuffer buffer = super.newByteBuffer(capacity + 4, direct);
            buffer.limit(buffer.capacity());
            buffer.putInt(tag.incrementAndGet());
            ByteBuffer slice = buffer.slice();
            BufferUtil.clear(slice);
            return slice;
        }
    }

    public static class Bucket
    {
        private final Queue<ByteBuffer> _queue = new LinkedTransferQueue<>();
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

        public ByteBuffer acquire()
        {
            ByteBuffer buffer = queuePoll();
            if (buffer == null)
                return null;
            if (_size != null)
                _size.decrementAndGet();
            return buffer;
        }

        public void release(ByteBuffer buffer)
        {
            _lastUpdate.setOpaque(System.nanoTime());
            BufferUtil.clear(buffer);
            if (_size == null)
                queueOffer(buffer);
            else if (_size.incrementAndGet() <= _maxSize)
                queueOffer(buffer);
            else
                _size.decrementAndGet();
        }

        public void clear()
        {
            clear(null);
        }

        void clear(Consumer<ByteBuffer> memoryFn)
        {
            int size = _size == null ? 0 : _size.get() - 1;
            while (size >= 0)
            {
                ByteBuffer buffer = queuePoll();
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

        private void queueOffer(ByteBuffer buffer)
        {
            _queue.offer(buffer);
        }

        private ByteBuffer queuePoll()
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
