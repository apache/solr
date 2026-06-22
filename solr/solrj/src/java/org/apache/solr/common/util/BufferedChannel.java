package org.apache.solr.common.util;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.eclipse.jetty.io.RuntimeIOException;

import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class BufferedChannel extends OutputStream implements Closeable {

    /**
     * System property controlling the minimum buffer size (in bytes) at which {@link BufferedChannel}
     * will allocate a <em>direct</em> (off-heap) internal buffer instead of a heap buffer.
     * Buffers smaller than this threshold use {@link ByteBuffer#allocate(int)} (heap);
     * buffers at or above use {@link ByteBuffer#allocateDirect(int)} with a
     * {@link BufferMetrics#recordDirectAllocated} accounting call.
     *
     * <p>Default: {@value #DEFAULT_DIRECT_THRESHOLD} bytes. Set to {@code Integer.MAX_VALUE}
     * (or any very large value) to force heap-only allocation; set to {@code 0} to always
     * allocate direct.
     *
     * <p>Legacy behaviour (heap for all sizes) is preserved whenever the buffer size is below
     * the threshold, so the default is safe without any configuration.
     */
    public static final String DIRECT_THRESHOLD_PROP = "solr.bufferedchannel.direct.threshold";

    /**
     * Default threshold: buffers &ge; 4096 bytes use a direct ByteBuffer.
     * Smaller buffers (typical small tlog writes) remain heap-allocated.
     */
    public static final int DEFAULT_DIRECT_THRESHOLD = 4096;

    public static final int DIRECT_THRESHOLD =
        Integer.getInteger(DIRECT_THRESHOLD_PROP, DEFAULT_DIRECT_THRESHOLD);

    private final FileChannel ch;
    /** The internal write buffer. May be heap- or direct-backed depending on {@link #DIRECT_THRESHOLD}. */
    private final MutableDirectBuffer buff;
    /** True when {@link #buff} wraps a direct ByteBuffer; used to skip {@link BufferUtil#free} on heap. */
    private final boolean buffIsDirect;


    protected volatile int count;
    protected volatile int size;


    /**
     * Creates a new buffered output stream to write data to the specified underlying output stream
     * with the specified buffer size.
     *
     * <p>Allocation policy (controlled by {@value #DIRECT_THRESHOLD_PROP}):
     * <ul>
     *   <li>size &lt; threshold → heap {@link ByteBuffer#allocate(int)} (legacy-safe default)</li>
     *   <li>size &ge; threshold → direct {@link ByteBuffer#allocateDirect(int)}, with a
     *       {@link BufferMetrics#recordDirectAllocated} call to track the allocation</li>
     * </ul>
     *
     * @param out  the underlying file channel
     * @param size the buffer size in bytes
     */
    public BufferedChannel(FileChannel out, int size) {
        ch = out;
        if (size >= DIRECT_THRESHOLD) {
            // Allocate a direct buffer and account for it in BufferMetrics.
            ByteBuffer direct = ByteBuffer.allocateDirect(size);
            buff = new UnsafeBuffer(direct);
            buffIsDirect = true;
            BufferMetrics.getInstance().recordDirectAllocated(size);
        } else {
            buff = new UnsafeBuffer(ByteBuffer.allocate(size));
            buffIsDirect = false;
        }
    }

    /** Flush the internal buffer */
    public void flushBuffer() throws IOException {
        if (count > 0) {
            ByteBuffer bb = buff.byteBuffer();
            bb.limit(count + buff.wrapAdjustment());
            bb.position(0 + buff.wrapAdjustment());
            // FileChannel.write may write fewer bytes than requested (partial write);
            // drain until the buffer is fully written or data is silently lost.
            while (bb.hasRemaining()) {
                ch.write(bb);
            }
            bb.position(0 + buff.wrapAdjustment());
            bb.limit(bb.capacity() + buff.wrapAdjustment());
            count = 0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public void write(int b) throws IOException {
        if (count + 1 > buff.byteBuffer().remaining()) {
            flushBuffer();
        }
        buff.putByte(count++, (byte)b);
        size++;
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
//        if (len >= buff.byteBuffer().remaining()) {
//            /* If the request length exceeds the size of the output buffer,
//               flush the output buffer and then write the data directly.
//               In this way buffered streams will cascade harmlessly. */
//            flushBuffer();
//            ch.write(buff.byteBuffer());
//            return;
//        }
        if (len >= buff.byteBuffer().remaining()) {
            flushBuffer();

            if (len > buff.byteBuffer().remaining()) {
                // Direct large-write path: drain fully — FileChannel.write may short-write.
                ByteBuffer wrapped = ByteBuffer.wrap(b, off, len);
                while (wrapped.hasRemaining()) {
                    ch.write(wrapped);
                }
                return;
            }
        }

        buff.putBytes(count, b, off, len);
        count += len;
        size += len;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    public long size() {
        return size;
    }

    public void putInt(int v) {
        buff.putInt(count, v, ByteOrder.LITTLE_ENDIAN);

        count+=BitUtil.SIZE_OF_INT;
        size+=BitUtil.SIZE_OF_INT;
    }

    public void putLong(long v) {
        buff.putLong(count, v, ByteOrder.LITTLE_ENDIAN);

        count+=BitUtil.SIZE_OF_LONG;
        size+=BitUtil.SIZE_OF_LONG;
    }

    public void setWritten(int start) {
      size = start;
    }

    /**
     * Flushes any buffered bytes, closes the underlying {@link FileChannel}, and releases the
     * internal buffer. When the buffer is direct (allocated via {@link ByteBuffer#allocateDirect}
     * because the size met the {@value #DIRECT_THRESHOLD_PROP} threshold), the off-heap memory
     * is freed by {@link BufferUtil#free}. Heap-backed buffers are simply abandoned to GC.
     */
    public void close() throws IOException {
        flushBuffer();
        ch.close();
        if (buffIsDirect) {
            BufferUtil.free(buff);
        }
        // heap-backed UnsafeBuffer: no explicit free needed — ByteBuffer.allocate heap is GC'd.
    }

    public MutableDirectBuffer buffer() {
        return buff;
    }

    public void putFloat(float val) {
        buff.putFloat(count, val, ByteOrder.LITTLE_ENDIAN);
        count+=BitUtil.SIZE_OF_FLOAT;
        size+=BitUtil.SIZE_OF_FLOAT;
    }

    public void putShort(short val) {
        buff.putShort(count, val, ByteOrder.LITTLE_ENDIAN);
        count+=BitUtil.SIZE_OF_SHORT;
        size+=BitUtil.SIZE_OF_SHORT;
    }
}
