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

    private final FileChannel ch;
    private final MutableDirectBuffer buff;


    protected volatile int count;
    protected volatile int size;


    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream with the specified buffer
     * size.
     *
     * @param   out    the underlying output stream.
     * @param   size   the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public BufferedChannel(FileChannel out, int size) {
        ch = out;
        //buff = ExpandableBuffers.getInstance().acquire(-1, true);
        buff = new UnsafeBuffer(ByteBuffer.allocate(size));

        //buff.byteBuffer().limit(buff.byteBuffer().capacity());
    }

    /** Flush the internal buffer */
    public void flushBuffer() throws IOException {
        if (count > 0) {
            ByteBuffer bb = buff.byteBuffer();
         //   bb.flip();
            bb.limit(count + buff.wrapAdjustment());
            bb.position(0 + buff.wrapAdjustment());
        //    BufferUtil.flipToFlush(bb, pos);
            ch.write(bb);
       //     pos = BufferUtil.flipToFill(buff.byteBuffer());
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
                ch.write(ByteBuffer.wrap(b, off, len));
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

    public void close() throws IOException {
        flushBuffer();
        ch.close();
        BufferUtil.free(buff);
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
