package org.apache.solr.common.util;

import org.agrona.BitUtil;
import org.agrona.concurrent.MappedResizeableBuffer;

import java.io.*;
import java.nio.ByteOrder;

/**
 * Input stream over a memory-mapped tlog buffer.
 *
 * <p><b>Buffer ownership:</b> this is a non-owning reader view over a {@link MappedResizeableBuffer}
 * owned by the {@code TransactionLog}. It must never unmap/close that buffer (invariant #3:
 * no unmap while a reader can access it). The {@code length} field bounds reads to the logical
 * written size, which is distinct from the mapping's capacity — keep the two separate in any
 * accounting ({@link BufferMetrics.Category#MMAP_TLOG} capacity vs logical size).
 */
public class DirectMemBufferedInputStream extends JavaBinInputStream implements DataInputInputStream, DataInput {

    private final MappedResizeableBuffer buffer;
    private int offset;
    private long length;
    private long position;

    public DirectMemBufferedInputStream(final MappedResizeableBuffer buffer, long length) {
        this.buffer = buffer;
        this.length = length;
    }

    @Override
    public int read() throws IOException {
        if (position + 1 > length) {
            throw new EOFException();
        }
        // InputStream.read() must return the byte as an unsigned value in 0..255 (or -1 at EOF).
        // buffer.getByte returns a signed byte, so a 0xFF byte would come back as -1 and be
        // misinterpreted as EOF by callers (e.g. JavaBinCodec.read / readVLong continuation bytes).
        int b = buffer.getByte(offset + position) & 0xff;
        ++position;

        return b;
    }


    public int read(final byte[] dstBytes, final int dstOffset, final int length) throws EOFException {
        if (position + length > this.length) {
            throw new EOFException();
        }
        int bytesRead = length;

        buffer.getBytes(offset + position, dstBytes, dstOffset, length);
        position += bytesRead;

        return bytesRead;
    }

    public int available() {
        return (int) (length - position);
    }

    /**
     * Bound a read against the logical written {@code length} and fail with {@link EOFException}
     * before touching the mapped buffer. The mapping has pre-grown slack beyond the written size
     * (and beyond {@code raf.length()}), so an unchecked {@code buffer.getX(position)} past the
     * logical end either reads zero/stale slack (silent garbage) or, with a torn length field,
     * walks past the mapping capacity into a native SIGSEGV that {@code catch(Throwable)} cannot
     * catch. Throwing EOFException here lets {@code LogReader.next()}'s torn-record tolerance treat
     * it as a clean end-of-log.
     */
    private void ensureAvailable(long n) throws EOFException {
        if (position + n > length) {
            throw new EOFException();
        }
    }

    /**
     * The offset within the underlying buffer at which to start.
     *
     * @return offset within the underlying buffer at which to start.
     */
    public int offset() {
        return offset;
    }

    /**
     * The length of the underlying buffer to use
     *
     * @return length of the underlying buffer to use
     */
    public long length() {
        return length;
    }

    /**
     * The underlying buffer being wrapped.
     *
     * @return the underlying buffer being wrapped.
     */
    public MappedResizeableBuffer buffer() {
        return buffer;
    }

    public int position() {
        return (int) position;
    }

    public void position(long pos) {
        this.position = pos;
    }

    @Override
    public void readFully(byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte b[], int off, int len) throws IOException {
        ensureAvailable(len);
        buffer.getBytes(position, b, off, len);
        position += len;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        position += n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public byte readByte() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_BYTE);
        return buffer.getByte(position++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int ch = read() & 0xff;
        if (ch < 0)
            throw new EOFException();
        return ch;
    }


    @Override
    public short readShort() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_SHORT);
        var s = buffer.getShort(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_SHORT;
        return s;
    }

    @Override
    public void readFully(JavaBinInputStream dis, byte[] bytes) throws IOException {
      ensureAvailable(bytes.length);
      buffer.getBytes(position, bytes);
      position += bytes.length;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_SHORT);
        var s = buffer.getShort(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_SHORT;
        return s;
    }

    @Override
    public char readChar() throws IOException {
        throw new UnsupportedEncodingException();
    }

    public int readInt() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_INT);
        var i = buffer.getInt(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_INT;
        return i;
    }

    public int getInt() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_INT);
        var i = buffer.getInt(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_INT;
        return i;
    }

    @Override
    public long readLong() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_LONG);
        var l = buffer.getLong(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_LONG;
        return l;
    }

    @Override
    public float readFloat() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_FLOAT);
        var f = buffer.getFloat(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_FLOAT;
        return f;
    }

    @Override
    public double readDouble() throws IOException {
        ensureAvailable(BitUtil.SIZE_OF_DOUBLE);
        var d = buffer.getDouble(position, ByteOrder.LITTLE_ENDIAN);
        position += BitUtil.SIZE_OF_DOUBLE;
        return d;
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedEncodingException();
    }
}
