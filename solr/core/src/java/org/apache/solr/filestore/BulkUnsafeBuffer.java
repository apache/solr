package org.apache.solr.filestore;

import org.agrona.*;
import org.agrona.concurrent.AtomicBuffer;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.agrona.AsciiEncoding.*;
import static org.agrona.BitUtil.*;
import static org.agrona.BufferUtil.*;
import static org.agrona.UnsafeAccess.UNSAFE;

public class BulkUnsafeBuffer implements AtomicBuffer {

    /**
     * Buffer alignment to ensure atomic word accesses.
     */
    public static final int ALIGNMENT = SIZE_OF_LONG;

    /**
     * Name of the system property that specify if the bounds checks should be disabled.
     * To disable bounds checks set this property to {@code true}.
     */
    public static final String DISABLE_BOUNDS_CHECKS_PROP_NAME = "agrona.disable.bounds.checks";

    /**
     * Should bounds checks be done or not. Controlled by the {@link #DISABLE_BOUNDS_CHECKS_PROP_NAME} system property.
     *
     * @see #DISABLE_BOUNDS_CHECKS_PROP_NAME
     */
    public static final boolean SHOULD_BOUNDS_CHECK = !Boolean.getBoolean(DISABLE_BOUNDS_CHECKS_PROP_NAME);

    private long addressOffset;
    private long capacity;
    private byte[] byteArray;
    private ByteBuffer byteBuffer;

    public static BulkUnsafeBuffer wrapBuffer(final ByteBuffer buffer) {


        BulkUnsafeBuffer buf = new BulkUnsafeBuffer();
        buf.wrap(buffer);

        return buf;
    }

    private static void boundsCheckWrap(final int offset, final int length, final int capacity) {
        if (offset < 0) {
            throw new IllegalArgumentException("invalid offset: " + offset);
        }

        if (length < 0) {
            throw new IllegalArgumentException("invalid length: " + length);
        }

        if ((offset > capacity - length) || (length > capacity - offset)) {
            throw new IllegalArgumentException(
                    "offset=" + offset + " length=" + length + " not valid for capacity=" + capacity);
        }
    }

    private static void lengthCheck(final int length) {
        if (length < 0) {
            throw new IllegalArgumentException("negative length: " + length);
        }
    }

    private static void lengthCheck(final long length) {
        if (length < 0) {
            throw new IllegalArgumentException("negative length: " + length);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final byte[] buffer) {
        addressOffset = ARRAY_BASE_OFFSET;
        capacity = buffer.length;
        byteBuffer = null;

        if (buffer != byteArray) {
            byteArray = buffer;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final byte[] buffer, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheckWrap(offset, length, buffer.length);
        }

        addressOffset = ARRAY_BASE_OFFSET + offset;
        capacity = length;
        byteBuffer = null;

        if (buffer != byteArray) {
            byteArray = buffer;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final ByteBuffer buffer) {
        if (buffer != byteBuffer) {
            byteBuffer = buffer;
        }

        if (buffer.isDirect()) {
            byteArray = null;
            addressOffset = address(buffer);
        } else {
            byteArray = array(buffer);
            addressOffset = ARRAY_BASE_OFFSET + arrayOffset(buffer);
        }

        capacity = buffer.capacity();
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final ByteBuffer buffer, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheckWrap(offset, length, buffer.capacity());
        }

        if (buffer != byteBuffer) {
            byteBuffer = buffer;
        }

        if (buffer.isDirect()) {
            byteArray = null;
            addressOffset = address(buffer) + offset;
        } else {
            byteArray = array(buffer);
            addressOffset = ARRAY_BASE_OFFSET + arrayOffset(buffer) + offset;
        }

        capacity = length;
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final DirectBuffer buffer) {
        addressOffset = buffer.addressOffset();
        capacity = buffer.capacity();

        final byte[] byteArray = buffer.byteArray();
        if (byteArray != this.byteArray) {
            this.byteArray = byteArray;
        }

        final ByteBuffer byteBuffer = buffer.byteBuffer();
        if (byteBuffer != this.byteBuffer) {
            this.byteBuffer = byteBuffer;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheckWrap(offset, length, buffer.capacity());
        }

        addressOffset = buffer.addressOffset() + offset;
        capacity = length;

        final byte[] byteArray = buffer.byteArray();
        if (byteArray != this.byteArray) {
            this.byteArray = byteArray;
        }

        final ByteBuffer byteBuffer = buffer.byteBuffer();
        if (byteBuffer != this.byteBuffer) {
            this.byteBuffer = byteBuffer;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void wrap(final long address, final int length) {
        addressOffset = address;
        capacity = length;
        byteArray = null;
        byteBuffer = null;
    }

    public void getLongs(final long index, long[] dst, int offset, int length) {
        long len = (long) length << 3;
        if (len > 6) {
            if (SHOULD_BOUNDS_CHECK) {

                boundsCheck0(index, len);

                lengthCheck(len);

                boundsCheck(index, dst, offset, len);
            }

            long dstOffset = Unsafe.ARRAY_LONG_BASE_OFFSET + ((long) offset << 3);
            try {

                if (ByteOrder.LITTLE_ENDIAN != ByteOrder.nativeOrder())
                    throw new UnsupportedOperationException();
                else

                    UnsafeAccess.UNSAFE.copyMemory(null,
                            this.addressOffset + (index << 3),
                            dst,
                            dstOffset,
                            (long) length << 3);
            } finally {
                Reference.reachabilityFence(this);
            }

        } else {
            get(index, dst, offset, length);
        }


    }

    private void boundsCheck(long index, long[] dst, int offset, long len) {
        final int capacity = dst.length;
        final long resultingPosition = index + len;
        if (index < 0 || resultingPosition > capacity)
        {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + len + " capacity=" + capacity);
        }
    }

    public void get(long index, long[] dst, int offset, int length) {
        long end = offset + length;
        for (int i = offset; i < end; i++)
            dst[i] = getLong(index);
    }

    public void get(long index, float[] dst, int offset, int length) {
        int end = offset + length;
        for (int i = offset; i < end; i++)
            dst[i] = getFloat(index);
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }

        return UNSAFE.getFloat(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putFloat(final long index, final float value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }


        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            final int bits = Integer.reverseBytes(Float.floatToRawIntBits(value));
            UNSAFE.putInt(byteArray, addressOffset + index, bits);
        } else {
            UNSAFE.putFloat(byteArray, addressOffset + index, value);
        }
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = UNSAFE.getLong(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }

        return bits;
    }

    public long getLong(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = UNSAFE.getLong(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }

        return bits;
    }

    private void boundsCheck0(final long index, final long length) {
        int capacity = capacity();
        final long resultingPosition = index + length;
        if (index < 0 || length < 0 || resultingPosition > capacity) {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    public long remaining(long position) {
        long rem = byteBuffer().limit() - position;
        return rem > 0 ? rem : 0;
    }

    public final boolean hasRemaining(long position) {
        return position < byteBuffer().limit();
    }

    public void getFloats(long index, float[] dst, int offset, int length) {
        long len = (long) length << 3;
        if (len > 6) {
            if (SHOULD_BOUNDS_CHECK) {
                boundsCheck0(index, len);

                lengthCheck(len);

                boundsCheck(dst, offset, len);
            }

            long dstOffset = Unsafe.ARRAY_LONG_BASE_OFFSET + ((long) offset << 2);
            try {

                if (ByteOrder.LITTLE_ENDIAN != ByteOrder.nativeOrder())
                    throw new UnsupportedOperationException();
                else

                    UnsafeAccess.UNSAFE.copyMemory(null,
                            this.addressOffset + (index << 2),
                            dst,
                            dstOffset,
                            (long) length << 2);
            } finally {
                Reference.reachabilityFence(this);
            }

        } else {
            get(index, dst, offset, length);
        }

    }

    public static void boundsCheck(final float[] buffer, final long index, final int length)
    {
        final int capacity = buffer.length;
        final long resultingPosition = index + (long)length;
        if (index < 0 || resultingPosition > capacity)
        {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean isExpandable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void verifyAlignment() {
        if (0 != (addressOffset & (ALIGNMENT - 1))) {
            throw new IllegalStateException(
                    "AtomicBuffer is not correctly aligned: addressOffset=" + addressOffset +
                            " is not divisible by " + ALIGNMENT);
        }
    }

    /**
     * {@inheritDoc}
     */
    public long addressOffset() {
        return addressOffset;
    }

    /**
     * {@inheritDoc}
     */
    public byte[] byteArray() {
        return byteArray;
    }

    /**
     * {@inheritDoc}
     */
    public ByteBuffer byteBuffer() {
        return byteBuffer;
    }

    /**
     * {@inheritDoc}
     */
    public void setMemory(final int index, final int length, final byte value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        final long indexOffset = addressOffset + index;
        if (0 == (indexOffset & 1) && length > 64) {
            // This horrible filth is to encourage the JVM to call memset() when address is even.
            // TODO: check if this still applies for versions beyond Java 11.
            UNSAFE.putByte(byteArray, indexOffset, value);
            UNSAFE.setMemory(byteArray, indexOffset + 1, length - 1, value);
        } else {
            UNSAFE.setMemory(byteArray, indexOffset, length, value);
        }
    }

    public void setMemory(final long index, final int length, final byte value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        final long indexOffset = addressOffset + index;
        if (0 == (indexOffset & 1) && length > 64) {
            // This horrible filth is to encourage the JVM to call memset() when address is even.
            // TODO: check if this still applies for versions beyond Java 11.
            UNSAFE.putByte(byteArray, indexOffset, value);
            UNSAFE.setMemory(byteArray, indexOffset + 1, length - 1, value);
        } else {
            UNSAFE.setMemory(byteArray, indexOffset, length, value);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void putLong(final int index, final long value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLong(byteArray, addressOffset + index, bits);
    }


    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public int capacity() {
        return (int) capacity;
    }

    public long capacityLong() {
        return capacity;
    }

    /**
     * {@inheritDoc}
     */
    public void checkLimit(final int limit) {
        if (limit > capacity) {
            throw new IndexOutOfBoundsException("limit=" + limit + " is beyond capacity=" + capacity);
        }
    }

    public void checkLimit(final long limit) {
        if (limit > capacity) {
            throw new IndexOutOfBoundsException("limit=" + limit + " is beyond capacity=" + capacity);
        }
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = UNSAFE.getLong(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Long.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putLong(final long index, final long value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLong(byteArray, addressOffset + index, bits);
    }

    /**
     * {@inheritDoc}
     */
    public void putLong(final int index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLong(byteArray, addressOffset + index, bits);
    }

    public void putLong(final long index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLong(byteArray, addressOffset + index, bits);
    }

    public long getLongVolatile(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = UNSAFE.getLongVolatile(byteArray, addressOffset + index);

        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }
        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public long getLongVolatile(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        long bits = UNSAFE.getLongVolatile(byteArray, addressOffset + index);

        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }
        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putLongVolatile(final int index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }
        long bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }

        UNSAFE.putLongVolatile(byteArray, addressOffset + index, bits);
    }

    public void putLongOrdered(final long index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }
        long bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }
        UNSAFE.putOrderedLong(byteArray, addressOffset + index, bits);
    }

    public void putLongOrdered(final int index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }
        long bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Long.reverseBytes(bits);
        }
        UNSAFE.putOrderedLong(byteArray, addressOffset + index, bits);
    }

    /**
     * {@inheritDoc}
     */
    public long addLongOrdered(final int index, final long increment) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        final long offset = addressOffset + index;
        final byte[] byteArray = this.byteArray;
        final long value = UNSAFE.getLong(byteArray, offset);
        UNSAFE.putOrderedLong(byteArray, offset, value + increment);

        return value;
    }

    public long addLongOrdered(final long index, final long increment) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }


    final long offset = addressOffset + index;
    final byte[] byteArray = this.byteArray;
    final long value = UNSAFE.getLong(byteArray, offset);
        UNSAFE.putOrderedLong(byteArray, offset, value + increment);

        return value;
}

    /**
     * {@inheritDoc}
     */
    public boolean compareAndSetLong(final int index, final long expectedValue, final long updateValue) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }


        return UNSAFE.compareAndSwapLong(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    public boolean compareAndSetLong(final long index, final long expectedValue, final long updateValue) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }


        return UNSAFE.compareAndSwapLong(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public long getAndSetLong(final int index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        return UNSAFE.getAndSetLong(byteArray, addressOffset + index, value);
    }

    public long getAndSetLong(final long index, final long value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        return UNSAFE.getAndSetLong(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public long getAndAddLong(final int index, final long delta) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_LONG);
        }

        return UNSAFE.getAndAddLong(byteArray, addressOffset + index, delta);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    public int getInt(final long index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }


    /**
     * {@inheritDoc}
     */
    public void putInt(final int index, final int value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
    }

    public void putInt(final long index, final int value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    public int getInt(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putInt(final int index, final int value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
    }

    public void putInt(final long index, final int value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        int bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
    }
    /**
     * {@inheritDoc}
     */
    public int getIntVolatile(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        return UNSAFE.getIntVolatile(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putIntVolatile(final int index, final int value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        UNSAFE.putIntVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public void putIntOrdered(final int index, final int value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        UNSAFE.putOrderedInt(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public int addIntOrdered(final int index, final int increment) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        final long offset = addressOffset + index;
        final byte[] byteArray = this.byteArray;
        final int value = UNSAFE.getInt(byteArray, offset);
        UNSAFE.putOrderedInt(byteArray, offset, value + increment);

        return value;
    }

    /**
     * {@inheritDoc}
     */
    public boolean compareAndSetInt(final int index, final int expectedValue, final int updateValue) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        return UNSAFE.compareAndSwapInt(byteArray, addressOffset + index, expectedValue, updateValue);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public int getAndSetInt(final int index, final int value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        return UNSAFE.getAndSetInt(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public int getAndAddInt(final int index, final int delta) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_INT);
        }

        return UNSAFE.getAndAddInt(byteArray, addressOffset + index, delta);
    }

    /**
     * {@inheritDoc}
     */
    public double getDouble(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_DOUBLE);
        }

        if (NATIVE_BYTE_ORDER != byteOrder) {
            final long bits = UNSAFE.getLong(byteArray, addressOffset + index);
            return Double.longBitsToDouble(Long.reverseBytes(bits));
        } else {
            return UNSAFE.getDouble(byteArray, addressOffset + index);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void putDouble(final int index, final double value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_DOUBLE);
        }

        if (NATIVE_BYTE_ORDER != byteOrder) {
            final long bits = Long.reverseBytes(Double.doubleToRawLongBits(value));
            UNSAFE.putLong(byteArray, addressOffset + index, bits);
        } else {
            UNSAFE.putDouble(byteArray, addressOffset + index, value);
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public double getDouble(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_DOUBLE);
        }

        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            final long bits = UNSAFE.getLong(byteArray, addressOffset + index);
            return Double.longBitsToDouble(Long.reverseBytes(bits));
        } else {
            return UNSAFE.getDouble(byteArray, addressOffset + index);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void putDouble(final int index, final double value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_DOUBLE);
        }

        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            final long bits = Long.reverseBytes(Double.doubleToRawLongBits(value));
            UNSAFE.putLong(byteArray, addressOffset + index, bits);
        } else {
            UNSAFE.putDouble(byteArray, addressOffset + index, value);
        }
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }

        if (NATIVE_BYTE_ORDER != byteOrder) {
            final int bits = UNSAFE.getInt(byteArray, addressOffset + index);
            return Float.intBitsToFloat(Integer.reverseBytes(bits));
        } else {
            return UNSAFE.getFloat(byteArray, addressOffset + index);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void putFloat(final int index, final float value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }

        if (NATIVE_BYTE_ORDER != byteOrder) {
            final int bits = Integer.reverseBytes(Float.floatToRawIntBits(value));
            UNSAFE.putInt(byteArray, addressOffset + index, bits);
        } else {
            UNSAFE.putFloat(byteArray, addressOffset + index, value);
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public float getFloat(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }

        return UNSAFE.getFloat(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putFloat(final int index, final float value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_FLOAT);
        }

        UNSAFE.putFloat(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        short bits = UNSAFE.getShort(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Short.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putShort(final int index, final short value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        short bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Short.reverseBytes(bits);
        }

        UNSAFE.putShort(byteArray, addressOffset + index, bits);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        short bits = UNSAFE.getShort(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Short.reverseBytes(bits);
        }

        return bits;
    }

    public short getShort(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        short bits = UNSAFE.getShort(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Short.reverseBytes(bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putShort(final int index, final short value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        short bits = value;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Short.reverseBytes(bits);
        }

        UNSAFE.putShort(byteArray, addressOffset + index, bits);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public short getShortVolatile(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        return UNSAFE.getShortVolatile(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putShortVolatile(final int index, final short value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_SHORT);
        }

        UNSAFE.putShortVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByte(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        return UNSAFE.getByte(byteArray, addressOffset + index);
    }

    public byte getByte(final long index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        return UNSAFE.getByte(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putByte(final int index, final byte value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        UNSAFE.putByte(byteArray, addressOffset + index, value);
    }

    public void putByte(final long index, final byte value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        UNSAFE.putByte(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByteVolatile(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        return UNSAFE.getByteVolatile(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putByteVolatile(final int index, final byte value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck(index);
        }

        UNSAFE.putByteVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public void getBytes(final int index, final byte[] dst) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, dst.length);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET, dst.length);
    }

    /**
     * {@inheritDoc}
     */
    public void getBytes(final int index, final byte[] dst, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(dst, offset, length);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET + offset, length);
    }

    public void getBytes(final long index, final byte[] dst, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(dst, offset, length);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET + offset, length);
    }

//    public static void boundsCheck(final byte[] buffer, final long index, final long length)
//    {
//        final int capacity = buffer.length;
//        final long resultingPosition = index + (long)length;
//        if (index < 0 || resultingPosition > capacity)
//        {
//            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
//        }
//    }

    public static void boundsCheck(final byte[] buffer, final long index, final long length)
    {
        final int capacity = buffer.length;
        final long resultingPosition = index + (long)length;
        if (index < 0 || resultingPosition > capacity)
        {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    public static void boundsCheck(final float[] buffer, final long index, final long length)
    {
        final int capacity = buffer.length;
        final long resultingPosition = index + length;
        if (index < 0 || resultingPosition > capacity)
        {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    public static void boundsCheck(final BulkUnsafeBuffer buffer, final long index, final long length)
    {
        final long capacity = buffer.capacity;
        final long resultingPosition = index + length;
        if (index < 0 || resultingPosition > capacity)
        {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void getBytes(final int index, final MutableDirectBuffer dstBuffer, final int dstIndex, final int length) {
        dstBuffer.putBytes(dstIndex, this, index, length);
    }

    /**
     * {@inheritDoc}
     */
    public void getBytes(final int index, final ByteBuffer dstBuffer, final int length) {
        final int dstOffset = dstBuffer.position();
        getBytes(index, dstBuffer, dstOffset, length);
        dstBuffer.position(dstOffset + length);
    }

    public void getBytes(final long index, final ByteBuffer dstBuffer, final int length) {
        final int dstOffset = dstBuffer.position();
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(dstBuffer, dstOffset, length);
        }

        final byte[] dstByteArray;
        final long dstBaseOffset;
        if (dstBuffer.isDirect()) {
            dstByteArray = null;
            dstBaseOffset = address(dstBuffer);
        } else {
            dstByteArray = array(dstBuffer);
            dstBaseOffset = ARRAY_BASE_OFFSET + arrayOffset(dstBuffer);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dstByteArray, dstBaseOffset + dstOffset, length);
        dstBuffer.position(dstOffset + length);
    }

    /**
     * {@inheritDoc}
     */
    public void getBytes(final int index, final ByteBuffer dstBuffer, final int dstOffset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(dstBuffer, dstOffset, length);
        }

        final byte[] dstByteArray;
        final long dstBaseOffset;
        if (dstBuffer.isDirect()) {
            dstByteArray = null;
            dstBaseOffset = address(dstBuffer);
        } else {
            dstByteArray = array(dstBuffer);
            dstBaseOffset = ARRAY_BASE_OFFSET + arrayOffset(dstBuffer);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dstByteArray, dstBaseOffset + dstOffset, length);
    }

    /**
     * {@inheritDoc}
     */
    public void putBytes(final int index, final byte[] src) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, src.length);
        }

        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET, byteArray, addressOffset + index, src.length);
    }

    /**
     * {@inheritDoc}
     */
    public void putBytes(final int index, final byte[] src, final int offset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(src, offset, length);
        }

        UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray, addressOffset + index, length);
    }

    /**
     * {@inheritDoc}
     */
    public void putBytes(final int index, final ByteBuffer srcBuffer, final int length) {
        final int srcIndex = srcBuffer.position();
        putBytes(index, srcBuffer, srcIndex, length);
        srcBuffer.position(srcIndex + length);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public void putBytes(final int index, final ByteBuffer srcBuffer, final int srcIndex, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(srcBuffer, srcIndex, length);
        }

        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (srcBuffer.isDirect()) {
            srcByteArray = null;
            srcBaseOffset = address(srcBuffer);
        } else {
            srcByteArray = array(srcBuffer);
            srcBaseOffset = ARRAY_BASE_OFFSET + arrayOffset(srcBuffer);
        }

        UNSAFE.copyMemory(srcByteArray, srcBaseOffset + srcIndex, byteArray, addressOffset + index, length);
    }

    public void putBytes(final long index, final ByteBuffer srcBuffer, final int srcIndex, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(srcBuffer, srcIndex, length);
        }

        final byte[] srcByteArray;
        final long srcBaseOffset;
        if (srcBuffer.isDirect()) {
            srcByteArray = null;
            srcBaseOffset = address(srcBuffer);
        } else {
            srcByteArray = array(srcBuffer);
            srcBaseOffset = ARRAY_BASE_OFFSET + arrayOffset(srcBuffer);
        }

        UNSAFE.copyMemory(srcByteArray, srcBaseOffset + srcIndex, byteArray, addressOffset + index, length);
    }

    /**
     * {@inheritDoc}
     */
    public void putBytes(final int index, final DirectBuffer srcBuffer, final int srcIndex, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            srcBuffer.boundsCheck(srcIndex, length);
        }

        UNSAFE.copyMemory(
                srcBuffer.byteArray(),
                srcBuffer.addressOffset() + srcIndex,
                byteArray,
                addressOffset + index,
                length);
    }

    /**
     * {@inheritDoc}
     */
    public char getChar(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        char bits = UNSAFE.getChar(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = (char) Short.reverseBytes((short) bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putChar(final int index, final char value, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        char bits = value;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = (char) Short.reverseBytes((short) bits);
        }

        UNSAFE.putChar(byteArray, addressOffset + index, bits);
    }

    /**
     * {@inheritDoc}
     */
    public char getChar(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        char bits = UNSAFE.getChar(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = (char) Short.reverseBytes((short) bits);
        }

        return bits;
    }

    /**
     * {@inheritDoc}
     */
    public void putChar(final int index, final char value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        UNSAFE.putChar(byteArray, addressOffset + index, value);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public char getCharVolatile(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        return UNSAFE.getCharVolatile(byteArray, addressOffset + index);
    }

    /**
     * {@inheritDoc}
     */
    public void putCharVolatile(final int index, final char value) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, SIZE_OF_CHAR);
        }

        UNSAFE.putCharVolatile(byteArray, addressOffset + index, value);
    }

    /**
     * {@inheritDoc}
     */
    public String getStringAscii(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        final int length = bits;

        return getStringAscii(index, length);
    }

    /**
     * {@inheritDoc}
     */
    public int getStringAscii(final int index, final Appendable appendable) {
        boundsCheck0(index, STR_HEADER_LEN);

        final int length = UNSAFE.getInt(byteArray, addressOffset + index);

        return getStringAscii(index, length, appendable);
    }

    /**
     * {@inheritDoc}
     */
    public String getStringAscii(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        final int length = bits;

        return getStringAscii(index, length);
    }

    /**
     * {@inheritDoc}
     */
    public int getStringAscii(final int index, final Appendable appendable, final ByteOrder byteOrder) {
        boundsCheck0(index, STR_HEADER_LEN);

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        final int length = bits;

        return getStringAscii(index, length, appendable);
    }

    /**
     * {@inheritDoc}
     */
    public String getStringAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index + STR_HEADER_LEN, length);
        }

        final byte[] dst = new byte[length];
        UNSAFE.copyMemory(byteArray, addressOffset + index + STR_HEADER_LEN, dst, ARRAY_BASE_OFFSET, length);

        return new String(dst, US_ASCII);
    }

    /**
     * {@inheritDoc}
     */
    public int getStringAscii(final int index, final int length, final Appendable appendable) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length + STR_HEADER_LEN);
        }

        try {
            for (int i = index + STR_HEADER_LEN, limit = index + STR_HEADER_LEN + length; i < limit; i++) {
                final char c = (char) UNSAFE.getByte(byteArray, addressOffset + i);
                appendable.append(c > 127 ? '?' : c);
            }
        } catch (final IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    public int putStringAscii(final int index, final String value) {
        final int length = value != null ? value.length() : 0;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length + STR_HEADER_LEN);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, length);

        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c > 127) {
                c = '?';
            }

            UNSAFE.putByte(byteArray, addressOffset + STR_HEADER_LEN + index + i, (byte) c);
        }

        return STR_HEADER_LEN + length;
    }

    /**
     * {@inheritDoc}
     */
    public int putStringAscii(final int index, final String value, final ByteOrder byteOrder) {
        final int length = value != null ? value.length() : 0;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length + STR_HEADER_LEN);
        }

        int bits = length;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);

        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c > 127) {
                c = '?';
            }

            UNSAFE.putByte(byteArray, addressOffset + STR_HEADER_LEN + index + i, (byte) c);
        }

        return STR_HEADER_LEN + length;
    }

    /**
     * {@inheritDoc}
     */
    public String getStringWithoutLengthAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        final byte[] dst = new byte[length];
        UNSAFE.copyMemory(byteArray, addressOffset + index, dst, ARRAY_BASE_OFFSET, length);

        return new String(dst, US_ASCII);
    }

    /**
     * {@inheritDoc}
     */
    public int getStringWithoutLengthAscii(final int index, final int length, final Appendable appendable) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        try {
            for (int i = index, limit = index + length; i < limit; i++) {
                final char c = (char) UNSAFE.getByte(byteArray, addressOffset + i);
                appendable.append(c > 127 ? '?' : c);
            }
        } catch (final IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }

        return length;
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public int putStringWithoutLengthAscii(final int index, final String value) {
        final int length = value != null ? value.length() : 0;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if (c > 127) {
                c = '?';
            }

            UNSAFE.putByte(byteArray, addressOffset + index + i, (byte) c);
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    public int putStringWithoutLengthAscii(final int index, final String value, final int valueOffset, final int length) {
        final int len = value != null ? Math.min(value.length() - valueOffset, length) : 0;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, len);
        }

        for (int i = 0; i < len; i++) {
            char c = value.charAt(valueOffset + i);
            if (c > 127) {
                c = '?';
            }

            UNSAFE.putByte(byteArray, addressOffset + index + i, (byte) c);
        }

        return len;
    }

    /**
     * {@inheritDoc}
     */
    public String getStringUtf8(final int index) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        final int length = bits;

        return getStringUtf8(index, length);
    }

    /**
     * {@inheritDoc}
     */
    public String getStringUtf8(final int index, final ByteOrder byteOrder) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN);
        }

        int bits = UNSAFE.getInt(byteArray, addressOffset + index);
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        final int length = bits;

        return getStringUtf8(index, length);
    }

    /**
     * {@inheritDoc}
     */
    public String getStringUtf8(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index + STR_HEADER_LEN, length);
        }

        final byte[] stringInBytes = new byte[length];
        UNSAFE.copyMemory(byteArray, addressOffset + index + STR_HEADER_LEN, stringInBytes, ARRAY_BASE_OFFSET, length);

        return new String(stringInBytes, UTF_8);
    }

    /**
     * {@inheritDoc}
     */
    public int putStringUtf8(final int index, final String value) {
        return putStringUtf8(index, value, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    public int putStringUtf8(final int index, final String value, final ByteOrder byteOrder) {
        return putStringUtf8(index, value, byteOrder, Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    public int putStringUtf8(final int index, final String value, final int maxEncodedLength) {
        final byte[] bytes = value != null ? value.getBytes(UTF_8) : NULL_BYTES;
        if (bytes.length > maxEncodedLength) {
            throw new IllegalArgumentException("Encoded string larger than maximum size: " + maxEncodedLength);
        }

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN + bytes.length);
        }

        int bits = bytes.length;
        if (NATIVE_BYTE_ORDER != ByteOrder.LITTLE_ENDIAN) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
        UNSAFE.copyMemory(bytes, ARRAY_BASE_OFFSET, byteArray, addressOffset + index + STR_HEADER_LEN, bytes.length);

        return STR_HEADER_LEN + bytes.length;
    }

    /**
     * {@inheritDoc}
     */
    public int putStringUtf8(final int index, final String value, final ByteOrder byteOrder, final int maxEncodedLength) {
        final byte[] bytes = value != null ? value.getBytes(UTF_8) : NULL_BYTES;
        if (bytes.length > maxEncodedLength) {
            throw new IllegalArgumentException("Encoded string larger than maximum size: " + maxEncodedLength);
        }

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, STR_HEADER_LEN + bytes.length);
        }

        int bits = bytes.length;
        if (NATIVE_BYTE_ORDER != byteOrder) {
            bits = Integer.reverseBytes(bits);
        }

        UNSAFE.putInt(byteArray, addressOffset + index, bits);
        UNSAFE.copyMemory(bytes, ARRAY_BASE_OFFSET, byteArray, addressOffset + index + STR_HEADER_LEN, bytes.length);

        return STR_HEADER_LEN + bytes.length;
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public String getStringWithoutLengthUtf8(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        final byte[] stringInBytes = new byte[length];
        UNSAFE.copyMemory(byteArray, addressOffset + index, stringInBytes, ARRAY_BASE_OFFSET, length);

        return new String(stringInBytes, UTF_8);
    }

    /**
     * {@inheritDoc}
     */
    public int putStringWithoutLengthUtf8(final int index, final String value) {
        final byte[] bytes = value != null ? value.getBytes(UTF_8) : NULL_BYTES;
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, bytes.length);
        }

        UNSAFE.copyMemory(bytes, ARRAY_BASE_OFFSET, byteArray, addressOffset + index, bytes.length);

        return bytes.length;
    }

    /**
     * {@inheritDoc}
     */
    public int parseNaturalIntAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        if (length <= 0) {
            throw new AsciiNumberFormatException("empty string: index=" + index + " length=" + length);
        }

        final int end = index + length;
        int tally = 0;
        for (int i = index; i < end; i++) {
            tally = (tally * 10) + AsciiEncoding.getDigit(i, UNSAFE.getByte(byteArray, addressOffset + i));
        }

        return tally;
    }

    /**
     * {@inheritDoc}
     */
    public long parseNaturalLongAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        if (length <= 0) {
            throw new AsciiNumberFormatException("empty string: index=" + index + " length=" + length);
        }

        final int end = index + length;
        long tally = 0;
        for (int i = index; i < end; i++) {
            tally = (tally * 10) + AsciiEncoding.getDigit(i, UNSAFE.getByte(byteArray, addressOffset + i));
        }

        return tally;
    }

    /**
     * {@inheritDoc}
     */
    public int parseIntAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        if (length <= 0) {
            throw new AsciiNumberFormatException("empty string: index=" + index + " length=" + length);
        } else if (1 == length) {
            return AsciiEncoding.getDigit(index, UNSAFE.getByte(byteArray, addressOffset + index));
        }

        final int endExclusive = index + length;
        final int first = UNSAFE.getByte(byteArray, addressOffset + index);
        int i = index;

        if (first == MINUS_SIGN) {
            i++;
        }

        int tally = 0;
        for (; i < endExclusive; i++) {
            tally = (tally * 10) + AsciiEncoding.getDigit(i, UNSAFE.getByte(byteArray, addressOffset + i));
        }

        if (first == MINUS_SIGN) {
            tally = -tally;
        }

        return tally;
    }

    /**
     * {@inheritDoc}
     */
    public long parseLongAscii(final int index, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        if (length <= 0) {
            throw new AsciiNumberFormatException("empty string: index=" + index + " length=" + length);
        } else if (1 == length) {
            return AsciiEncoding.getDigit(index, UNSAFE.getByte(byteArray, addressOffset + index));
        }

        final int endExclusive = index + length;
        final int first = UNSAFE.getByte(byteArray, addressOffset + index);
        int i = index;

        if (first == MINUS_SIGN) {
            i++;
        }

        long tally = 0;
        for (; i < endExclusive; i++) {
            tally = (tally * 10) + AsciiEncoding.getDigit(i, UNSAFE.getByte(byteArray, addressOffset + i));
        }

        if (first == MINUS_SIGN) {
            tally = -tally;
        }

        return tally;
    }

    /**
     * {@inheritDoc}
     */
    public int putIntAscii(final int index, final int value) {
        if (value == 0) {
            putByte(index, ZERO);
            return 1;
        }

        if (value == Integer.MIN_VALUE) {
            putBytes(index, MIN_INTEGER_VALUE);
            return MIN_INTEGER_VALUE.length;
        }

        int start = index;
        int quotient = value;
        int length = 1;
        if (value < 0) {
            putByte(index, MINUS_SIGN);
            start++;
            length++;
            quotient = -quotient;
        }

        int i = endOffset(quotient);
        length += i;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        while (i >= 0) {
            final int remainder = quotient % 10;
            quotient = quotient / 10;
            UNSAFE.putByte(byteArray, addressOffset + i + start, (byte) (ZERO + remainder));
            i--;
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    public int putNaturalIntAscii(final int index, final int value) {
        if (value == 0) {
            putByte(index, ZERO);
            return 1;
        }

        int i = endOffset(value);
        final int length = i + 1;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        int quotient = value;
        while (i >= 0) {
            final int remainder = quotient % 10;
            quotient = quotient / 10;
            UNSAFE.putByte(byteArray, addressOffset + i + index, (byte) (ZERO + remainder));
            i--;
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    public void putNaturalPaddedIntAscii(final int offset, final int length, final int value) {
        final int end = offset + length;
        int remainder = value;
        for (int index = end - 1; index >= offset; index--) {
            final int digit = remainder % 10;
            remainder = remainder / 10;
            putByte(index, (byte) (ZERO + digit));
        }

        if (remainder != 0) {
            throw new NumberFormatException("Cannot write " + value + " in " + length + " bytes");
        }
    }

    /**
     * {@inheritDoc}
     */
    public int putNaturalIntAsciiFromEnd(final int value, final int endExclusive) {
        int remainder = value;
        int index = endExclusive;
        while (remainder > 0) {
            index--;
            final int digit = remainder % 10;
            remainder = remainder / 10;
            putByte(index, (byte) (ZERO + digit));
        }

        return index;
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public int putNaturalLongAscii(final int index, final long value) {
        if (value == 0) {
            putByte(index, ZERO);
            return 1;
        }

        int i = endOffset(value);
        final int length = i + 1;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        long quotient = value;
        while (i >= 0) {
            final long remainder = quotient % 10;
            quotient = quotient / 10;
            UNSAFE.putByte(byteArray, addressOffset + i + index, (byte) (ZERO + remainder));
            i--;
        }

        return length;
    }

    /**
     * {@inheritDoc}
     */
    public int putLongAscii(final int index, final long value) {
        if (value == 0) {
            putByte(index, ZERO);
            return 1;
        }

        if (value == Long.MIN_VALUE) {
            putBytes(index, MIN_LONG_VALUE);
            return MIN_LONG_VALUE.length;
        }

        int start = index;
        long quotient = value;
        int length = 1;
        if (value < 0) {
            putByte(index, MINUS_SIGN);
            start++;
            length++;
            quotient = -quotient;
        }

        int i = endOffset(quotient);
        length += i;

        if (SHOULD_BOUNDS_CHECK) {
            boundsCheck0(index, length);
        }

        while (i >= 0) {
            final long remainder = quotient % 10L;
            quotient = quotient / 10L;
            UNSAFE.putByte(byteArray, addressOffset + i + start, (byte) (ZERO + remainder));
            i--;
        }

        return length;
    }

    private void boundsCheck(final int index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index=" + index + " capacity=" + capacity);
        }
    }

    private void boundsCheck(final long index) {
        if (index < 0 || index >= capacity) {
            throw new IndexOutOfBoundsException("index=" + index + " capacity=" + capacity);
        }
    }

    private void boundsCheck0(final int index, final int length) {
        final long resultingPosition = index + (long) length;
        if (index < 0 || length < 0 || resultingPosition > capacity) {
            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void boundsCheck(final int index, final int length) {
        boundsCheck0(index, length);
    }

    /**
     * {@inheritDoc}
     */
    public int wrapAdjustment() {
        final long offset = byteArray != null ? ARRAY_BASE_OFFSET : BufferUtil.address(byteBuffer);

        return (int) (addressOffset - offset);
    }

    ///////////////////////////////////////////////////////////////////////////

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        final BulkUnsafeBuffer that = (BulkUnsafeBuffer) obj;

        if (capacity != that.capacity) {
            return false;
        }

        final byte[] thisByteArray = this.byteArray;
        final byte[] thatByteArray = that.byteArray;
        final long thisOffset = this.addressOffset;
        final long thatOffset = that.addressOffset;

        for (long i = 0, length = capacity; i < length; i++) {
            if (UNSAFE.getByte(thisByteArray, thisOffset + i) != UNSAFE.getByte(thatByteArray, thatOffset + i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        int hashCode = 1;

        final byte[] byteArray = this.byteArray;
        final long addressOffset = this.addressOffset;
        for (long i = 0, length = capacity; i < length; i++) {
            hashCode = 31 * hashCode + UNSAFE.getByte(byteArray, addressOffset + i);
        }

        return hashCode;
    }

    /**
     * {@inheritDoc}
     */
    public int compareTo(final DirectBuffer that) {
        final long thisCapacity = this.capacity;
        final int thatCapacity = that.capacity();
        final byte[] thisByteArray = this.byteArray;
        final byte[] thatByteArray = that.byteArray();
        final long thisOffset = this.addressOffset;
        final long thatOffset = that.addressOffset();

        for (long i = 0, length = Math.min(thisCapacity, thatCapacity); i < length; i++) {
            final int cmp = Byte.compare(
                    UNSAFE.getByte(thisByteArray, thisOffset + i),
                    UNSAFE.getByte(thatByteArray, thatOffset + i));

            if (0 != cmp) {
                return cmp;
            }
        }

        if (thisCapacity != thatCapacity) {
            return (int) (thisCapacity - thatCapacity);
        }

        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return "UnsafeBuffer{" +
                "addressOffset=" + addressOffset +
                ", capacity=" + capacity +
                ", byteArray=" + byteArray + // lgtm [java/print-array]
                ", byteBuffer=" + byteBuffer +
                '}';
    }

    public void capacity(long sliceEnd) {
        this.capacity = sliceEnd;
    }

    public void getBytes(final long index, final ByteBuffer dstBuffer, final int dstOffset, final int length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            BufferUtil.boundsCheck(dstBuffer, dstOffset, length);
        }

        final byte[] dstByteArray;
        final long dstBaseOffset;
        if (dstBuffer.isDirect()) {
            dstByteArray = null;
            dstBaseOffset = address(dstBuffer);
        } else {
            dstByteArray = array(dstBuffer);
            dstBaseOffset = ARRAY_BASE_OFFSET + arrayOffset(dstBuffer);
        }

        UNSAFE.copyMemory(byteArray, addressOffset + index, dstByteArray, dstBaseOffset + dstOffset, length);
    }

    public void putBytes(long index, BulkUnsafeBuffer dstBuffer, long dstOffset, long length) {
        if (SHOULD_BOUNDS_CHECK) {
            lengthCheck(length);
            boundsCheck0(index, length);
            boundsCheck(dstBuffer, dstOffset, length);
        }

         //   dstByteArray = null;
          //  dstBaseOffset = address(dstBuffer);


        UNSAFE.copyMemory(byteArray, addressOffset + index, dstBuffer.addressOffset, addressOffset + dstOffset, length);
    }

//    public static void boundsCheck(final byte[] buffer, final long index, final long length)
//    {
//        final int capacity = buffer.length;
//        final long resultingPosition = index + (long)length;
//        if (index < 0 || resultingPosition > capacity)
//        {
//            throw new IndexOutOfBoundsException("index=" + index + " length=" + length + " capacity=" + capacity);
//        }
//    }


//    public void limit(long limit) {
//        this.l
//    }
}
