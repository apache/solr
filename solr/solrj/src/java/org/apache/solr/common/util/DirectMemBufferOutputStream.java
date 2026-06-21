/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MappedResizeableBuffer;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@link OutputStream} that wraps an underlying {@link MutableDirectBuffer}.
 */
public class DirectMemBufferOutputStream extends OutputStream {
  private MappedResizeableBuffer buffer;

  private volatile long position;
  private int offset;

  /**
   * Constructs output stream wrapping the given buffer at an offset.
   *
   * @param buffer to wrap.
   */
  public DirectMemBufferOutputStream(final MappedResizeableBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * The position in the buffer from the offset up to which has been written.
   *
   * @return the position in the buffer from the offset up to which has been written.
   */
  public long position() {
    return position;
  }

  public void position(int pos) {
    this.position = pos;
  }

  /**
   * The underlying buffer being wrapped.
   *
   * @return the underlying buffer being wrapped.
   */
  public MappedResizeableBuffer buffer() {
    return buffer;
  }

  /**
   * The length of the underlying buffer to use
   *
   * @return length of the underlying buffer to use
   */
  public long length() {
    return position;
  }

  /**
   * Write a byte to buffer.
   *
   * @param b to be written.
   * @throws IllegalStateException if insufficient capacity remains in the buffer.
   */
  public void write(final int b) {

    if (position == buffer.capacity()) {
      throw new IllegalStateException("position has reached the end of underlying buffer");
    }

    buffer.putByte(offset + position, (byte) b);
    ++position;
  }

  /**
   * Write a byte[] to the buffer.
   *
   * @param srcBytes  to write
   * @param srcOffset at which to begin reading bytes from the srcBytes.
   * @param length    of the srcBytes to read.
   * @throws IllegalStateException if insufficient capacity remains in the buffer.
   */
  public void write(final byte[] srcBytes, final int srcOffset, final int length) {
    final long resultingOffset = position + ((long) length);
    if (resultingOffset > this.buffer.capacity()) {
      throw new IllegalStateException("insufficient capacity in the buffer");
    }

    buffer.putBytes(offset + position, srcBytes, srcOffset, length);
    position += length;
  }

  /**
   * Write a byte[] to the buffer.
   *
   * @param srcBytes to write
   * @throws IllegalStateException if insufficient capacity remains in the buffer.
   */
  public void write(final byte[] srcBytes) {
    write(srcBytes, 0, srcBytes.length);
  }

  /**
   * Override to remove {@link IOException}. This method does nothing.
   */
  public void flush() {
  }

  /**
   * Override to remove {@link IOException}. This method does nothing.
   */
  public void close() {
    IOUtils.closeQuietly(buffer);
  }

  public void writeInt(int val) {
    buffer.putInt(position, val);
  }

  public long size() {
    return position;
  }

  public void putBytes(long pos, ByteBuffer byteBuffer, int length) {
    buffer.putBytes(pos, byteBuffer, length);
    //  position += lastAddSize;
  }

  public void putBytes(long pos, byte[] bytes, int lastAddSize) {
    buffer.putBytes(pos, bytes, 0, lastAddSize);
    //  position += lastAddSize;
  }

  public void putInt(int position, int val) {
    buffer.putInt(position, val);
  }

  public void putInt(int val) {
    buffer.putInt(position, val, ByteOrder.LITTLE_ENDIAN);
    this.position += BitUtil.SIZE_OF_INT;
  }

  public void putLong(long val) {
    buffer.putLong(position, val, ByteOrder.LITTLE_ENDIAN);
    this.position += BitUtil.SIZE_OF_LONG;
  }

  public void putFloat(float val) {
    buffer.putFloat(position, val, ByteOrder.LITTLE_ENDIAN);
    this.position += BitUtil.SIZE_OF_FLOAT;
  }

  public void putShort(short val) {
    buffer.putShort(position, val, ByteOrder.LITTLE_ENDIAN);
    this.position += BitUtil.SIZE_OF_SHORT;
  }
}
