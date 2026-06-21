/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.filestore;

import org.agrona.BitUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.solr.common.AlreadyClosedException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;

/**
 * Base IndexInput implementation that uses an array of ByteBuffers to represent a file.
 *
 * <p>Because Java's ByteBuffer uses an int to address the values, it's necessary to access a file
 * greater Integer.MAX_VALUE in size using multiple byte buffers.
 *
 * <p>For efficiency, this class requires that the buffers are a power-of-two (<code>chunkSizePower
 * </code>).
 */
public abstract class ByteBufferIndexInput extends IndexInput implements RandomAccessInput {

  protected final long length;

  protected final UnsafeByteBufferGuard guard;

  protected long curBufPos = -1;
  protected BulkUnsafeBuffer curBuf; // redundant for speed: buffers[curBufIndex]


  protected boolean isClone = false;

  public static ByteBufferIndexInput newInstance(
          String resourceDescription,
          BulkUnsafeBuffer buffer,
          long length,
          long pos,
          UnsafeByteBufferGuard guard) {

      return new SingleBufferImpl(resourceDescription, buffer, length, pos, guard);

  }

  ByteBufferIndexInput(
          String resourceDescription,
          BulkUnsafeBuffer buffer,
          long length,
          long pos,
          UnsafeByteBufferGuard guard) {
    super(resourceDescription);
    this.length = length;
    this.guard = guard;
    curBufPos = pos;
    curBuf = buffer;
  }

  @Override
  public final byte readByte() throws IOException {
    try {
      byte b = guard.getByte(curBuf, curBufPos);
      curBufPos++;
      return b;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final void readBytes(byte[] b, int offset, int len) throws IOException {
    try {
      guard.getBytes(curBuf, curBufPos, b, offset, len);
      curBufPos += len;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public void readLongs(long[] dst, int offset, int length) throws IOException {

    try {

      guard.getLongs(curBuf, curBufPos, dst, offset, length);
      // if the above call succeeded, then we know the below sum cannot overflow
      curBufPos = (curBufPos + (length << 3));
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final void readFloats(float[] floats, int offset, int len) throws IOException {
    try {

      guard.getFloats(curBuf, curBufPos, floats, offset, len);
      // if the above call succeeded, then we know the below sum cannot overflow
      curBufPos = (curBufPos + (len << 2));
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final short readShort() throws IOException {
    try {
      short s = guard.getShort(curBuf, curBufPos);
      curBufPos += BitUtil.SIZE_OF_SHORT;
      return s;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final int readInt() throws IOException {
    try {
      int i = guard.getInt(curBuf, curBufPos);
      curBufPos += BitUtil.SIZE_OF_INT;
      return i;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final long readLong() throws IOException {
    try {
      long l = guard.getLong(curBuf, curBufPos);
      curBufPos += BitUtil.SIZE_OF_LONG;
      return l;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public long getFilePointer() {
    try {
      return curBufPos;
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    curBufPos = pos;
  }

  @Override
  public byte readByte(long pos) throws IOException {
    try {

      return guard.getByte(curBuf, curBufPos);
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public short readShort(long pos) throws IOException {
    try {
      return guard.getShort(curBuf, pos);
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public int readInt(long pos) throws IOException {

    try {
      return guard.getInt(curBuf, pos);
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public long readLong(long pos) throws IOException {
    try {
      return guard.getLong(curBuf, pos);
    } catch (
            @SuppressWarnings("unused")
                    NullPointerException npe) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public final ByteBufferIndexInput clone() {
    final ByteBufferIndexInput clone = buildSlice((String) null, 0L, this.length);
    try {
      clone.seek(getFilePointer());
    } catch (IOException ioe) {
      throw new AssertionError(ioe);
    }

    return clone;
  }

  /**
   * Creates a slice of this index input, with the given description, offset, and length. The slice
   * is seeked to the beginning.
   */
  @Override
  public final ByteBufferIndexInput slice(String sliceDescription, long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IllegalArgumentException(
              "slice() "
                      + sliceDescription
                      + " out of bounds: offset="
                      + offset
                      + ",length="
                      + length
                      + ",fileLength="
                      + this.length
                      + ": "
                      + this);
    }

    return buildSlice(sliceDescription, offset, length);
  }

  /** Builds the actual sliced IndexInput (may apply extra offset in subclasses). * */
  protected ByteBufferIndexInput buildSlice(String sliceDescription, long offset, long length) {
    if (curBuf == null) {
      throw new AlreadyClosedException("Already closed: " + this);
    }

//    final BulkUnsafeBuffer newBuffer = buildSlice(curBuf, offset, length);

    final ByteBufferIndexInput clone =
            newCloneInstance(getFullSliceDescription(sliceDescription), curBuf, offset, length);
    clone.isClone = true;

    return clone;
  }

  /**
   * Factory method that creates a suitable implementation of this class for the given ByteBuffers.
   */
  @SuppressWarnings("resource")
  protected ByteBufferIndexInput newCloneInstance(
          String newResourceDescription, BulkUnsafeBuffer buffer, long offset, long length) {

    // newBuffers[0].byteBuffer().position(offset);
      BulkUnsafeBuffer newBuffer = new BulkUnsafeBuffer();
      newBuffer.wrap(newBuffer.byteBuffer().slice().order(ByteOrder.LITTLE_ENDIAN));
      return new SingleBufferImpl(
              newResourceDescription,
              newBuffer,
              length,
              offset,
              this.guard);

  }

  /**
   * Returns a sliced view from a set of already-existing buffers: the last buffer's limit() will be
   * correct, but you must deal with offset separately (the first buffer will not be adjusted)
   */
  private BulkUnsafeBuffer buildSlice(BulkUnsafeBuffer buffer, long offset, long length) {
    final long sliceEnd = offset + length;


    // we always allocate one more slice, the last one may be a 0 byte one
   // final BulkUnsafeBuffer slices[ = new BulkUnsafeBuffer[endIndex - startIndex + 1];

      BulkUnsafeBuffer buff = new BulkUnsafeBuffer();
      buffer.wrap(buffer.byteBuffer().duplicate().order(ByteOrder.LITTLE_ENDIAN));

   // }

    // set the last buffer's limit for the sliced view.
    buff.capacity(sliceEnd);

    return buff;
  }

  @Override
  public final void close() throws IOException {
    try {
      if (curBuf == null) return;


      unsetBuffer();

      if (isClone) return;

      // tell the guard to invalidate and later unmap the bytebuffers (if supported):
      guard.invalidateAndUnmap(curBuf);
    } finally {
      unsetBuffer();
    }
  }

  /** Called to remove all references to byte buffers, so we can throw AlreadyClosed on NPE. */
  private void unsetBuffer() {
    curBuf = null;
    curBufPos = 0;
  }

  /** Optimization of ByteBufferIndexInput for when there is only one buffer */
  static final class SingleBufferImpl extends ByteBufferIndexInput {

    SingleBufferImpl(
            String resourceDescription,
            BulkUnsafeBuffer buffer,
            long length,
            long pos,
            UnsafeByteBufferGuard guard) {
      super(resourceDescription, buffer, length, pos, guard);

      this.curBufPos = pos;
      assert buffer.byteBuffer().order() == ByteOrder.LITTLE_ENDIAN;

    }

    // TODO: investigate optimizing readByte() & Co?

    @Override
    public void seek(long pos) throws IOException {
      try {
        curBufPos = ((int) pos);
      } catch (IllegalArgumentException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }

    @Override
    public long getFilePointer() {
      try {
        return curBufPos;
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }

    @Override
    public byte readByte(long pos) throws IOException {
      try {
        return guard.getByte(curBuf, (int) pos);
      } catch (IllegalArgumentException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }

    @Override
    public short readShort(long pos) throws IOException {
      try {
        return guard.getShort(curBuf, (int) pos);
      } catch (IllegalArgumentException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }

    @Override
    public int readInt(long pos) throws IOException {
      try {
        return guard.getInt(curBuf, (int) pos);
      } catch (IllegalArgumentException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }

    @Override
    public long readLong(long pos) throws IOException {
      try {
        return guard.getLong(curBuf, (int) pos);
      } catch (IllegalArgumentException e) {
        if (pos < 0) {
          throw new IllegalArgumentException("Seeking to negative position: " + this, e);
        } else {
          throw new EOFException("seek past EOF: " + this);
        }
      } catch (
              @SuppressWarnings("unused")
                      NullPointerException npe) {
        throw new AlreadyClosedException("Already closed: " + this);
      }
    }
  }

  /** This class adds offset support to ByteBufferIndexInput, which is needed for slices. */
  static final class MultiBufferImpl extends ByteBufferIndexInput {
    private final int offset;

    MultiBufferImpl(
            String resourceDescription,
            BulkUnsafeBuffer buffer,
            int offset,
            long length,
            int chunkSizePower,
            UnsafeByteBufferGuard guard) {
      super(resourceDescription, buffer, length, chunkSizePower, guard);
      this.offset = offset;
      try {
        seek(0L);
      } catch (IOException ioe) {
        throw new AssertionError(ioe);
      }
    }

    @Override
    public void seek(long pos) throws IOException {
      assert pos >= 0L;
      super.seek(pos + offset);
    }

    @Override
    public long getFilePointer() {
      return super.getFilePointer() - offset;
    }

    @Override
    public byte readByte(long pos) throws IOException {
      return super.readByte(pos + offset);
    }

    @Override
    public short readShort(long pos) throws IOException {
      return super.readShort(pos + offset);
    }

    @Override
    public int readInt(long pos) throws IOException {
      return super.readInt(pos + offset);
    }

    @Override
    public long readLong(long pos) throws IOException {
      return super.readLong(pos + offset);
    }

    @Override
    protected ByteBufferIndexInput buildSlice(String sliceDescription, long ofs, long length) {
      return super.buildSlice(sliceDescription, this.offset + ofs, length);
    }
  }
}
