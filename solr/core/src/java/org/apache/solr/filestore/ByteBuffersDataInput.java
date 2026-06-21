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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.stream.Collectors;


public final class ByteBuffersDataInput extends DataInput
    implements Accountable, RandomAccessInput {
  private final BulkUnsafeBuffer buffer;

  private final long size;
//  private final long offset;

  private long pos;

  /**
   * Read data from a set of contiguous buffers. All data buffers except for the last one must have
   * an identical remaining number of bytes in the buffer (that is a power of two). The last buffer
   * can be of an arbitrary remaining length.
   */
  public ByteBuffersDataInput(BulkUnsafeBuffer buff, long offset, long size) {
  //  ensureAssumptions(buffers);

    this.buffer = buff;



    this.size = size;

    // The initial "position" of this stream is shifted by the position of the first block.

    this.pos = offset;
  }

  public long size() {
    return size;
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for what the type of buffer is (direct vs. heap-based).
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF *  buffer.capacity();
  }

  @Override
  public byte readByte() throws EOFException {
    try {

      byte v = buffer.getByte(pos);
      pos++;
      return v;
    } catch (IndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  /**
   * Reads exactly {@code len} bytes into the given buffer. The buffer must have enough remaining
   * limit.
   *
   * <p>If there are fewer than {@code len} bytes in the input, {@link EOFException} is thrown.
   */
  public void readBytes(BulkUnsafeBuffer buffer, int len) throws EOFException {
    try {

        // Update pos early on for EOF detection on output buffer, then try to get buffer content.
        pos += len;
      //  block.byteBuffer().limit(blockOffset + chunk);

        buffer.getBytes(pos, buffer.byteBuffer(), len);


    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public void readBytes(byte[] arr, int off, int len) throws EOFException {
    try {


      //  block.byteBuffer().position(blockOffset(pos));
        long remaining = Math.min(len, this.size - this.pos);
        if (remaining == 0) {
          throw new EOFException();
        }

        // Update pos early on for EOF detection, then try to get buffer content.


        buffer.getBytes(pos, arr, off, len);
        pos += len;



    } catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
      if (pos >= size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  @Override
  public byte readByte(long pos) {
    return buffer.getByte(pos);
  }

  @Override
  public short readShort(long pos) {

 //   if (blockOffset + Short.BYTES <= blockMask) {
      return buffer.getShort(pos);
 //   } else {
 //     return (short) ((readByte(pos) & 0xFF) | (readByte(pos + 1) & 0xFF) << 8);
  //  }
  }

  @Override
  public int readInt(long pos) {

//{
      return buffer.getInt(pos);
 //   } else {
//      return ((readByte(pos) & 0xFF)
//          | (readByte(pos + 1) & 0xFF) << 8
//          | (readByte(pos + 2) & 0xFF) << 16
//          | (readByte(pos + 3) << 24));
 //   }
  }

  @Override
  public long readLong(long pos) {


   // if (blockOffset + Long.BYTES <= blockMask) {
      return buffer.getLong(pos);
   // } else {
//      final byte b1 = readByte(pos);
//      final byte b2 = readByte(pos + 1);
//      final byte b3 = readByte(pos + 2);
//      final byte b4 = readByte(pos + 3);
//      final byte b5 = readByte(pos + 4);
//      final byte b6 = readByte(pos + 5);
//      final byte b7 = readByte(pos + 6);
//      final byte b8 = readByte(pos + 7);
//      return (b8 & 0xFFL) << 56
//          | (b7 & 0xFFL) << 48
//          | (b6 & 0xFFL) << 40
//          | (b5 & 0xFFL) << 32
//          | (b4 & 0xFFL) << 24
//          | (b3 & 0xFFL) << 16
//          | (b2 & 0xFFL) << 8
//          | (b1 & 0xFFL);
//    }
  }

  @Override
  public void readFloats(float[] arr, int off, int len) throws EOFException {
    try {



        // Update pos early on for EOF detection, then try to get buffer content.

        buffer.getFloats(pos, arr, off, len);
        pos += (len - off) << 2;


    } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
      if (pos + (len - off) * Float.BYTES > size()) {
        throw new EOFException();
      } else {
        throw e; // Something is wrong.
      }
    }
  }

  public long position() {
    return pos;
  }

  public void seek(long position) throws EOFException {
    this.pos = position;
    if (position > size()) {
      this.pos = size();
      throw new EOFException();
    }
  }

  @Override
  public void skipBytes(long numBytes) throws IOException {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    long skipTo = position() + numBytes;
    seek(skipTo);
  }

  public ByteBuffersDataInput slice(long offset, long length) {
    if (offset < 0 || length < 0 || offset + length > this.size) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "slice(offset=%s, length=%s) is out of bounds: %s",
              offset,
              length,
              this));
    }

    return new ByteBuffersDataInput(buffer, offset, length);
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%,d bytes, position: %,d%s",
        size(),
        position());
  }



  private static final boolean isPowerOfTwo(int v) {
    return (v & (v - 1)) == 0;
  }

  private static void ensureAssumptions(List<BulkUnsafeBuffer> buffers) {
    if (buffers.isEmpty()) {
      throw new IllegalArgumentException("Buffer list must not be empty.");
    }

    if (buffers.size() == 1) {
      // Special case of just a single buffer, conditions don't apply.
    } else {
      final int blockPage = determineBlockPage(buffers);

      // First buffer decides on block page length.
      if (!isPowerOfTwo(blockPage)) {
        throw new IllegalArgumentException(
            "The first buffer must have power-of-two position() + remaining(): 0x"
                + Integer.toHexString(blockPage));
      }

      // Any block from 2..last-1 should have the same page size.
      for (int i = 1, last = buffers.size() - 1; i < last; i++) {
        BulkUnsafeBuffer buffer = buffers.get(i);
        if (buffer.byteBuffer().position() != 0) {
          throw new IllegalArgumentException(
              "All buffers except for the first one must have position() == 0: " + buffer);
        }
        if (i != last && buffer.byteBuffer().remaining() != blockPage) {
          throw new IllegalArgumentException(
              "Intermediate buffers must share an identical remaining() power-of-two block size: 0x"
                  + Integer.toHexString(blockPage));
        }
      }
    }
  }

  static int determineBlockPage(List<BulkUnsafeBuffer> buffers) {
    BulkUnsafeBuffer first = buffers.get(0);

    final int blockPage = Math.toIntExact((long) first.byteBuffer().position()+ first.byteBuffer().remaining());
    return blockPage;
  }

}
