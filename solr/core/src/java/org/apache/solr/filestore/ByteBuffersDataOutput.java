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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.UnicodeUtil;

public final class ByteBuffersDataOutput extends DataOutput implements Accountable {
  private static final BulkUnsafeBuffer EMPTY = BulkUnsafeBuffer.wrapBuffer(ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN));

  private static final byte[] EMPTY_BYTE_ARRAY = {};

  /** A singleton instance of "no-reuse" buffer strategy. */
  public static final Consumer<BulkUnsafeBuffer> REUSE =
      (bb) -> { };

//  public ByteBuffer copy() {
//    return currentBlock.byteBuffer().duplicate();
//  }

  public BulkUnsafeBuffer getBuffer() {
    return currentBlock;
  }

  public long position() {
    return pos;
  }

  /**
   * An implementation of a {@link ByteBuffer} allocation and recycling policy. The blocks are
   * recycled if exactly the same size is requested, otherwise they're released to be GCed.
   */
  public static final class ByteBufferRecycler {
    private final ArrayDeque<BulkUnsafeBuffer> reuse = new ArrayDeque<>();


    public ByteBufferRecycler() {

    }

    public BulkUnsafeBuffer allocate(int size) {
      while (!reuse.isEmpty()) {
        BulkUnsafeBuffer bb = reuse.removeFirst();
        // If we don't have a buffer of exactly the requested size, discard it.
        if (bb.byteBuffer().remaining() == size) {
          return bb;
        }
      }

      return BulkUnsafeBuffer.wrapBuffer(ByteBuffer.allocate(size));
    }

    public void reuse(BulkUnsafeBuffer buffer) {
      buffer.byteBuffer().rewind();
      reuse.addLast(buffer);
    }
  }




  /** {@link ByteBuffer} recycler on {@link #reset}. */
  private final Consumer<BulkUnsafeBuffer> blockReuse;


  /** Cumulative RAM usage across all blocks. */
  private long ramBytesUsed;

  /** The current-or-next write block. */
  private BulkUnsafeBuffer currentBlock = EMPTY;
  private long pos = 0;

//  public ByteBuffersDataOutput() {
//    this();
//  }

  public ByteBuffersDataOutput(
      Consumer<BulkUnsafeBuffer> blockReuse) {

    this.blockReuse = Objects.requireNonNull(blockReuse, "Block reuse must not be null.");
  }

  @Override
  public void writeByte(byte b) {
//    if (!currentBlock.byteBuffer().hasRemaining()) {
//     throw new EOFException();
//    }

    currentBlock.putByte(pos, b);
    pos+=1;
  }

  @Override
  public void writeBytes(byte[] src, int offset, int length) {
    assert length >= 0;

//      if (!currentBlock.byteBuffer().hasRemaining()) {
//        throw new EOFException();
//      }


      currentBlock.putBytes(offset, src, offset, length - offset);
     pos+=length - offset;

  }

  @Override
  public void writeBytes(byte[] b, int length) {
    writeBytes(b, 0, length);
  }

  public void writeBytes(byte[] b) {
    writeBytes(b, 0, b.length);
  }

  public void writeBytes(BulkUnsafeBuffer buffer, long position,  long length) {
    //ByteBuffer buff = buffer.byteBuffer().duplicate();
    //int length = buff.limit();

//      if (!currentBlock.byteBuffer().hasRemaining()) {
//        appendBlock();
//      }


    //buff.limit(pos + buff.remaining());

    currentBlock.putBytes(pos, buffer, position, length);
    this.pos += (length - position);


  }

  /**
   * Return a list of read-only view of {@link ByteBuffer} blocks over the current content written
   * to the output.
   */
//  public ArrayList<BulkUnsafeBuffer> toBufferList() {
//    ArrayList<BulkUnsafeBuffer> result = new ArrayList<>(Math.max(blocks.size(), 1));
//    if (blocks.isEmpty()) {
//      result.add(EMPTY);
//    } else {
//      for (BulkUnsafeBuffer bb : blocks) {
//        bb = BulkUnsafeBuffer.wrapBuffer(bb.byteBuffer().asReadOnlyBuffer().flip().order(ByteOrder.LITTLE_ENDIAN));
//        result.add(bb);
//      }
//    }
//    return result;
//  }

//  public ArrayList<BulkUnsafeBuffer> toWriteableBufferList() {
//    ArrayList<BulkUnsafeBuffer> result = new ArrayList<>(Math.max(blocks.size(), 1));
//    if (blocks.isEmpty()) {
//      result.add(EMPTY);
//    } else {
//      for (BulkUnsafeBuffer bb : blocks) {
//        result.add(bb);
//      }
//    }
//    return result;
//  }


  public ByteBuffersDataInput toDataInput() {
    return new ByteBuffersDataInput(currentBlock, 0, size());
  }

  /**
   * Return a contiguous array with the current content written to the output. The returned array is
   * always a copy (can be mutated).
   *
   * <p>If the {@link #size()} of the underlying buffers exceeds maximum size of Java array, an
   * {@link RuntimeException} will be thrown.
   */
//  public byte[] toArrayCopy() {
//    if (blocks.isEmpty()) {
//      return EMPTY_BYTE_ARRAY;
//    }
//
//    // We could try to detect single-block, array-based ByteBuffer here
//    // and use Arrays.copyOfRange, but I don't think it's worth the extra
//    // instance checks.
//    long size = size();
//    if (size > Integer.MAX_VALUE) {
//      throw new RuntimeException("Data exceeds maximum size of a single byte array: " + size);
//    }
//
//    byte[] arr = new byte[Math.toIntExact(size())];
//    int offset = 0;
//    for (BulkUnsafeBuffer bb : toBufferList()) {
//      int len = bb.byteBuffer().remaining();
//      bb.getBytes(bb.byteBuffer().position(), arr, offset, len);
//      offset += len;
//    }
//    return arr;
//  }

  /** Copy the current content of this object into another {@link DataOutput}. */
//  public void copyTo(DataOutput output) throws IOException {
//
//    BulkUnsafeBuffer bb = BulkUnsafeBuffer.wrapBuffer(currentBlock.byteBuffer().asReadOnlyBuffer().flip());
//        output.copyBytes(new ByteBuffersDataInput(bb, 0, size()), size());
//
//
//  }

  /** @return The number of bytes written to this output so far. */
  public long size() {
    return currentBlock.capacity();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT, "%,d bytes", size());
  }

  // Specialized versions of writeXXX methods that break execution into
  // fast/ slow path if the result would fall on the current block's
  // boundary.
  //
  // We also remove the IOException from methods because it (theoretically)
  // cannot be thrown from byte buffers.

  @Override
  public void writeShort(short v) {
    try {
      if (currentBlock.capacity() - pos >= Short.BYTES) {
        int pos = currentBlock.byteBuffer().position();
        currentBlock.putShort(pos, v);
       pos += org.agrona.BitUtil.SIZE_OF_SHORT;
      } else {
        super.writeShort(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeInt(int v) {
    try {
      if (currentBlock.capacity() - pos >= Integer.BYTES) {

        currentBlock.putInt(pos, v);
        pos+= org.agrona.BitUtil.SIZE_OF_INT;
      } else {
        super.writeInt(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeLong(long v) {
    try {
      if (currentBlock.capacity() - pos  >= Long.BYTES) {

        currentBlock.putLong(pos, v);
       pos += org.agrona.BitUtil.SIZE_OF_LONG;
      } else {
        super.writeLong(v);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeString(String v) {
    try {
      final int MAX_CHARS_PER_WINDOW = 1024;
      if (v.length() <= MAX_CHARS_PER_WINDOW) {
        final BytesRef utf8 = new BytesRef(v);
        writeVInt(utf8.length);
        writeBytes(utf8.bytes, utf8.offset, utf8.length);
      } else {
        writeVInt(UnicodeUtil.calcUTF16toUTF8Length(v, 0, v.length()));
        final byte[] buf = new byte[UnicodeUtil.MAX_UTF8_BYTES_PER_CHAR * MAX_CHARS_PER_WINDOW];
        UTF16toUTF8(
            v,
            0,
            v.length(),
            buf,
            (len) -> {
              writeBytes(buf, 0, len);
            });
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeMapOfStrings(Map<String, String> map) {
    try {
      super.writeMapOfStrings(map);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void writeSetOfStrings(Set<String> set) {
    try {
      super.writeSetOfStrings(set);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long ramBytesUsed() {
    // Return a rough estimation for allocated blocks. Note that we do not make
    // any special distinction for direct memory buffers.
//    assert ramBytesUsed
//        == currentBlock.capacity() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    return ramBytesUsed;
  }

  /**
   * This method resets this object to a clean (zero-size) state and publishes any currently
   * allocated buffers for reuse to the reuse strategy provided in the constructor.
   *
   * <p>Sharing byte buffers for reads and writes is dangerous and will very likely lead to
   * hard-to-debug issues, use with great care.
   */
  public void reset() {

    blockReuse.accept(currentBlock);


    ramBytesUsed = 0;
    pos = 0;
    currentBlock = EMPTY;
  }

  /** @return Returns a new {@link ByteBuffersDataOutput} with the {@link #reset()} capability. */
  // TODO: perhaps we can move it out to an utility class (as a supplier of preconfigured
  // instances?)
  public static ByteBuffersDataOutput newResettableInstance() {
    ByteBuffersDataOutput.ByteBufferRecycler reuser =
            new ByteBuffersDataOutput.ByteBufferRecycler();
    return new ByteBuffersDataOutput(
        reuser::reuse);
  }

//  private int blockSize() {
//    return 1 << blockBits;
//  }

//  private void appendBlock() {
//    if (blocks.size() >= MAX_BLOCKS_BEFORE_BLOCK_EXPANSION && blockBits < maxBitsPerBlock) {
//      rewriteToBlockSize(blockBits + 1);
//      if (blocks.getLast().byteBuffer().hasRemaining()) {
//        return;
//      }
//    }
//
//    final int requiredBlockSize = 1 << blockBits;
//    currentBlock = BulkUnsafeBuffer.wrapBuffer(ByteBuffer.allocate(requiredBlockSize).order(ByteOrder.LITTLE_ENDIAN));
//
//    assert currentBlock.capacity() == requiredBlockSize;
//    blocks.add(currentBlock);
//    ramBytesUsed += RamUsageEstimator.NUM_BYTES_OBJECT_REF + currentBlock.capacity();
//  }

//  private void rewriteToBlockSize(int targetBlockBits) {
//    assert targetBlockBits <= maxBitsPerBlock;
//
//    // We copy over data blocks to an output with one-larger block bit size.
//    // We also discard references to blocks as we're copying to allow GC to
//    // clean up partial results in case of memory pressure.
//    ByteBuffersDataOutput cloned =
//        new ByteBuffersDataOutput(targetBlockBits, targetBlockBits, NO_REUSE);
//    BulkUnsafeBuffer block;
//    while ((block = blocks.pollFirst()) != null) {
//      block.byteBuffer().flip();
//      cloned.writeBytes(block);
//      if (blockReuse != NO_REUSE) {
//        blockReuse.accept(block);
//      }
//    }
//
//    assert blocks.isEmpty();
//    this.blockBits = targetBlockBits;
//    blocks.addAll(cloned.blocks);
//    ramBytesUsed = cloned.ramBytesUsed;
//  }

  // TODO: move this block-based conversion to UnicodeUtil.

  private static final long HALF_SHIFT = 10;
  private static final int SURROGATE_OFFSET =
      Character.MIN_SUPPLEMENTARY_CODE_POINT
          - (UnicodeUtil.UNI_SUR_HIGH_START << HALF_SHIFT)
          - UnicodeUtil.UNI_SUR_LOW_START;

  /** A consumer-based UTF16-UTF8 encoder (writes the input string in smaller buffers.). */
  private static int UTF16toUTF8(
      final CharSequence s,
      final int offset,
      final int length,
      byte[] buf,
      IntConsumer bufferFlusher) {
    int utf8Len = 0;
    int j = 0;
    for (int i = offset, end = offset + length; i < end; i++) {
      final int chr = (int) s.charAt(i);

      if (j + 4 >= buf.length) {
        bufferFlusher.accept(j);
        utf8Len += j;
        j = 0;
      }

      if (chr < 0x80) buf[j++] = (byte) chr;
      else if (chr < 0x800) {
        buf[j++] = (byte) (0xC0 | (chr >> 6));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else if (chr < 0xD800 || chr > 0xDFFF) {
        buf[j++] = (byte) (0xE0 | (chr >> 12));
        buf[j++] = (byte) (0x80 | ((chr >> 6) & 0x3F));
        buf[j++] = (byte) (0x80 | (chr & 0x3F));
      } else {
        // A surrogate pair. Confirm valid high surrogate.
        if (chr < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // Confirm valid low surrogate and write pair.
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            utf32 = (chr << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            buf[j++] = (byte) (0xF0 | (utf32 >> 18));
            buf[j++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
            buf[j++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
            buf[j++] = (byte) (0x80 | (utf32 & 0x3F));
            continue;
          }
        }
        // Replace unpaired surrogate or out-of-order low surrogate
        // with substitution character.
        buf[j++] = (byte) 0xEF;
        buf[j++] = (byte) 0xBF;
        buf[j++] = (byte) 0xBD;
      }
    }

    bufferFlusher.accept(j);
    utf8Len += j;

    return utf8Len;
  }
}
