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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Proves that {@link ByteBuffersDataOutput#size()} returns the <em>logical write position</em>
 * (bytes written so far, i.e. {@code pos}) and NOT the capacity of the backing buffer.
 *
 * <p>The pre-fix bug: {@code size()} returned {@code currentBlock.capacity()}, which made callers
 * such as {@link ByteBuffersDirectory#OUTPUT_AS_ONE_BUFFER} over-read into uninitialised bytes.
 */
public class ByteBuffersDataOutputTest extends SolrTestCase {

  /** Inject a pre-sized BulkUnsafeBuffer into the output's private currentBlock field. */
  private static ByteBuffersDataOutput withBackingBuffer(int capacity) throws Exception {
    BulkUnsafeBuffer buf = BulkUnsafeBuffer.wrapBuffer(
        ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN));
    ByteBuffersDataOutput out = new ByteBuffersDataOutput(ByteBuffersDataOutput.REUSE);
    Field f = ByteBuffersDataOutput.class.getDeclaredField("currentBlock");
    f.setAccessible(true);
    f.set(out, buf);
    return out;
  }

  /** Before any write, size() and position() are both 0. */
  @Test
  public void testSizeZeroBeforeWrite() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(1024);
    assertEquals(0L, out.size());
    assertEquals(0L, out.position());
  }

  /**
   * After writing N bytes, size() == N — not CAPACITY (1024). If size() returned capacity this
   * test would report 1024 and fail.
   */
  @Test
  public void testSizeEqualsWrittenBytesNotCapacity() throws Exception {
    final int CAPACITY = 1024;
    final int TO_WRITE = 7;

    ByteBuffersDataOutput out = withBackingBuffer(CAPACITY);
    byte[] data = new byte[TO_WRITE];
    for (int i = 0; i < TO_WRITE; i++) data[i] = (byte) (i + 1);
    out.writeBytes(data);

    assertEquals("size() must equal bytes written, not backing capacity",
        (long) TO_WRITE, out.size());
    assertEquals("size() must equal position()", out.position(), out.size());
    assertTrue("size() < capacity proves it is not returning capacity()",
        out.size() < CAPACITY);
  }

  /** size() grows by exactly 1 on each writeByte call. */
  @Test
  public void testSizeGrowsByOnePerWriteByte() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(64);
    for (int i = 1; i <= 10; i++) {
      out.writeByte((byte) i);
      assertEquals("size() after writeByte #" + i, (long) i, out.size());
    }
  }

  /** size() grows by N on writeBytes(byte[], len). */
  @Test
  public void testSizeGrowsByLengthOnWriteBytes() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(256);
    byte[] chunk = new byte[20];
    out.writeBytes(chunk);
    assertEquals(20L, out.size());
    // write another chunk
    out.writeBytes(chunk);
    assertEquals(40L, out.size());
    assertEquals(out.position(), out.size());
  }

  /** size() grows by 4 on writeInt. */
  @Test
  public void testSizeGrowsByFourOnWriteInt() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(64);
    out.writeInt(0xDEADBEEF);
    assertEquals(4L, out.size());
    assertEquals(out.position(), out.size());
  }

  /** size() grows by 8 on writeLong. */
  @Test
  public void testSizeGrowsByEightOnWriteLong() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(64);
    out.writeLong(0xAABBCCDDEEFF0011L);
    assertEquals(8L, out.size());
    assertEquals(out.position(), out.size());
  }

  /** After reset(), size() returns 0. */
  @Test
  public void testSizeZeroAfterReset() throws Exception {
    ByteBuffersDataOutput out = withBackingBuffer(128);
    out.writeByte((byte) 42);
    out.writeByte((byte) 43);
    assertEquals(2L, out.size());

    out.reset();
    assertEquals("size() must be 0 after reset()", 0L, out.size());
    assertEquals("position() must be 0 after reset()", 0L, out.position());
  }

  /**
   * toDataInput().size() equals the output's size() (bytes written), not backing capacity.
   * This is the caller-side proof: ByteBuffersDirectory's OUTPUT_AS_ONE_BUFFER uses size() to
   * bound the DataInput — if size() were capacity, reads would over-run into uninitialised bytes.
   */
  @Test
  public void testToDataInputSizeMatchesWrittenBytes() throws Exception {
    final int CAPACITY = 256;
    final int TO_WRITE = 13;

    ByteBuffersDataOutput out = withBackingBuffer(CAPACITY);
    byte[] data = new byte[TO_WRITE];
    for (int i = 0; i < TO_WRITE; i++) data[i] = (byte) (i * 3);
    out.writeBytes(data);

    ByteBuffersDataInput input = out.toDataInput();
    assertEquals("DataInput size must equal bytes written, not backing capacity",
        (long) TO_WRITE, input.size());
  }
}
