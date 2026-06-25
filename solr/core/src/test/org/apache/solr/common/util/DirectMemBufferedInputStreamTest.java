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

package org.apache.solr.common.util;

import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import org.agrona.concurrent.MappedResizeableBuffer;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestUtil;
import org.junit.Test;

/**
 * Regression for review finding H5: the {@link java.io.DataInput} primitives
 * ({@code readByte/readShort/readInt/readLong/readFully}) did an unchecked Agrona
 * {@code buffer.getX(position)} with no bound against the logical written {@code length}.
 * Because the memory mapping has pre-grown slack beyond the written size, a read past the
 * logical end either returned stale/zero slack bytes (silent garbage decoded as a plausible
 * record) or, with a torn length field, walked past the mapping capacity into a native SIGSEGV
 * that {@code catch(Throwable)} cannot catch. Every primitive must now fail closed with
 * {@link EOFException} so {@code LogReader.next()}'s torn-record tolerance sees a clean EOF.
 */
public class DirectMemBufferedInputStreamTest extends SolrTestCase {

  private interface Read {
    void run() throws Exception;
  }

  private static void expectEOF(String what, Read r) {
    try {
      r.run();
      fail(what + " did not throw EOFException past the logical end (read mapped slack)");
    } catch (EOFException expected) {
      // good: fail closed
    } catch (Exception e) {
      fail(what + " threw " + e + " instead of EOFException");
    }
  }

  @Test
  public void testPrimitivesFailClosedPastLength() throws Exception {
    Path dir = SolrTestUtil.createTempDir();
    File f = new File(dir.toFile(), "h5buf.bin");
    final long capacity = 256; // mapped capacity — everything past 'length' is pre-grown slack
    final long length = 8;     // logical written size
    try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
      // Back the whole mapping with real (zero-filled) file bytes so the slack between 'length'
      // and 'capacity' reads as silent zero-garbage (the H5.2 stale-slack case) rather than
      // SIGBUS-ing the test itself — the bound must turn that slack read into a clean EOFException.
      raf.setLength(capacity);
      FileChannel channel = raf.getChannel();
      MappedResizeableBuffer buffer = new MappedResizeableBuffer(channel, 0, capacity);
      try {
        // 8 real bytes of content; everything from 8..capacity is mapped slack (zeros/stale).
        for (long i = 0; i < length; i++) {
          buffer.putByte(i, (byte) (i + 1));
        }

        // In-bounds reads must still work (two ints == the 8 logical bytes).
        DirectMemBufferedInputStream in = new DirectMemBufferedInputStream(buffer, length);
        in.readInt();
        in.readInt();
        // position == length; any further read consumes slack and must fail closed.
        expectEOF("readByte at end", in::readByte);

        // readInt straddling the boundary (pos 6, needs 4 of the 8 logical bytes).
        DirectMemBufferedInputStream straddleInt = new DirectMemBufferedInputStream(buffer, length);
        straddleInt.position(6);
        expectEOF("readInt straddling length", straddleInt::readInt);

        // readLong needs 8 but only 4 logical bytes available.
        DirectMemBufferedInputStream shortLong = new DirectMemBufferedInputStream(buffer, 4);
        expectEOF("readLong past length", shortLong::readLong);

        // readShort straddling (pos 7, needs 2).
        DirectMemBufferedInputStream straddleShort = new DirectMemBufferedInputStream(buffer, length);
        straddleShort.position(7);
        expectEOF("readShort straddling length", straddleShort::readShort);

        // readFully(byte[]) past length (pos 6, needs 4).
        DirectMemBufferedInputStream rf = new DirectMemBufferedInputStream(buffer, length);
        rf.position(6);
        expectEOF("readFully past length", () -> rf.readFully(new byte[4]));

        buffer.close();
      } catch (Throwable t) {
        buffer.close();
        throw t;
      }
    }
  }
}
