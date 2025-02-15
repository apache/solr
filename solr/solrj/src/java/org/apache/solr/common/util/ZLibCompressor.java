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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class ZLibCompressor implements Compressor {

  private static final byte[] ZLIB_MAGIC = new byte[] {0x78, 0x1};
  private static final int COMPRESSED_SIZE_MAGIC_NUMBER = 2018370979;

  /**
   * {@inheritDoc}
   *
   * <p>Uses the hex magic number for zlib compression '78 01' to check if the bytes are compressed
   */
  @Override
  public boolean isCompressedBytes(byte[] data) {
    if (data == null || data.length < 2) return false;
    return ZLIB_MAGIC[0] == data[0] && ZLIB_MAGIC[1] == data[1];
  }

  @Override
  public byte[] decompressBytes(byte[] data) throws Exception {
    if (data == null) return null;
    Inflater inflater = new Inflater();
    try {
      inflater.setInput(data, 0, data.length);
      // Attempt to get the decompressed size from trailing bytes, this will be present if
      // compressed by Solr
      ByteBuffer bb = ByteBuffer.wrap(data, data.length - 8, 8);
      int decompressedSize = bb.getInt();
      int xoredSize = bb.getInt();
      if ((decompressedSize ^ COMPRESSED_SIZE_MAGIC_NUMBER) != xoredSize) {
        // Take best guess of decompressed size since it wasn't included in trailing bytes, assume a
        // 5:1 ratio
        decompressedSize = 5 * data.length;
      }
      byte[] buf = new byte[decompressedSize];
      int actualDecompressedSize = 0;
      while (!inflater.finished()) {
        if (actualDecompressedSize >= buf.length) {
          buf = Arrays.copyOf(buf, (int) (buf.length * 1.5));
        }
        actualDecompressedSize += inflater.inflate(buf, actualDecompressedSize, decompressedSize);
      }
      if (buf.length != actualDecompressedSize) {
        buf = Arrays.copyOf(buf, actualDecompressedSize);
      }
      return buf;
    } finally {
      inflater.end();
    }
  }

  @Override
  public byte[] compressBytes(byte[] data) {
    // By default, the compression ratio is assumed to be 5:1 to set the initial capacity of the
    // compression buffer.
    return compressBytes(data, data.length / 5);
  }

  @Override
  public byte[] compressBytes(byte[] data, int initialBufferCapacity) {
    Deflater compressor = new Deflater(Deflater.BEST_SPEED);
    try {
      compressor.setInput(data);
      compressor.finish();
      byte[] buf = new byte[Math.max(initialBufferCapacity, 16)];
      int compressedSize = 0;
      while (!compressor.finished()) {
        if (compressedSize >= buf.length) {
          buf = Arrays.copyOf(buf, (int) (buf.length * 1.5));
        }
        compressedSize += compressor.deflate(buf, compressedSize, buf.length - compressedSize);
      }

      buf = Arrays.copyOf(buf, compressedSize + 8);

      // Include the decompressed size and xored decompressed size in trailing bytes, this makes
      // decompression
      // efficient while also being compatible with alternative zlib implementations
      ByteBuffer.wrap(buf, compressedSize, 8)
          .putInt(data.length)
          .putInt(data.length ^ COMPRESSED_SIZE_MAGIC_NUMBER);
      return buf;
    } finally {
      compressor.end();
    }
  }
}
