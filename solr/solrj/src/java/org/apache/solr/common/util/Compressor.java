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

// Interface for compression implementations, providing methods to compress and decompress data
public interface Compressor {

  /**
   * Check to determine if the data is compressed in the expected compression implementation
   *
   * @param data - the bytes to check for compression
   * @return true if the data is compressed in the expected compression implementation
   */
  boolean isCompressedBytes(byte[] data);

  /**
   * Decompresses compressed bytes, returning the uncompressed data as a byte[]
   *
   * @param data the input compressed data to decompress
   * @return the decompressed bytes
   * @throws Exception - The data is not compressed or the data is not compressed in the correct
   *     format
   */
  byte[] decompressBytes(byte[] data) throws Exception;

  /**
   * Compresses bytes into compressed bytes using the compression implementation
   *
   * @param data the input uncompressed data to be compressed
   * @return compressed bytes
   */
  byte[] compressBytes(byte[] data);

  /**
   * Compresses bytes into compressed bytes using the compression implementation
   *
   * @param data the input uncompressed data to be compressed
   * @param initialBufferCapacity the initial capacity of the buffer storing the compressed data. It
   *     depends on the data type and the caller may know the expected average compression factor.
   *     If this initial capacity is smaller than 16, the buffer capacity will be 16 anyway.
   * @return compressed bytes
   */
  byte[] compressBytes(byte[] data, int initialBufferCapacity);
}
