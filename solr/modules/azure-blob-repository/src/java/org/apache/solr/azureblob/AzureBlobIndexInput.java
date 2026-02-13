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
package org.apache.solr.azureblob;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.store.IndexInput;

class AzureBlobIndexInput extends IndexInput {

  private static final int MIN_PAGE_SIZE = 4 * 1024;
  private static final int DEFAULT_PAGE_SIZE = 512 * 1024;
  private static final int MAX_CACHED_PAGES = 128;

  private final String path;
  private final AzureBlobStorageClient client;
  private final long length;
  private final int pageSize;
  private final LruPageCache cache;

  private long position = 0L;
  private boolean closed = false;

  AzureBlobIndexInput(String path, AzureBlobStorageClient client, long length) {
    this(path, client, length, DEFAULT_PAGE_SIZE, MAX_CACHED_PAGES);
  }

  AzureBlobIndexInput(
      String path, AzureBlobStorageClient client, long length, int pageSize, int maxCachedPages) {
    super(path);
    this.path = path;
    this.client = client;
    this.length = length;
    this.pageSize = Math.max(MIN_PAGE_SIZE, pageSize);
    this.cache = new LruPageCache(maxCachedPages);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    cache.clear();
  }

  @Override
  public long getFilePointer() {
    return position;
  }

  @Override
  public void seek(long pos) throws IOException {
    ensureOpen();
    if (pos < 0 || pos > length) {
      throw new IOException("Seek position out of bounds: " + pos);
    }

    position = pos;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    ensureOpen();
    if (offset < 0 || length < 0 || offset + length > this.length) {
      throw new IOException("Slice out of bounds: offset=" + offset + ", length=" + length);
    }

    AzureBlobIndexInput slice =
        new AzureBlobIndexInput(
            getFullSliceDescription(sliceDescription), client, length, pageSize, MAX_CACHED_PAGES);

    slice.position = 0L;

    // Wrap client in a view that remaps range requests by adding base offset
    slice.clientViewBaseOffset = this.clientViewBaseOffset + offset;
    return slice;
  }

  @Override
  public byte readByte() throws IOException {
    ensureOpen();
    if (position >= length) {
      throw new EOFException("End of stream reached");
    }

    byte[] page = getPage(pageIndex(position));
    int inPageOffset = (int) (position % pageSize);
    byte value = page[inPageOffset];
    position += 1L;
    return value;
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    ensureOpen();
    if (len < 0) {
      throw new IOException("Length must be non-negative");
    }

    if (position + len > length) {
      throw new EOFException("End of stream reached");
    }

    int remaining = len;
    while (remaining > 0) {
      long pageIdx = pageIndex(position);
      byte[] page = getPage(pageIdx);
      int inPageOffset = (int) (position % pageSize);
      int toCopy = Math.min(remaining, pageSize - inPageOffset);
      System.arraycopy(page, inPageOffset, b, offset + (len - remaining), toCopy);
      position += toCopy;
      remaining -= toCopy;
    }
  }

  // Internal state for slices: base offset to add to all range requests
  private long clientViewBaseOffset = 0L;

  private byte[] getPage(long pageIdx) throws IOException {
    byte[] page = cache.get(pageIdx);
    if (page != null) {
      return page;
    }

    long absoluteOffset = clientViewBaseOffset + pageIdx * (long) pageSize;
    int bytesToRead = (int) Math.min(pageSize, length - pageIdx * (long) pageSize);
    if (bytesToRead <= 0) {
      throw new EOFException("End of stream reached");
    }

    page = new byte[bytesToRead];
    try (InputStream in = client.pullRangeStream(path, absoluteOffset, bytesToRead)) {
      int readTotal = 0;
      while (readTotal < bytesToRead) {
        int read = in.read(page, readTotal, bytesToRead - readTotal);
        if (read == -1) break;
        readTotal += read;
      }

      if (readTotal < bytesToRead) {
        throw new EOFException(
            "End of stream reached: expected " + bytesToRead + " bytes, got " + readTotal);
      }
    } catch (AzureBlobException e) {
      throw new IOException("Failed to fetch range page", e);
    }

    cache.put(pageIdx, page);
    return page;
  }

  private long pageIndex(long pos) {
    return pos / pageSize;
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("IndexInput is closed");
    }
  }

  private static final class LruPageCache extends LinkedHashMap<Long, byte[]> {
    private final int maxEntries;

    LruPageCache(int maxEntries) {
      super(16, 0.75f, true);
      this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
      return size() > maxEntries;
    }

    @Override
    public void clear() {
      super.clear();
    }
  }
}
