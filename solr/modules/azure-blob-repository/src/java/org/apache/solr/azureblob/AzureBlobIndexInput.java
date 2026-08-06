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
import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

/**
 * {@link BufferedIndexInput} implementation that reads from a single blob in Azure Blob Storage,
 * lazily opening per-instance HTTP range streams via {@link AzureBlobStorageClient}.
 */
class AzureBlobIndexInput extends BufferedIndexInput {

  static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  static final int LOCAL_BUFFER_SIZE = 16 * 1024;

  private final AzureBlobStorageClient client;
  private final String path;

  private final long absoluteOffset;
  private final long length;

  private InputStream inputStream;
  private long streamAbsolutePos = -1L;
  private boolean closed = false;

  AzureBlobIndexInput(AzureBlobStorageClient client, String path, long length) {
    this(client, path, 0L, length, "AzureBlobIndexInput(" + path + ")", DEFAULT_BUFFER_SIZE);
  }

  private AzureBlobIndexInput(
      AzureBlobStorageClient client,
      String path,
      long absoluteOffset,
      long length,
      String resourceDescription,
      int bufferSize) {
    super(resourceDescription, bufferSize);
    this.client = client;
    this.path = path;
    this.absoluteOffset = absoluteOffset;
    this.length = length;
  }

  @Override
  protected void readInternal(ByteBuffer dst) throws IOException {
    if (closed) {
      throw new AlreadyClosedException("Already closed: " + this);
    }

    int expectedLength = dst.remaining();
    if (expectedLength == 0) {
      return;
    }

    long targetAbsolutePos = absoluteOffset + getFilePointer();
    ensureStreamAt(targetAbsolutePos);

    byte[] localBuffer = null;
    try {
      while (dst.hasRemaining()) {
        int read;
        if (dst.hasArray()) {
          read = inputStream.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
        } else {
          if (localBuffer == null) {
            localBuffer = new byte[LOCAL_BUFFER_SIZE];
          }
          read = inputStream.read(localBuffer, 0, Math.min(dst.remaining(), localBuffer.length));
        }

        if (read <= 0) {
          break;
        }

        if (dst.hasArray()) {
          dst.position(dst.position() + read);
        } else {
          dst.put(localBuffer, 0, read);
        }
        streamAbsolutePos += read;
      }

      if (dst.remaining() > 0) {
        throw new EOFException(
            String.format(
                Locale.ROOT,
                "read past EOF: expected %d bytes at pos %d but only got %d (length=%d): %s",
                expectedLength,
                targetAbsolutePos,
                expectedLength - dst.remaining(),
                length,
                this));
      }
    } catch (IOException | RuntimeException e) {
      closeStream();
      throw e;
    }
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (closed) {
      throw new AlreadyClosedException("Already closed: " + this);
    }
    if (pos < 0 || pos > length) {
      throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length + ": " + this);
    }

    closeStream();
  }

  private void ensureStreamAt(long targetAbsolutePos) throws IOException {
    if (inputStream != null && streamAbsolutePos == targetAbsolutePos) {
      return;
    }

    closeStream();

    long remaining = (absoluteOffset + length) - targetAbsolutePos;
    if (remaining <= 0) {
      throw new EOFException(
          "read past EOF: pos=" + targetAbsolutePos + " vs end=" + (absoluteOffset + length));
    }

    inputStream = client.pullRangeStream(path, targetAbsolutePos, remaining);
    streamAbsolutePos = targetAbsolutePos;
  }

  private void closeStream() {
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException ignored) {
        // best-effort
      }
      inputStream = null;
      streamAbsolutePos = -1L;
    }
  }

  @Override
  public final long length() {
    return length;
  }

  @Override
  public AzureBlobIndexInput clone() {
    AzureBlobIndexInput clone = (AzureBlobIndexInput) super.clone();
    clone.inputStream = null;
    clone.streamAbsolutePos = -1L;
    return clone;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (closed) {
      throw new AlreadyClosedException("Already closed: " + this);
    }

    if (offset < 0 || length < 0 || length > this.length - offset) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "slice() %s out of bounds: offset=%d,length=%d,fileLength=%d: %s",
              sliceDescription,
              offset,
              length,
              this.length,
              this));
    }
    return new AzureBlobIndexInput(
        client,
        path,
        this.absoluteOffset + offset,
        length,
        getFullSliceDescription(sliceDescription),
        getBufferSize());
  }

  @Override
  public void close() throws IOException {
    closed = true;
    closeStream();
  }
}
