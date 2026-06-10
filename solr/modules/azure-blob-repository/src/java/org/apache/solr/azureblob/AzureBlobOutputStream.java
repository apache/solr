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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OutputStream implementation for Azure Blob Storage using block blobs. Supports chunked uploads
 * for large files.
 */
class AzureBlobOutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int BLOCK_SIZE = 4 * 1024 * 1024;

  private final BlobClient blobClient;
  private final String blobPath;
  private boolean closed;
  private final ByteBuffer buffer;
  private BlockUpload blockUpload;

  AzureBlobOutputStream(BlobClient blobClient, String blobPath) {
    this.blobClient = blobClient;
    this.blobPath = blobPath;
    this.closed = false;
    this.buffer = ByteBuffer.allocate(BLOCK_SIZE);
    this.blockUpload = null;

    if (log.isDebugEnabled()) {
      log.debug("Created BlobOutputStream for blobPath '{}'", blobPath);
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    buffer.put((byte) b);

    if (!buffer.hasRemaining()) {
      uploadBlock();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    int currentOffset = off;
    int lenRemaining = len;
    while (buffer.remaining() < lenRemaining) {
      int firstPart = buffer.remaining();
      buffer.put(b, currentOffset, firstPart);
      uploadBlock();

      currentOffset += firstPart;
      lenRemaining -= firstPart;
    }
    if (lenRemaining > 0) {
      buffer.put(b, currentOffset, lenRemaining);
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  private void uploadBlock() throws IOException {
    int size = buffer.position();

    if (size == 0) {
      return;
    }

    if (blockUpload == null) {
      if (log.isDebugEnabled()) {
        log.debug("New block upload for blobPath '{}'", blobPath);
      }

      blockUpload = new BlockUpload();
    }

    BinaryData data = BinaryData.fromByteBuffer(ByteBuffer.wrap(buffer.array(), 0, size));
    try {
      blockUpload.uploadBlock(data);
    } catch (IOException | RuntimeException e) {
      blockUpload.markFailed();
      if (log.isDebugEnabled()) {
        log.debug("Block upload marked as failed for blobPath '{}'.", blobPath);
      }
      throw e;
    }

    buffer.clear();
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    // Intentionally a no-op. Full blocks are staged as the buffer fills in write(), and the
    // partial tail is staged in close(). Staging on every flush() would create tiny blocks and a
    // frequently-flushing caller could exhaust Azure's 50,000-committed-block limit on small files.
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      if (blockUpload != null && blockUpload.failed) {
        blockUpload = null;
        return;
      }

      // Stage any remaining buffered bytes as the final block.
      uploadBlock();

      if (blockUpload != null) {
        blockUpload.complete();
        blockUpload = null;
      } else {
        try {
          blobClient.upload(BinaryData.fromBytes(new byte[0]), true);
        } catch (BlobStorageException e) {
          throw new IOException(
              "Failed to create empty blob", AzureBlobStorageClient.handleBlobException(e));
        }
      }
    } finally {
      closed = true;
    }
  }

  private class BlockUpload {
    private final List<String> blockIds;
    private boolean failed = false;

    BlockUpload() {
      this.blockIds = new ArrayList<>();
      if (log.isDebugEnabled()) {
        log.debug("Initiated block upload for blobPath '{}'", blobPath);
      }
    }

    void uploadBlock(BinaryData data) throws IOException {
      if (failed) {
        throw new IllegalStateException(
            "Can't upload new blocks on a BlockUpload that previously failed");
      }

      String blockId =
          Base64.getEncoder()
              .encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

      if (log.isDebugEnabled()) {
        log.debug("Uploading block {} for blobPath '{}'", blockId, blobPath);
      }

      try {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        blockBlobClient.stageBlock(blockId, data);
        blockIds.add(blockId);
      } catch (BlobStorageException e) {
        throw new IOException(
            "Failed to upload block", AzureBlobStorageClient.handleBlobException(e));
      }
    }

    void complete() throws IOException {
      if (failed) {
        throw new IllegalStateException("Can't complete a BlockUpload that previously failed");
      }

      if (log.isDebugEnabled()) {
        log.debug("Completing block upload for blobPath '{}'", blobPath);
      }

      try {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        blockBlobClient.commitBlockList(blockIds);
      } catch (BlobStorageException e) {
        throw new IOException(
            "Failed to commit block list", AzureBlobStorageClient.handleBlobException(e));
      }
    }

    void markFailed() {
      if (log.isWarnEnabled()) {
        log.warn("Marking block upload as failed for blobPath '{}'", blobPath);
      }

      failed = true;
    }
  }
}
