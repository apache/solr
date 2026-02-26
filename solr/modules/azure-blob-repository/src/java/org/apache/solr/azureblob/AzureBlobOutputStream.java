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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.ByteArrayInputStream;
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
public class AzureBlobOutputStream extends OutputStream {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int BLOCK_SIZE = 4 * 1024 * 1024;

  private final BlobClient blobClient;
  private final String blobPath;
  private volatile boolean closed;
  private final ByteBuffer buffer;
  private BlockUpload blockUpload;
  private boolean committed;

  public AzureBlobOutputStream(BlobClient blobClient, String blobPath) {
    this.blobClient = blobClient;
    this.blobPath = blobPath;
    this.closed = false;
    this.buffer = ByteBuffer.allocate(BLOCK_SIZE);
    this.blockUpload = null;
    this.committed = false;

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
    int size = buffer.position() - buffer.arrayOffset();

    if (size == 0) {
      return;
    }

    if (blockUpload == null) {
      if (log.isDebugEnabled()) {
        log.debug("New block upload for blobPath '{}'", blobPath);
      }

      blockUpload = newBlockUpload();
    }

    try (ByteArrayInputStream inputStream =
        new ByteArrayInputStream(buffer.array(), buffer.arrayOffset(), size)) {
      blockUpload.uploadBlock(inputStream, size);
    } catch (BlobStorageException e) {
      if (blockUpload != null) {
        blockUpload.abort();
        if (log.isDebugEnabled()) {
          log.debug("Block upload aborted for blobPath '{}'.", blobPath);
        }
      }

      throw new IOException(
          "Failed to upload block", AzureBlobStorageClient.handleBlobException(e));
    }

    buffer.clear();
  }

  @Override
  public void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (buffer.position() - buffer.arrayOffset() > 0) {
      uploadBlock();
    }

    if (blockUpload != null) {
      blockUpload.complete();
      blockUpload = null;
      committed = true;
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    if (blockUpload != null && blockUpload.aborted) {
      blockUpload = null;
      closed = true;
      return;
    }

    if (!committed) {
      uploadBlock();
      if (blockUpload != null) {
        blockUpload.complete();
        blockUpload = null;
        committed = true;
      } else {
        try {
          blobClient.upload(new ByteArrayInputStream(new byte[0]), 0, true);
        } catch (BlobStorageException e) {
          throw new IOException(
              "Failed to create empty blob", AzureBlobStorageClient.handleBlobException(e));
        }
      }
    } else {
      if (blockUpload != null) {
        blockUpload.complete();
        blockUpload = null;
      }
    }

    closed = true;
  }

  private BlockUpload newBlockUpload() throws IOException {
    try {
      return new BlockUpload();
    } catch (BlobStorageException e) {
      throw new IOException(
          "Failed to create block upload", AzureBlobStorageClient.handleBlobException(e));
    }
  }

  private class BlockUpload {
    private final List<String> blockIds;
    private boolean aborted = false;

    public BlockUpload() {
      this.blockIds = new ArrayList<>();
      if (log.isDebugEnabled()) {
        log.debug("Initiated block upload for blobPath '{}'", blobPath);
      }

      try {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        blockBlobClient.deleteIfExists();
      } catch (BlobStorageException e) {
        // ignore; subsequent stage/commit will surface real issues
      }
    }

    void uploadBlock(ByteArrayInputStream inputStream, long blockSize) {
      if (aborted) {
        throw new IllegalStateException(
            "Can't upload new blocks on a BlockUpload that was aborted");
      }

      String blockId =
          Base64.getEncoder()
              .encodeToString(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

      if (log.isDebugEnabled()) {
        log.debug("Uploading block {} for blobPath '{}'", blockId, blobPath);
      }

      try {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        blockBlobClient.stageBlock(blockId, inputStream, blockSize);
        blockIds.add(blockId);
      } catch (BlobStorageException e) {
        throw new RuntimeException("Failed to upload block", e);
      }
    }

    void complete() {
      if (aborted) {
        throw new IllegalStateException("Can't complete a BlockUpload that was aborted");
      }

      if (log.isDebugEnabled()) {
        log.debug("Completing block upload for blobPath '{}'", blobPath);
      }

      try {
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        blockBlobClient.commitBlockList(blockIds);
      } catch (BlobStorageException e) {
        throw new RuntimeException("Failed to commit block list", e);
      }
    }

    public void abort() {
      if (log.isWarnEnabled()) {
        log.warn("Aborting block upload for blobPath '{}'", blobPath);
      }

      aborted = true;
    }
  }
}
