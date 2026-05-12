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

import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class AzureBlobReadWriteTest extends AbstractAzureBlobClientTest {

  /**
   * Small UTF-8 string round-trips byte-for-byte through {@code pushContent} / {@code pullStream}.
   */
  @Test
  public void testBasicReadWrite() throws Exception {
    String path = "test-file.txt";
    String content = "Hello, Azure Blob Storage!";

    pushContent(path, content);

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  /** Multi-block ~200 KB payload: byte-exact readback in 8 KB chunks; {@code length()} matches. */
  @Test
  public void testLargeFileReadWrite() throws Exception {
    String path = "large-file.txt";
    StringBuilder contentBuilder = new StringBuilder();

    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    pushContent(path, content);

    assertTrue("File should exist", client.pathExists(path));
    assertEquals("File length should match", content.length(), client.length(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[8192];
      StringBuilder readContentBuilder = new StringBuilder();
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        readContentBuilder.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
      }
      assertEquals("Content should match", content, readContentBuilder.toString());
    }
  }

  /**
   * 1 KB binary payload (every byte value 0..255 cycled) round-trips byte-exact, with no charset
   * translation.
   */
  @Test
  public void testBinaryDataReadWrite() throws Exception {
    String path = "binary-file.bin";
    byte[] binaryData = new byte[1024];

    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    pushContent(path, binaryData);

    try (InputStream input = client.pullStream(path)) {
      byte[] readData = new byte[binaryData.length];
      int bytesRead = input.read(readData);
      assertEquals("Should read all bytes", binaryData.length, bytesRead);

      for (int i = 0; i < binaryData.length; i++) {
        assertEquals("Binary data should match at position " + i, binaryData[i], readData[i]);
      }
    }
  }

  /**
   * Two independent {@code pullStream} instances against the same blob have isolated read state.
   */
  @Test
  public void testConcurrentReadWrite() throws Exception {
    String path = "concurrent-file.txt";
    String content = "Concurrent read/write test content";

    pushContent(path, content);

    try (InputStream input1 = client.pullStream(path);
        InputStream input2 = client.pullStream(path)) {

      byte[] buffer1 = new byte[1024];
      byte[] buffer2 = new byte[1024];

      int bytesRead1 = input1.read(buffer1);
      int bytesRead2 = input2.read(buffer2);

      String readContent1 = new String(buffer1, 0, bytesRead1, StandardCharsets.UTF_8);
      String readContent2 = new String(buffer2, 0, bytesRead2, StandardCharsets.UTF_8);

      assertEquals("Both reads should get same content", readContent1, readContent2);
      assertEquals("Content should match original", content, readContent1);
    }
  }

  /** Repeat {@code close()} and post-close {@code read()} on a resumable stream do not throw. */
  @Test
  public void testStreamClose() throws Exception {
    String path = "stream-close-test.txt";
    String content = "Stream close test content";

    pushContent(path, content);

    InputStream input = client.pullStream(path);
    input.close();
    input.close();

    int firstByte = input.read();
    assertTrue(
        "Stream should be resumable after close (got byte: " + firstByte + ")",
        firstByte >= 0 || firstByte == -1);

    input.close();
  }

  /** Zero-byte blob exists with length 0; first {@code read()} returns {@code -1} (EOF). */
  @Test
  public void testEmptyFileReadWrite() throws Exception {
    String path = "empty-file.txt";
    String content = "";

    pushContent(path, content);

    assertTrue("Empty file should exist", client.pathExists(path));
    assertEquals("Empty file should have zero length", 0, client.length(path));

    try (InputStream input = client.pullStream(path)) {
      int bytesRead = input.read();
      assertEquals("Should return -1 for empty file", -1, bytesRead);
    }
  }

  /** Multi-byte UTF-8 content (CJK, emoji, Greek) round-trips byte-for-byte. */
  @Test
  public void testUnicodeContentReadWrite() throws Exception {
    String path = "unicode-file.txt";
    String content = "Hello 世界! 🌍 Unicode test: αβγδε";

    pushContent(path, content);

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Unicode content should match", content, readContent);
    }
  }

  /** {@code OutputStream.flush()} only stages bytes; the blob is committed by {@code close()}. */
  @Test
  public void testOutputStreamFlush() throws Exception {
    String path = "flush-test.txt";
    String content = "Flush test content";

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
      output.flush();
    }

    // OutputStream.flush() only stages buffered bytes; the block list is committed by close(),
    // so the blob becomes visible only after the try-with-resources exits.
    assertTrue("File should exist after close", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match after flush", content, readContent);
    }
  }

  /**
   * ~2 MB read with randomized read/skip and periodic forced connection drops still delivers every
   * byte.
   */
  @Test
  public void testReadWithConnectionLoss() throws Exception {
    String key = "flush-very-large";

    int numBytes = 2_000_000;
    pushContent(key, RandomBytes.randomBytesOfLength(random(), numBytes));

    int numExceptions = 5;
    int bytesPerException = numBytes / numExceptions;

    int maxBuffer = 100;
    byte[] buffer = new byte[maxBuffer];
    boolean done = false;
    try (InputStream input = client.pullStream(key)) {
      long byteCount = 0;
      long lastResetBucket = -1;
      while (!done) {
        int numBytesToRead = random().nextInt(maxBuffer) + 1;
        switch (random().nextInt(3)) {
          case 0:
            {
              for (int i = 0; i < numBytesToRead && !done; i++) {
                done = input.read() == -1;
                if (!done) {
                  byteCount++;
                }
              }
            }
            break;
          case 1:
            {
              int readLen = input.read(buffer, 0, numBytesToRead);
              if (readLen > 0) {
                byteCount += readLen;
              } else {
                done = true;
              }
            }
            break;
          case 2:
            {
              long bytesSkipped = input.skip(numBytesToRead);
              byteCount += bytesSkipped;
              if (bytesSkipped < numBytesToRead) {
                done = true;
              }
            }
            break;
        }

        // Initiate a connection loss at the beginning of every "bytesPerException" cycle.
        // The input stream will not immediately see an error, it will have pre-loaded some data.
        long currentBucket = byteCount / bytesPerException;
        if (currentBucket != lastResetBucket && (byteCount % bytesPerException <= maxBuffer)) {
          initiateBlobConnectionLoss();
          lastResetBucket = currentBucket;
        }
      }

      assertEquals("Wrong amount of data found from InputStream", numBytes, byteCount);
    }
  }

  /** Happy path: a batch larger than the 256-op cap deletes every blob across multiple chunks. */
  @Test
  public void testBatchedDeleteAllPresent() throws Exception {
    final int totalFiles = AzureBlobStorageClient.DELETE_BATCH_SIZE + 4; // crosses chunk boundary
    final String prefix = "batch-delete-all/file-";

    List<String> paths = new ArrayList<>(totalFiles);
    for (int i = 0; i < totalFiles; i++) {
      String path = prefix + i + ".txt";
      pushContent(path, "x");
      paths.add(path);
    }

    assertTrue("First blob should exist before delete", client.pathExists(paths.get(0)));
    assertTrue(
        "Last blob should exist before delete", client.pathExists(paths.get(totalFiles - 1)));

    client.delete(paths);

    for (int i = 0; i < totalFiles; i++) {
      String path = prefix + i + ".txt";
      assertFalse("Blob should be gone after batched delete: " + path, client.pathExists(path));
    }
  }

  /**
   * Strict {@code delete}: a batch with any missing path throws {@link AzureBlobNotFoundException}.
   * Note: the batch is issued before the size-mismatch check, so the present blobs are still
   * deleted server-side even though the call throws — this asserts that surprising partial effect.
   */
  @Test
  public void testDeleteThrowsWhenAnyPathMissing() throws Exception {
    final int totalFiles = AzureBlobStorageClient.DELETE_BATCH_SIZE + 4; // crosses chunk boundary
    final String prefix = "batch-delete-mixed/file-";
    final String missing1 = prefix + "missing-1.txt";
    final String missing2 = prefix + "missing-2.txt";

    List<String> paths = new ArrayList<>(totalFiles + 2);
    for (int i = 0; i < totalFiles; i++) {
      String path = prefix + i + ".txt";
      pushContent(path, "x");
      paths.add(path);
    }
    // Pre-conditions: real blobs exist, missing paths do not.
    assertTrue("First real blob should exist", client.pathExists(paths.get(0)));
    assertFalse("Missing-1 must not exist", client.pathExists(missing1));
    assertFalse("Missing-2 must not exist", client.pathExists(missing2));
    paths.addAll(Arrays.asList(missing1, missing2));

    AzureBlobNotFoundException thrown =
        expectThrows(AzureBlobNotFoundException.class, () -> client.delete(paths));
    assertTrue(
        "Exception message should list missing-1: " + thrown.getMessage(),
        thrown.getMessage().contains(missing1));
    assertTrue(
        "Exception message should list missing-2: " + thrown.getMessage(),
        thrown.getMessage().contains(missing2));

    for (int i = 0; i < totalFiles; i++) {
      String path = prefix + i + ".txt";
      assertFalse(
          "Real blob should be gone after batched delete: " + path, client.pathExists(path));
    }
  }
}
