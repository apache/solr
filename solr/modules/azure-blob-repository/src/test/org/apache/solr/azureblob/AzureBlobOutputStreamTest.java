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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

public class AzureBlobOutputStreamTest extends AbstractAzureBlobClientTest {

  @Test
  public void testBasicOutputStream() throws Exception {
    String path = "output-stream-test.txt";
    String content = "Output stream test content";

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertTrue("File should exist", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  @Test
  public void testOutputStreamWriteByte() throws Exception {
    String path = "output-stream-byte-test.txt";
    String content = "Byte by byte write test";

    try (OutputStream output = client.pushStream(path)) {
      for (byte b : content.getBytes(StandardCharsets.UTF_8)) {
        output.write(b);
      }
    }

    assertTrue("File should exist", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  @Test
  public void testOutputStreamWriteByteArray() throws Exception {
    String path = "output-stream-array-test.txt";
    String content = "Byte array write test";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    try (OutputStream output = client.pushStream(path)) {
      output.write(contentBytes);
    }

    assertTrue("File should exist", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  @Test
  public void testOutputStreamWriteByteArrayWithOffset() throws Exception {
    String path = "output-stream-offset-test.txt";
    String fullContent = "Full content for offset test";
    String partialContent = "offset test"; // Last part
    byte[] fullBytes = fullContent.getBytes(StandardCharsets.UTF_8);
    int offset = fullContent.indexOf(partialContent);

    try (OutputStream output = client.pushStream(path)) {
      output.write(fullBytes, offset, partialContent.length());
    }

    assertTrue("File should exist", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content should match", partialContent, readContent);
    }
  }

  @Test
  public void testOutputStreamFlush() throws Exception {
    String path = "output-stream-flush-test.txt";
    String content = "Flush test content";

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
      output.flush();
      // flush() is a no-op: nothing is staged or committed until close(), so the blob is not yet
      // visible.
      assertFalse(
          "File should not be visible after flush(); commit happens on close()",
          client.pathExists(path));
    }

    assertTrue("File should exist after close", client.pathExists(path));
    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Content written before flush should be preserved", content, readContent);
    }
  }

  @Test
  public void testFlushDoesNotStageBlocks() throws Exception {
    String path = "output-stream-flush-noop-test.bin";

    // Write several sub-block records, flushing after each. Because flush() is a no-op, all
    // records accumulate in a single buffer and are staged as ONE block on close() rather than one
    // tiny block per flush (which could otherwise exhaust Azure's 50,000-committed-block limit).
    int records = 50;
    byte[] record = new byte[1024];
    Arrays.fill(record, (byte) 'x');

    try (OutputStream output = client.pushStream(path)) {
      for (int i = 0; i < records; i++) {
        output.write(record);
        output.flush();
      }
    }

    assertTrue("File should exist after close", client.pathExists(path));
    assertEquals(
        "Length should match total bytes written",
        (long) records * record.length,
        client.length(path));

    BlobServiceClient serviceClient =
        new BlobServiceClientBuilder().connectionString(getConnectionString()).buildClient();
    BlockBlobClient blockBlobClient =
        serviceClient
            .getBlobContainerClient(containerName)
            .getBlobClient(path)
            .getBlockBlobClient();
    BlockList blockList = blockBlobClient.listBlocks(BlockListType.COMMITTED);
    assertEquals(
        "flush() must not stage extra blocks; sub-block writes commit as a single block",
        1,
        blockList.getCommittedBlocks().size());
  }

  @Test
  public void testOutputStreamClose() throws Exception {
    String path = "output-stream-close-test.txt";
    String content = "Close test content";

    OutputStream output = client.pushStream(path);
    output.write(content.getBytes(StandardCharsets.UTF_8));
    output.close();

    assertTrue("File should exist after close", client.pathExists(path));

    OutputStream closedOutput = output;
    expectThrows(IOException.class, () -> closedOutput.write(1));
    expectThrows(IOException.class, () -> closedOutput.flush());
  }

  @Test
  public void testOutputStreamMultipleClose() throws Exception {
    String path = "output-stream-multiple-close-test.txt";
    String content = "Multiple close test content";

    OutputStream output = client.pushStream(path);
    output.write(content.getBytes(StandardCharsets.UTF_8));
    output.close();
    output.close();

    assertTrue("File should exist", client.pathExists(path));
  }

  @Test
  public void testOutputStreamLargeData() throws Exception {
    String path = "output-stream-large-test.txt";
    StringBuilder contentBuilder = new StringBuilder();

    for (int i = 0; i < 20000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertTrue("Large file should exist", client.pathExists(path));
    assertEquals("File length should match", content.length(), client.length(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[8192];
      StringBuilder readContentBuilder = new StringBuilder();
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        readContentBuilder.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
      }
      assertEquals("Large content should match", content, readContentBuilder.toString());
    }
  }

  @Test
  public void testOutputStreamChunkedWrite() throws Exception {
    String path = "output-stream-chunked-test.txt";
    String content = "Chunked write test content";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    try (OutputStream output = client.pushStream(path)) {
      int chunkSize = 5;
      for (int i = 0; i < contentBytes.length; i += chunkSize) {
        int remaining = Math.min(chunkSize, contentBytes.length - i);
        output.write(contentBytes, i, remaining);
      }
    }

    assertTrue("File should exist", client.pathExists(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String readContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Chunked content should match", content, readContent);
    }
  }

  @Test
  public void testOutputStreamBinaryData() throws Exception {
    String path = "output-stream-binary-test.bin";
    byte[] binaryData = new byte[1024];

    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    try (OutputStream output = client.pushStream(path)) {
      output.write(binaryData);
    }

    assertTrue("Binary file should exist", client.pathExists(path));
    assertEquals("Binary file length should match", binaryData.length, client.length(path));

    try (InputStream input = client.pullStream(path)) {
      byte[] readData = new byte[binaryData.length];
      int bytesRead = input.read(readData);
      assertEquals("Should read all bytes", binaryData.length, bytesRead);

      for (int i = 0; i < binaryData.length; i++) {
        assertEquals("Binary data should match at position " + i, binaryData[i], readData[i]);
      }
    }
  }
}
