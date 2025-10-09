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
package org.apache.solr.blob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class BlobOutputStreamTest extends AbstractBlobClientTest {

  @Test
  public void testBasicOutputStream() throws Exception {
    String path = "output-stream-test.txt";
    String content = "Output stream test content";

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    // Verify content was written
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

    // Verify content was written
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

    // Verify content was written
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

    // Verify content was written
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

      // Verify content is available after flush
      assertTrue("File should exist after flush", client.pathExists(path));
    }
  }

  @Test
  public void testOutputStreamClose() throws Exception {
    String path = "output-stream-close-test.txt";
    String content = "Close test content";

    OutputStream output = client.pushStream(path);
    output.write(content.getBytes(StandardCharsets.UTF_8));
    output.close();

    // Verify content was written
    assertTrue("File should exist after close", client.pathExists(path));

    // Test that operations on closed stream throw exception
    try {
      output.write(1);
      fail("Should throw IOException when writing to closed stream");
    } catch (IOException e) {
      // Expected
    }

    try {
      output.flush();
      fail("Should throw IOException when flushing closed stream");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testOutputStreamMultipleClose() throws Exception {
    String path = "output-stream-multiple-close-test.txt";
    String content = "Multiple close test content";

    OutputStream output = client.pushStream(path);
    output.write(content.getBytes(StandardCharsets.UTF_8));
    output.close();
    output.close(); // Should not throw exception

    // Verify content was written
    assertTrue("File should exist", client.pathExists(path));
  }

  @Test
  public void testOutputStreamLargeData() throws Exception {
    String path = "output-stream-large-test.txt";
    StringBuilder contentBuilder = new StringBuilder();

    // Create large content (2MB)
    for (int i = 0; i < 20000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    try (OutputStream output = client.pushStream(path)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    // Verify content was written
    assertTrue("Large file should exist", client.pathExists(path));
    assertEquals("File length should match", content.length(), client.length(path));

    // Verify content integrity
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
      // Write in small chunks
      int chunkSize = 5;
      for (int i = 0; i < contentBytes.length; i += chunkSize) {
        int remaining = Math.min(chunkSize, contentBytes.length - i);
        output.write(contentBytes, i, remaining);
      }
    }

    // Verify content was written correctly
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

    // Fill with some binary data
    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    try (OutputStream output = client.pushStream(path)) {
      output.write(binaryData);
    }

    // Verify binary data was written
    assertTrue("Binary file should exist", client.pathExists(path));
    assertEquals("Binary file length should match", binaryData.length, client.length(path));

    // Verify binary data integrity
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
