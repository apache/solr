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
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class BlobIndexInputTest extends AbstractBlobClientTest {

  @Test
  public void testBasicIndexInput() throws Exception {
    String path = "index-input-test.txt";
    String content = "Index input test content";

    // Write content
    pushContent(path, content);

    // Read using BlobIndexInput
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      byte[] buffer = new byte[1024];
      input.readBytes(buffer, 0, content.length());
      String readContent = new String(buffer, 0, content.length(), StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  @Test
  public void testIndexInputSeek() throws Exception {
    String path = "index-input-seek-test.txt";
    String content = "Index input seek test content";

    // Write content
    pushContent(path, content);

    // Test seeking
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      // Seek to middle of content
      long seekPosition = content.length() / 2;
      input.seek(seekPosition);

      // Read remaining content
      byte[] buffer = new byte[1024];
      String expectedContent = content.substring((int) seekPosition);
      input.readBytes(buffer, 0, expectedContent.length());
      String readContent = new String(buffer, 0, expectedContent.length(), StandardCharsets.UTF_8);
      assertEquals("Content from seek position should match", expectedContent, readContent);
    }
  }

  @Test
  public void testIndexInputLength() throws Exception {
    String path = "index-input-length-test.txt";
    String content = "Length test content";

    // Write content
    pushContent(path, content);

    // Test length
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should match", content.length(), input.length());
    }
  }

  @Test
  public void testIndexInputReadByte() throws Exception {
    String path = "index-input-byte-test.txt";
    String content = "Byte read test";

    // Write content
    pushContent(path, content);

    // Test reading byte by byte
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      StringBuilder readContent = new StringBuilder();
      for (int i = 0; i < content.length(); i++) {
        byte b = input.readByte();
        readContent.append((char) b);
      }
      assertEquals("Byte by byte content should match", content, readContent.toString());
    }
  }

  @Test
  public void testIndexInputReadBytes() throws Exception {
    String path = "index-input-bytes-test.txt";
    String content = "Bytes read test content";

    // Write content
    pushContent(path, content);

    // Test reading bytes
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      byte[] buffer = new byte[10];
      StringBuilder readContent = new StringBuilder();

      // Read all content in chunks
      long remaining = input.length();
      while (remaining > 0) {
        int toRead = (int) Math.min(buffer.length, remaining);
        input.readBytes(buffer, 0, toRead);
        readContent.append(new String(buffer, 0, toRead, StandardCharsets.UTF_8));
        remaining -= toRead;
      }

      assertEquals("Bytes content should match", content, readContent.toString());
    }
  }

  @Test
  public void testIndexInputSeekToEnd() throws Exception {
    String path = "index-input-seek-end-test.txt";
    String content = "Seek to end test";

    // Write content
    pushContent(path, content);

    // Test seeking to end
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      input.seek(content.length());

      // Should be at end, no more bytes to read
      try {
        input.readByte();
        fail("Should throw EOFException when reading past end");
      } catch (IOException e) {
        // Expected
      }
    }
  }

  @Test
  public void testIndexInputSeekBeyondEnd() throws Exception {
    String path = "index-input-seek-beyond-test.txt";
    String content = "Seek beyond end test";

    // Write content
    pushContent(path, content);

    // Test seeking beyond end
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      try {
        input.seek(content.length() + 1);
        fail("Should throw IOException when seeking beyond end");
      } catch (IOException e) {
        // Expected
      }
    }
  }

  @Test
  public void testIndexInputGetFilePointer() throws Exception {
    String path = "index-input-pointer-test.txt";
    String content = "File pointer test content";

    // Write content
    pushContent(path, content);

    // Test file pointer
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      assertEquals("Initial position should be 0", 0, input.getFilePointer());

      // Read some bytes
      byte[] buffer = new byte[5];
      input.readBytes(buffer, 0, buffer.length);
      assertEquals("Position should be 5 after reading 5 bytes", 5, input.getFilePointer());

      // Seek to different position
      input.seek(10);
      assertEquals("Position should be 10 after seek", 10, input.getFilePointer());
    }
  }

  @Test
  public void testIndexInputLargeFile() throws Exception {
    String path = "index-input-large-test.txt";
    StringBuilder contentBuilder = new StringBuilder();

    // Create large content (1MB)
    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    // Write content
    pushContent(path, content);

    // Test reading large file
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should match", content.length(), input.length());

      // Read in chunks
      byte[] buffer = new byte[8192];
      StringBuilder readContent = new StringBuilder();

      // Read all content in chunks
      long remaining = input.length();
      while (remaining > 0) {
        int toRead = (int) Math.min(buffer.length, remaining);
        input.readBytes(buffer, 0, toRead);
        readContent.append(new String(buffer, 0, toRead, StandardCharsets.UTF_8));
        remaining -= toRead;
      }

      assertEquals("Large content should match", content, readContent.toString());
    }
  }

  @Test
  public void testIndexInputEmptyFile() throws Exception {
    String path = "index-input-empty-test.txt";
    String content = "";

    // Write empty content
    pushContent(path, content);

    // Test reading empty file
    try (BlobIndexInput input = new BlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should be 0", 0, input.length());
      assertEquals("Position should be 0", 0, input.getFilePointer());

      // Should be at end immediately
      try {
        input.readByte();
        fail("Should throw EOFException when reading from empty file");
      } catch (IOException e) {
        // Expected
      }
    }
  }

  @Test
  public void testIndexInputClose() throws Exception {
    String path = "index-input-close-test.txt";
    String content = "Close test content";

    // Write content
    pushContent(path, content);

    // Test closing
    BlobIndexInput input = new BlobIndexInput(path, client, client.length(path));
    input.close();

    // Test that operations on closed input throw exception
    try {
      input.readByte();
      fail("Should throw IOException when reading from closed input");
    } catch (IOException e) {
      // Expected
    }

    try {
      input.seek(0);
      fail("Should throw IOException when seeking on closed input");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testIndexInputMultipleClose() throws Exception {
    String path = "index-input-multiple-close-test.txt";
    String content = "Multiple close test content";

    // Write content
    pushContent(path, content);

    // Test multiple close calls
    BlobIndexInput input = new BlobIndexInput(path, client, client.length(path));
    input.close();
    input.close(); // Should not throw exception
  }
}
