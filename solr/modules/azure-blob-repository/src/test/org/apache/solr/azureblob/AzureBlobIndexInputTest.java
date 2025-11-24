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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class AzureBlobIndexInputTest extends AbstractAzureBlobClientTest {

  @Test
  public void testBasicIndexInput() throws Exception {
    String path = "index-input-test.txt";
    String content = "Index input test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
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

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      long seekPosition = content.length() / 2;
      input.seek(seekPosition);

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

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should match", content.length(), input.length());
    }
  }

  @Test
  public void testIndexInputReadByte() throws Exception {
    String path = "index-input-byte-test.txt";
    String content = "Byte read test";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
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

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      byte[] buffer = new byte[10];
      StringBuilder readContent = new StringBuilder();

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

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      input.seek(content.length());
      expectThrows(IOException.class, input::readByte);
    }
  }

  @Test
  public void testIndexInputSeekBeyondEnd() throws Exception {
    String path = "index-input-seek-beyond-test.txt";
    String content = "Seek beyond end test";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      long invalidPosition = content.length() + 1L;
      expectThrows(IOException.class, () -> input.seek(invalidPosition));
    }
  }

  @Test
  public void testIndexInputGetFilePointer() throws Exception {
    String path = "index-input-pointer-test.txt";
    String content = "File pointer test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      assertEquals("Initial position should be 0", 0, input.getFilePointer());

      byte[] buffer = new byte[5];
      input.readBytes(buffer, 0, buffer.length);
      assertEquals("Position should be 5 after reading 5 bytes", 5, input.getFilePointer());

      input.seek(10);
      assertEquals("Position should be 10 after seek", 10, input.getFilePointer());
    }
  }

  @Test
  public void testIndexInputLargeFile() throws Exception {
    String path = "index-input-large-test.txt";
    StringBuilder contentBuilder = new StringBuilder();

    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should match", content.length(), input.length());

      byte[] buffer = new byte[8192];
      StringBuilder readContent = new StringBuilder();
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

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path))) {
      assertEquals("Length should be 0", 0, input.length());
      assertEquals("Position should be 0", 0, input.getFilePointer());
      expectThrows(IOException.class, input::readByte);
    }
  }

  @Test
  public void testIndexInputClose() throws Exception {
    String path = "index-input-close-test.txt";
    String content = "Close test content";

    pushContent(path, content);

    AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path));
    input.close();

    expectThrows(IOException.class, input::readByte);
    expectThrows(IOException.class, () -> input.seek(0));
  }

  @Test
  public void testIndexInputMultipleClose() throws Exception {
    String path = "index-input-multiple-close-test.txt";
    String content = "Multiple close test content";

    pushContent(path, content);

    AzureBlobIndexInput input = new AzureBlobIndexInput(path, client, client.length(path));
    input.close();
    input.close();
  }
}
