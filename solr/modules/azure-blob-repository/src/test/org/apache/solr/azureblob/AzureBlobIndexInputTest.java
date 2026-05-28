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
import java.util.Locale;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.junit.Test;

public class AzureBlobIndexInputTest extends AbstractAzureBlobClientTest {

  /** Sequential read of a small blob via {@code readBytes} returns the full content unchanged. */
  @Test
  public void testBasicIndexInput() throws Exception {
    String path = "index-input-test.txt";
    String content = "Index input test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      byte[] buffer = new byte[1024];
      input.readBytes(buffer, 0, content.length());
      String readContent = new String(buffer, 0, content.length(), StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  /** Forward {@code seek()} into the middle of the blob, then read returns the suffix. */
  @Test
  public void testIndexInputSeek() throws Exception {
    String path = "index-input-seek-test.txt";
    String content = "Index input seek test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      long seekPosition = content.length() / 2;
      input.seek(seekPosition);

      byte[] buffer = new byte[1024];
      String expectedContent = content.substring((int) seekPosition);
      input.readBytes(buffer, 0, expectedContent.length());
      String readContent = new String(buffer, 0, expectedContent.length(), StandardCharsets.UTF_8);
      assertEquals("Content from seek position should match", expectedContent, readContent);
    }
  }

  /** {@code length()} reports the blob's content length. */
  @Test
  public void testIndexInputLength() throws Exception {
    String path = "index-input-length-test.txt";
    String content = "Length test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      assertEquals("Length should match", content.length(), input.length());
    }
  }

  /** Byte-by-byte sequential read via {@code readByte()} reconstructs the original content. */
  @Test
  public void testIndexInputReadByte() throws Exception {
    String path = "index-input-byte-test.txt";
    String content = "Byte read test";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      StringBuilder readContent = new StringBuilder();
      for (int i = 0; i < content.length(); i++) {
        byte b = input.readByte();
        readContent.append((char) b);
      }

      assertEquals("Byte by byte content should match", content, readContent.toString());
    }
  }

  /** Chunked reads with a small buffer cover the whole file across multiple buffer refills. */
  @Test
  public void testIndexInputReadBytes() throws Exception {
    String path = "index-input-bytes-test.txt";
    String content = "Bytes read test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
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

  /** Seeking exactly to {@code length} is allowed; the next {@code readByte()} throws EOF. */
  @Test
  public void testIndexInputSeekToEnd() throws Exception {
    String path = "index-input-seek-end-test.txt";
    String content = "Seek to end test";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      input.seek(content.length());
      expectThrows(IOException.class, input::readByte);
    }
  }

  /** Seeking past {@code length} throws {@link IOException}. */
  @Test
  public void testIndexInputSeekBeyondEnd() throws Exception {
    String path = "index-input-seek-beyond-test.txt";
    String content = "Seek beyond end test";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      long invalidPosition = content.length() + 1L;
      expectThrows(IOException.class, () -> input.seek(invalidPosition));
    }
  }

  /** {@code getFilePointer()} reflects both incremental reads and explicit seeks. */
  @Test
  public void testIndexInputGetFilePointer() throws Exception {
    String path = "index-input-pointer-test.txt";
    String content = "File pointer test content";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      assertEquals("Initial position should be 0", 0, input.getFilePointer());

      byte[] buffer = new byte[5];
      input.readBytes(buffer, 0, buffer.length);
      assertEquals("Position should be 5 after reading 5 bytes", 5, input.getFilePointer());

      input.seek(10);
      assertEquals("Position should be 10 after seek", 10, input.getFilePointer());
    }
  }

  /**
   * Reading a multi-hundred-KB blob in 8 KB chunks exercises the buffer-refill / range-stream
   * draining path end-to-end.
   */
  @Test
  public void testIndexInputLargeFile() throws Exception {
    String path = "index-input-large-test.txt";
    StringBuilder contentBuilder = new StringBuilder();

    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large file.\n");
    }
    String content = contentBuilder.toString();

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
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

  /** On a 0-byte blob: {@code length} and {@code getFilePointer} are 0, and any read throws EOF. */
  @Test
  public void testIndexInputEmptyFile() throws Exception {
    String path = "index-input-empty-test.txt";
    String content = "";

    pushContent(path, content);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      assertEquals("Length should be 0", 0, input.length());
      assertEquals("Position should be 0", 0, input.getFilePointer());
      expectThrows(IOException.class, input::readByte);
    }
  }

  /**
   * After {@code close()}, both {@code readByte} and {@code slice} throw {@link
   * AlreadyClosedException} rather than silently re-opening a fresh stream.
   */
  @Test
  public void testIndexInputClose() throws Exception {
    String path = "index-input-close-test.txt";
    String content = "Close test content";

    pushContent(path, content);

    AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path));
    input.close();
    expectThrows(AlreadyClosedException.class, input::readByte);
    expectThrows(AlreadyClosedException.class, () -> input.slice("after-close", 0L, 1L));
  }

  /** {@code close()} is idempotent: calling it twice does not throw. */
  @Test
  public void testIndexInputMultipleClose() throws Exception {
    String path = "index-input-multiple-close-test.txt";
    String content = "Multiple close test content";

    pushContent(path, content);

    AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path));
    input.close();
    input.close();
  }

  /**
   * Lucene's {@code CodecUtil.retrieveChecksum} and several other codec routines seek backward to
   * positions before the current buffer (e.g. {@code seek(0)} after reading the trailing footer).
   * Verify that an interleaved forward/backward seek pattern returns correct data.
   */
  @Test
  public void testIndexInputBackwardSeek() throws Exception {
    String path = "index-input-backward-seek-test.txt";
    // Content larger than the default buffer so seeks cross buffer boundaries.
    StringBuilder contentBuilder = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      contentBuilder.append(String.format(Locale.ROOT, "line%04d ", i));
    }
    String content = contentBuilder.toString();
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    pushContent(path, contentBytes);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      // Read tail
      long tailLength = 16;
      input.seek(contentBytes.length - tailLength);
      byte[] tail = new byte[(int) tailLength];
      input.readBytes(tail, 0, tail.length);
      byte[] expectedTail = new byte[(int) tailLength];
      System.arraycopy(
          contentBytes, contentBytes.length - (int) tailLength, expectedTail, 0, (int) tailLength);
      assertArrayEquals("Tail should match", expectedTail, tail);

      // Seek back to the start and re-read
      input.seek(0);
      assertEquals("Position should be 0 after backward seek", 0, input.getFilePointer());
      byte[] head = new byte[32];
      input.readBytes(head, 0, head.length);
      byte[] expectedHead = new byte[32];
      System.arraycopy(contentBytes, 0, expectedHead, 0, 32);
      assertArrayEquals("Head bytes after backward seek should match", expectedHead, head);

      // Seek somewhere in the middle, both backward and forward, several times
      int[] offsets = {2000, 100, 4000, 500, 3000};
      byte[] sample = new byte[8];
      for (int off : offsets) {
        input.seek(off);
        input.readBytes(sample, 0, sample.length);
        byte[] expected = new byte[sample.length];
        System.arraycopy(contentBytes, off, expected, 0, sample.length);
        assertArrayEquals("Sample at offset " + off, expected, sample);
      }
    }
  }

  /**
   * Verify that {@code IndexInput.slice(...)} produces an independent view of a portion of the blob
   * with correct length and bytes.
   */
  @Test
  public void testIndexInputSlice() throws Exception {
    String path = "index-input-slice-test.txt";
    String content = "abcdefghijklmnopqrstuvwxyz0123456789";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    pushContent(path, contentBytes);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      long sliceOffset = 10;
      long sliceLength = 20;
      try (IndexInput slice = input.slice("middle", sliceOffset, sliceLength)) {
        assertEquals("Slice length", sliceLength, slice.length());
        assertEquals("Initial pointer", 0, slice.getFilePointer());

        byte[] buf = new byte[(int) sliceLength];
        slice.readBytes(buf, 0, buf.length);
        byte[] expected = new byte[(int) sliceLength];
        System.arraycopy(contentBytes, (int) sliceOffset, expected, 0, (int) sliceLength);
        assertArrayEquals("Slice content", expected, buf);

        // backward seek inside the slice
        slice.seek(0);
        byte first = slice.readByte();
        assertEquals(contentBytes[(int) sliceOffset], first);

        // out-of-bounds slice should throw
        expectThrows(
            IllegalArgumentException.class, () -> input.slice("oob", 0, content.length() + 1L));
        expectThrows(IllegalArgumentException.class, () -> input.slice("neg", -1, 1));
      }
    }
  }

  /**
   * A clone has an independent file pointer (per {@link BufferedIndexInput#clone()}): seeks and
   * reads on the clone do not move the parent's position, and vice versa.
   */
  @Test
  public void testIndexInputCloneIndependent() throws Exception {
    String path = "index-input-clone-test.txt";
    String content = "abcdefghijklmnopqrstuvwxyz0123456789";
    byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

    pushContent(path, contentBytes);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      input.seek(5);
      IndexInput clone = input.clone();
      assertEquals("Clone starts at parent position", 5, clone.getFilePointer());

      // Move clone forward; parent should be unaffected
      clone.seek(20);
      byte fromClone = clone.readByte();
      assertEquals(contentBytes[20], fromClone);
      assertEquals("Parent position unchanged after clone read", 5, input.getFilePointer());

      // Read from parent
      byte fromParent = input.readByte();
      assertEquals(contentBytes[5], fromParent);

      // Clone can also seek backward independently
      clone.seek(0);
      byte cloneFirst = clone.readByte();
      assertEquals(contentBytes[0], cloneFirst);
    }
  }

  /**
   * {@code readByte(long pos)} from arbitrary offsets after the buffer is seeded — exercises
   * backward {@code seekInternal} that crosses the buffered window.
   */
  @Test
  public void testIndexInputRandomAccessReads() throws Exception {
    String path = "index-input-random-access-test.txt";
    // 256 bytes of well-known data: byte at offset i has value (byte)(i & 0xFF)
    byte[] contentBytes = new byte[256];
    for (int i = 0; i < contentBytes.length; i++) {
      contentBytes[i] = (byte) i;
    }
    pushContent(path, contentBytes);

    try (AzureBlobIndexInput input = new AzureBlobIndexInput(client, path, client.length(path))) {
      // Seed the buffer with a forward read first
      input.seek(200);
      input.readByte();

      // Now exercise readByte(pos) backward — this calls seekInternal() with a smaller pos
      assertEquals((byte) 0, input.readByte(0));
      assertEquals((byte) 1, input.readByte(1));
      assertEquals((byte) 100, input.readByte(100));
      assertEquals((byte) 255, input.readByte(255));
    }
  }
}
