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
package org.apache.solr.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;

@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class
    })
public class S3OutputStreamTest extends SolrTestCaseJ4 {

  private static final String BUCKET = S3OutputStreamTest.class.getSimpleName();

  @ClassRule
  public static final S3MockContainerRule s3MockContainer = new S3MockContainerRule(BUCKET);

  private S3Client s3;

  @Before
  public void setUpClient() {
    s3 = s3MockContainer.createS3ClientV2();
  }

  @After
  public void tearDownClient() {
    s3.close();
  }

  /**
   * Basic check writing content byte-by-byte. They should be kept in the internal buffer and
   * flushed to S3 only once.
   */
  @Test
  public void testWriteByteByByte() throws IOException {
    try (S3OutputStream output = new S3OutputStream(s3, "byte-by-byte", BUCKET)) {
      output.write('h');
      output.write('e');
      output.write('l');
      output.write('l');
      output.write('o');
    }

    // Check we can re-read same content
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key("byte-by-byte"),
            ResponseTransformer.toInputStream(Duration.ZERO))) {
      String read = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("Contents saved to S3 file did not match expected", "hello", read);
    }
  }

  /** Write a small byte array, which is smaller than S3 part size. */
  @Test
  public void testWriteSmallBuffer() throws IOException {
    // must be smaller than S3 part size
    byte[] buffer = "hello".getBytes(StandardCharsets.UTF_8);
    // pre-check -- ensure that our test string isn't too big
    assertTrue(buffer.length < S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "small-buffer", BUCKET)) {
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key("small-buffer"),
            ResponseTransformer.toInputStream(Duration.ZERO))) {
      String read = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("hello", read);
    }
  }

  /** Write a byte array larger than S3 part size. Simulate a real multipart upload. */
  @Test
  public void testWriteLargeBuffer() throws IOException {
    // must be larger than S3 part size
    String content =
        RandomStrings.randomAsciiAlphanumOfLength(random(), S3OutputStream.PART_SIZE + 1024);
    byte[] buffer = content.getBytes(StandardCharsets.UTF_8);
    // pre-check -- ensure that our test string isn't too small
    assertTrue(buffer.length > S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "large-buffer", BUCKET)) {
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key("large-buffer"),
            ResponseTransformer.toInputStream(Duration.ZERO))) {
      String read = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(new String(buffer, StandardCharsets.UTF_8), read);
    }
  }

  /** Check flush is a no-op if data size is lower than required size of S3 part. */
  @Test
  public void testFlushSmallBuffer() throws IOException {
    // must be smaller than S3 minimal part size, so flush is a no-op
    byte[] buffer = "hello".getBytes(StandardCharsets.UTF_8);
    assertTrue(buffer.length < S3OutputStream.PART_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-small", BUCKET)) {
      output.write(buffer);
      output.flush();

      buffer = ", world!".getBytes(StandardCharsets.UTF_8);
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key("flush-small"),
            ResponseTransformer.toInputStream(Duration.ZERO))) {
      String read = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(
          "Flushing a small frame of an S3OutputStream should not impact data written",
          "hello, world!",
          read);
    }
  }

  /**
   * Check flush is happening when data in buffer is larger than S3 minimal part size. The buffered
   * size is a multiple of the cipher block size, so flush must hold back a whole block.
   */
  @Test
  public void testFlushLargeBuffer() throws IOException {
    // must be larger than S3 minimal part size, so we actually do something for flush()
    String content =
        RandomStrings.randomAsciiAlphanumOfLength(random(), S3OutputStream.MIN_PART_SIZE + 1024);
    byte[] buffer = content.getBytes(StandardCharsets.UTF_8);
    assertTrue(buffer.length > S3OutputStream.MIN_PART_SIZE);
    assertEquals(0, buffer.length % S3OutputStream.CIPHER_BLOCK_SIZE);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-large", BUCKET)) {
      output.write(buffer);
      output.flush();

      buffer = "some more".getBytes(StandardCharsets.UTF_8);
      output.write(buffer);
    }

    // Check we can re-read same content
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key("flush-large"),
            ResponseTransformer.toInputStream(Duration.ZERO))) {
      String read = new String(input.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(
          "Flushing a large frame of an S3OutputStream should not impact data written",
          content + "some more",
          read);
    }
  }

  @Test
  public void testWriteExactPartSizeByteByByte() throws IOException {
    assertRoundTrip("exact-part-bytes", S3OutputStream.PART_SIZE, true);
  }

  @Test
  public void testWriteExactPartSizeArray() throws IOException {
    assertRoundTrip("exact-part-array", S3OutputStream.PART_SIZE, false);
  }

  @Test
  public void testWritePartSizePlusOneByteByByte() throws IOException {
    assertRoundTrip("part-plus-one-bytes", S3OutputStream.PART_SIZE + 1, true);
  }

  @Test
  public void testWritePartSizePlusOneArray() throws IOException {
    assertRoundTrip("part-plus-one-array", S3OutputStream.PART_SIZE + 1, false);
  }

  @Test
  public void testWriteTwoPartsByteByByte() throws IOException {
    assertRoundTrip("two-parts-bytes", 2 * S3OutputStream.PART_SIZE, true);
  }

  @Test
  public void testWriteTwoPartsArray() throws IOException {
    assertRoundTrip("two-parts-array", 2 * S3OutputStream.PART_SIZE, false);
  }

  /** Flush with a buffered size that is not a multiple of the cipher block size. */
  @Test
  public void testFlushUnalignedBuffer() throws IOException {
    byte[] content = randomBytes(S3OutputStream.MIN_PART_SIZE + 1000);
    assertNotEquals(0, content.length % S3OutputStream.CIPHER_BLOCK_SIZE);
    byte[] more = randomBytes(1000);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-unaligned", BUCKET)) {
      output.write(content);
      output.flush();
      output.write(more);
    }

    assertArrayEquals(concat(content, more), readObject("flush-unaligned"));
  }

  /** Close directly after a flush that uploaded a part: the held-back bytes form the last part. */
  @Test
  public void testFlushThenClose() throws IOException {
    byte[] content = randomBytes(S3OutputStream.MIN_PART_SIZE + 1024);

    try (S3OutputStream output = new S3OutputStream(s3, "flush-close", BUCKET)) {
      output.write(content);
      output.flush();
    }

    assertArrayEquals(content, readObject("flush-close"));
  }

  private void assertRoundTrip(String key, int size, boolean byteByByte) throws IOException {
    byte[] content = randomBytes(size);
    try (S3OutputStream output = new S3OutputStream(s3, key, BUCKET)) {
      if (byteByByte) {
        for (byte b : content) {
          output.write(b);
        }
      } else {
        output.write(content);
      }
    }
    assertArrayEquals(content, readObject(key));
  }

  private byte[] readObject(String key) throws IOException {
    try (InputStream input =
        s3.getObject(
            b -> b.bucket(BUCKET).key(key), ResponseTransformer.toInputStream(Duration.ZERO))) {
      return input.readAllBytes();
    }
  }

  private static byte[] randomBytes(int size) {
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);
    return bytes;
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] result = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
}
