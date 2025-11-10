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

import static org.hamcrest.Matchers.containsString;

import com.carrotsearch.randomizedtesting.generators.RandomBytes;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.common.util.ResumableInputStream;
import org.apache.solr.util.LogListener;
import org.junit.Test;

/** Basic test that write data and read them through the S3 client. */
public class S3ReadWriteTest extends AbstractS3ClientTest {

  /** Write and read a simple file (happy path). */
  @Test
  public void testBasicWriteRead() throws Exception {
    pushContent("/foo", "my blob");

    try (InputStream stream = client.pullStream("/foo")) {
      assertEquals("my blob", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  /** Check writing a file with no path. */
  @Test
  public void testWriteNoPath() {
    assertThrows(
        "Should not be able to write content to empty path",
        S3Exception.class,
        () -> pushContent("", "empty path"));
    assertThrows(
        "Should not be able to write content to root path",
        S3Exception.class,
        () -> pushContent("/", "empty path"));
  }

  /** Check reading a file with no path. */
  @Test
  public void testReadNoPath() {
    assertThrows(
        "Should not be able to read content from empty path",
        S3Exception.class,
        () -> client.pullStream(""));
    assertThrows(
        "Should not be able to read content from empty path",
        S3Exception.class,
        () -> client.pullStream("/"));
  }

  /** Test writing over an existing file and overriding the content. */
  @Test
  public void testWriteOverFile() throws Exception {
    pushContent("/override", "old content");
    pushContent("/override", "new content");

    try (InputStream stream = client.pullStream("/override")) {
      assertEquals(
          "File contents should have been overridden",
          "new content",
          new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  /** Check getting the length of a written file. */
  @Test
  public void testLength() throws Exception {
    pushContent("/foo", "0123456789");
    assertEquals(10, client.length("/foo"));
  }

  /** Check an exception is raised when getting the length of a directory. */
  @Test
  public void testDirectoryLength() throws Exception {
    client.createDirectory("/directory");

    S3Exception exception =
        assertThrows(
            "Getting length on a dir should throw exception",
            S3Exception.class,
            () -> client.length("/directory"));
    assertThat(exception.getMessage(), exception.getMessage(), containsString("Path is Directory"));
  }

  /** Check various method throws the expected exception of a missing S3 key. */
  @Test
  public void testNotFound() {
    assertThrows(S3NotFoundException.class, () -> client.pullStream("/not-found"));
    assertThrows(S3NotFoundException.class, () -> client.length("/not-found"));
  }

  /** Check that a read can succeed even with Connection Loss. */
  @Test
  public void testReadWithConnectionLoss() throws IOException {
    String key = "flush-very-large";

    int numBytes = 20_000_000;
    pushContent(key, RandomBytes.randomBytesOfLength(random(), numBytes));

    int numExceptions = 20;
    int bytesPerException = numBytes / numExceptions;
    // Check we can re-read same content

    int maxBuffer = 100;
    byte[] buffer = new byte[maxBuffer];
    boolean done = false;
    try (LogListener logListener = LogListener.warn(ResumableInputStream.class)) {
      try (InputStream input = client.pullStream(key)) {
        long byteCount = 0;
        while (!done) {
          // Use the same number of bytes no matter which method we are testing
          int numBytesToRead = random().nextInt(maxBuffer) + 1;
          // test both read() and read(buffer, off, len)
          switch (random().nextInt(3)) {
              // read()
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
              // read(byte, off, len)
            case 1:
              {
                int readLen = input.read(buffer, 0, numBytesToRead);
                if (readLen > 0) {
                  byteCount += readLen;
                } else {
                  // We are done when readLen = -1
                  done = true;
                }
              }
              break;
              // skip(len)
            case 2:
              {
                // We only want to skip 1 because
                long bytesSkipped = input.skip(numBytesToRead);
                byteCount += bytesSkipped;
                if (bytesSkipped < numBytesToRead) {
                  // We are done when no bytes are skipped
                  done = true;
                }
              }
              break;
          }
          // Initiate a connection loss at the beginning of every "bytesPerException" cycle.
          // The input stream will not immediately see an error, it will have pre-loaded some data.
          if ((byteCount % bytesPerException <= maxBuffer)) {
            initiateS3ConnectionLoss();
          }
        }
        assertEquals("Wrong amount of data found from InputStream", numBytes, byteCount);
      }
      // We just need to ensure we saw at least one IOException
      assertNotEquals(
          "There was no logging of an IOException that caused the InputStream to be resumed",
          0,
          logListener.getCount());
      // LogListener will fail because we haven't polled for each warning.
      // Just clear the queue instead, we only care that the queue is not empty.
      logListener.clearQueue();
    }
  }
}
