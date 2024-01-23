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
package org.apache.solr.zero.util;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/** Test for Java IO stream reading the content of a Lucene index. */
public class IndexInputStreamTest extends SolrTestCaseJ4 {

  /**
   * Check the ability of {@link InputStream#mark(int) marking} and {@link InputStream#reset()
   * resetting} input streams that we use to push to Zero store.
   */
  @Test
  public void testInputStreamMarkReset() throws Exception {

    String content = RandomStrings.randomAsciiAlphanumOfLengthBetween(random(), 1000, 2000);

    // create a file that we will read with reset-able InputStream
    Path root = createTempDir();
    File myFile = root.resolve("my-file").toFile();
    try (Writer writer = new FileWriter(myFile, Charset.defaultCharset())) {
      writer.write(content);
    }

    try (Directory directory = new NIOFSDirectory(root);
        IndexInput input = directory.openInput("my-file", IOContext.READONCE)) {

      InputStream stream = new IndexInputStream(input);
      assertTrue(stream.markSupported());

      // Make sure we read what we are supposed to read
      String chunk1 = readString(stream, 100);
      String chunk2 = readString(stream, 200);
      assertEquals(content.substring(0, 100), chunk1);
      assertEquals(content.substring(100, 300), chunk2);

      // Now mark the stream at current position, then reset it to this position and read
      // again. We are supposed to read same content twice
      stream.mark(200);
      String chunk3 = readString(stream, 100);
      String chunk4 = readString(stream, 200);
      stream.reset();
      String chunk3again = readString(stream, 100);
      String chunk4again = readString(stream, 200);

      assertEquals(content.substring(300, 400), chunk3);
      assertEquals(content.substring(300, 400), chunk3again);
      assertEquals(content.substring(400, 600), chunk4);
      assertEquals(content.substring(400, 600), chunk4again);

      // we read 2 chunks of 100 bytes and 2 chunks of 200 bytes
      assertEquals(content.length() - 600, stream.available());
    }
  }

  private static String readString(InputStream input, int length) throws IOException {
    // Just assume we have one byte per character
    // That's the case here since we only have alpha-num characters
    byte[] buffer = new byte[length];

    int read = input.read(buffer);
    assertEquals(length, read);
    return new String(buffer, Charset.defaultCharset());
  }
}
