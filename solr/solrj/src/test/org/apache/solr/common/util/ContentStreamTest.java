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
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;

/** Tests {@link ContentStream} such as "stream.file". */
public class ContentStreamTest extends SolrTestCaseJ4 {

  public void testStringStream() throws IOException {
    String input = "aads ghaskdgasgldj asl sadg ajdsg &jag # @ hjsakg hsakdg hjkas s";
    ContentStreamBase stream = new ContentStreamBase.StringStream(input);
    assertEquals(input.length(), stream.getSize().longValue());
    assertEquals(input, new String(stream.getStream().readAllBytes(), StandardCharsets.UTF_8));
    assertEquals(input, StrUtils.stringFromReader(stream.getReader()));
  }

  public void testFileStream() throws IOException {
    Path file = createTempDir().resolve("README");
    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file)) {
      assertNotNull(is);
      is.transferTo(os);
    }

    ContentStreamBase stream = new ContentStreamBase.FileStream(file);
    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        InputStreamReader isr =
            new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8);
        Reader r = stream.getReader()) {
      assertEquals(Files.size(file), stream.getSize().longValue());
      // Test the code that sets content based on < being the 1st character
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(fis, s));
      assertTrue(IOUtils.contentEquals(isr, r));
    }
  }

  public void testFileStreamGZIP() throws IOException {
    Path file = createTempDir().resolve("README.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file);
        GZIPOutputStream zos = new GZIPOutputStream(os)) {
      is.transferTo(zos);
    }

    ContentStreamBase stream = new ContentStreamBase.FileStream(file);
    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        GZIPInputStream zis = new GZIPInputStream(fis);
        InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
        InputStream fis2 = Files.newInputStream(file);
        GZIPInputStream zis2 = new GZIPInputStream(fis2);
        Reader r = stream.getReader()) {
      assertEquals(Files.size(file), stream.getSize().longValue());
      // Test the code that sets content based on < being the 1st character
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
    }
  }

  public void testURLStream() throws IOException {
    Path file = createTempDir().resolve("README");

    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file)) {
      is.transferTo(os);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(file.toUri().toURL());

    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        InputStream fis2 = Files.newInputStream(file);
        InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
        Reader r = stream.getReader()) {
      // For File URLs, the content type is determined automatically by the mime type associated
      // with the file extension. This is inconsistent from the FileStream as that code tries to
      // guess the content based on the 1st character.
      //
      // HTTP URLS, the content type is determined by the headers.  Those are not tested here.
      //
      assertEquals("text/html", stream.getContentType());
      assertTrue(IOUtils.contentEquals(fis2, s));
      assertEquals(Files.size(file), stream.getSize().longValue());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertEquals(Files.size(file), stream.getSize().longValue());
    }
  }

  public void testURLStreamGZIP() throws IOException {
    Path file = createTempDir().resolve("README.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file);
        GZIPOutputStream zos = new GZIPOutputStream(os)) {
      is.transferTo(zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(file.toUri().toURL());
    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        GZIPInputStream zis = new GZIPInputStream(fis);
        InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
        InputStream fis2 = Files.newInputStream(file);
        GZIPInputStream zis2 = new GZIPInputStream(fis2);
        Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("application/xml", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(Files.size(file), stream.getSize().longValue());
    }
  }

  public void testURLStreamCSVGZIPExtension() throws IOException {
    Path file = createTempDir().resolve("README.CSV.gz");

    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file);
        GZIPOutputStream zos = new GZIPOutputStream(os)) {
      is.transferTo(zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(file.toUri().toURL());
    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        GZIPInputStream zis = new GZIPInputStream(fis);
        InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
        InputStream fis2 = Files.newInputStream(file);
        GZIPInputStream zis2 = new GZIPInputStream(fis2);
        Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("text/csv", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(Files.size(file), stream.getSize().longValue());
    }
  }

  public void testURLStreamJSONGZIPExtension() throws IOException {
    Path file = createTempDir().resolve("README.json.gzip");

    try (SolrResourceLoader srl = new SolrResourceLoader(Path.of("").toAbsolutePath());
        InputStream is = srl.openResource("solrj/README");
        OutputStream os = Files.newOutputStream(file);
        GZIPOutputStream zos = new GZIPOutputStream(os)) {
      is.transferTo(zos);
    }

    ContentStreamBase stream = new ContentStreamBase.URLStream(file.toUri().toURL());
    try (InputStream s = stream.getStream();
        InputStream fis = Files.newInputStream(file);
        GZIPInputStream zis = new GZIPInputStream(fis);
        InputStreamReader isr = new InputStreamReader(zis, StandardCharsets.UTF_8);
        InputStream fis2 = Files.newInputStream(file);
        GZIPInputStream zis2 = new GZIPInputStream(fis2);
        Reader r = stream.getReader()) {
      // See the non-GZIP test case for an explanation of header handling.
      assertEquals("application/json", stream.getContentType());
      assertTrue(IOUtils.contentEquals(isr, r));
      assertTrue(IOUtils.contentEquals(zis2, s));
      assertEquals(Files.size(file), stream.getSize().longValue());
    }
  }
}
