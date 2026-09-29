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
package org.apache.solr.handler.extraction;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.extraction.fromtika.ToXMLContentHandler;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for TikaServerExtractionBackend using a real Tika Server via Testcontainers.
 */
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      TikaServerExtractionBackendTest.TestcontainersThreadsFilter.class
    })
public class TikaServerExtractionBackendTest extends SolrTestCaseJ4 {

  // Ignore known non-daemon threads spawned by Testcontainers and Java HttpClient in this test
  @SuppressWarnings("NewClassNamingConvention")
  public static class TestcontainersThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      if (t == null || t.getName() == null) return false;
      String n = t.getName();
      return n.startsWith("testcontainers-ryuk")
          || n.startsWith("testcontainers-wait-")
          || n.startsWith("HttpClient-")
          || n.startsWith("HttpClient-TestContainers");
    }
  }

  @ClassRule
  public static final TikaServerContainerRule tikaContainer = new TikaServerContainerRule();

  private static ExtractionRequest newRequest(
      String resourceName,
      String contentType,
      String extractFormat,
      boolean recursive,
      Map<String, String> tikaRequestHeaders) {
    return ExtractionRequest.builder()
        .streamType(contentType)
        .resourceName(resourceName)
        .contentType(contentType)
        .streamName(resourceName)
        .extractFormat(extractFormat)
        .tikaServerRecursive(recursive)
        .tikaServerRequestHeaders(tikaRequestHeaders)
        .build();
  }

  @Test
  public void testExtractTextAndMetadata() throws Exception {
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl())) {
      byte[] data = "Hello TestContainers".getBytes(StandardCharsets.UTF_8);
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ExtractionResult res = backend.extract(in, newRequest("test.txt", "text/plain", "text"));
        assertNotNull(res);
        assertNotNull(res.getContent());
        assertTrue(res.getContent().contains("Hello TestContainers"));
        assertNotNull(res.getMetadata());
        List<String> cts = res.getMetadata().get("Content-Type");
        assertNotNull(cts);
        assertFalse(cts.isEmpty());
        // Tika may append charset; be flexible
        assertTrue(cts.getFirst().startsWith("text/plain"));
      }
    }
  }

  @Test
  public void testLegacyFieldNamesMigratesTika4KeysToTika3Names() throws Exception {
    byte[] data = "Hello TestContainers".getBytes(StandardCharsets.UTF_8);

    // First, extract without the flag to capture the Tika 4.x key names/values as a baseline.
    ExtractionMetadata tika4Metadata;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl())) {
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        tika4Metadata =
            backend.extract(in, newRequest("test.txt", "text/plain", "text")).getMetadata();
      }
    }
    assertNotNull(tika4Metadata.getFirst("tk:parsed-by"));

    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add(ExtractingParams.TIKASERVER_LEGACY_FIELD_NAMES, "true");
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(
            tikaContainer.getBaseUrl(),
            180,
            initArgs,
            TikaServerExtractionBackend.DEFAULT_MAXCHARS_LIMIT)) {
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ExtractionMetadata md =
            backend.extract(in, newRequest("test.txt", "text/plain", "text")).getMetadata();
        // The Tika 4.x key is gone, replaced by its Tika 3.x equivalent with the same value.
        assertNull(md.getFirst("tk:parsed-by"));
        assertEquals(tika4Metadata.getFirst("tk:parsed-by"), md.getFirst("X-TIKA:Parsed-By"));
      }
    }
  }

  @Test
  public void testExtractWithSaxHandlerXml() throws Exception {
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl())) {
      byte[] data = "Hello XML".getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request = newRequest("test.txt", "text/plain", "xml");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ToXMLContentHandler xmlHandler = new ToXMLContentHandler();
        ExtractionMetadata md = backend.buildMetadataFromRequest(request);
        backend.extractWithSaxHandler(in, request, md, xmlHandler);
        String c = xmlHandler.toString();
        assertNotNull(c);
        // Tika Server may return XHTML without XML declaration; be flexible
        assertTrue(
            c.contains("<?xml")
                || c.toLowerCase(Locale.ROOT).contains("<html")
                || c.toLowerCase(Locale.ROOT).contains("<xhtml"));
        assertTrue(c.contains("Hello XML"));
      }
    }
  }

  @Test
  public void testPdfWithImageRecursive() throws Exception {
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl())) {
      byte[] data = Files.readAllBytes(getFile("extraction/pdf-with-image.pdf"));
      // TikaServer 4.x's /rmeta OCRs the PDF's embedded image directly into the page's content
      // rather than exposing it as a separate "embedded:imageN.jpg" resource entry (unlike Tika
      // 3.x); the X-Tika-PDFextractInlineImages header no longer changes this.
      ExtractionRequest request =
          newRequest("pdf-with-image.pdf", "application/pdf", "xml", true, Map.of());
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ToXMLContentHandler xmlHandler = new ToXMLContentHandler();
        ExtractionMetadata md = backend.buildMetadataFromRequest(request);
        backend.extractWithSaxHandler(in, request, md, xmlHandler);
        String c = xmlHandler.toString();
        assertNotNull(c);
        assertTrue(c.contains("Puppet Apply"));
        // TikaServer 4.x uses a single lowercase tk: prefix for its metadata keys (TIKA-4816)
        assertEquals("org.apache.tika.parser.DefaultParser", md.getFirst("tk:parsed-by-full-set"));
      }
    }
  }

  private ExtractionRequest newRequest(String file, String contentType, String content) {
    return newRequest(file, contentType, content, false, Map.of());
  }

  @Test
  public void testMaxCharsLimitEnforced() throws Exception {
    // Set a very small max chars limit and attempt to extract more than that
    long maxChars = 10L;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl(), 180, null, maxChars)) {
      byte[] data =
          ("This content is definitely longer than ten characters.")
              .getBytes(StandardCharsets.UTF_8);
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        SolrException e =
            expectThrows(
                SolrException.class,
                () -> backend.extract(in, newRequest("test.txt", "text/plain", "xml")));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(
            "Expected message to mention max size exceeded",
            e.getMessage().contains("exceeded the configured maximum size"));
      }
    }
  }

  @Test
  public void testMaxCharsLimitEnforcedWithSaxHandler() throws Exception {
    long maxChars = 10L;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl(), 180, null, maxChars)) {
      byte[] data =
          ("This content is definitely longer than ten characters.")
              .getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request = newRequest("test.txt", "text/plain", "xml");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ToXMLContentHandler xmlHandler = new ToXMLContentHandler();
        ExtractionMetadata md = backend.buildMetadataFromRequest(request);
        SolrException e =
            expectThrows(
                SolrException.class,
                () -> backend.extractWithSaxHandler(in, request, md, xmlHandler));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(
            "Expected message to mention max size exceeded",
            e.getMessage().contains("exceeded the configured maximum size"));
      }
    }
  }

  /**
   * A single {@code TikaServerExtractionBackend} is constructed once by {@code
   * ExtractingRequestHandler.inform()} and reused for every request it handles, including
   * concurrently. {@code javax.xml.parsers.SAXParser} is not thread-safe, so parsing the response
   * must not share one {@code SAXParser} instance across concurrent {@code extract()} calls.
   */
  @Test
  public void testConcurrentExtractDoesNotShareSaxParser() throws Exception {
    int numThreads = 8;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(tikaContainer.getBaseUrl())) {
      ExecutorService pool =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              numThreads, new SolrNamedThreadFactory("TikaServerConcurrentExtractTest"));
      try {
        List<Future<ExtractionResult>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
          futures.add(
              pool.submit(
                  () -> {
                    byte[] data = "Hello TestContainers".getBytes(StandardCharsets.UTF_8);
                    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
                      return backend.extract(in, newRequest("test.txt", "text/plain", "text"));
                    }
                  }));
        }
        for (Future<ExtractionResult> future : futures) {
          ExtractionResult res = future.get(60, TimeUnit.SECONDS);
          assertNotNull(res);
          assertNotNull(res.getContent());
          assertTrue(res.getContent().contains("Hello TestContainers"));
        }
      } finally {
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }
}
