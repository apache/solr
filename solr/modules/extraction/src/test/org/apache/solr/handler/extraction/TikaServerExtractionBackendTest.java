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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.extraction.fromtika.ToXMLContentHandler;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

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

  private static GenericContainer<?> tika;
  private static String baseUrl;

  @SuppressWarnings("resource")
  @BeforeClass
  public static void startTikaServer() {
    Assume.assumeFalse(
        "Skipping on s390x", "s390x".equalsIgnoreCase(System.getProperty("os.arch")));

    try {
      // allowPerRequestConfig is off by default (it lets a client inject arbitrary parser
      // config, e.g. for encrypted-document passwords or tikaserver.config); enabling it here is
      // test-only.
      tika =
          new GenericContainer<>("apache/tika:4.0.0-full")
              .withExposedPorts(9998)
              .withCopyFileToContainer(
                  MountableFile.forHostPath(getFile("extraction/tika-server-config.json")),
                  "/tika-config.json")
              .withCommand("-c", "/tika-config.json");
      tika.start();
      baseUrl = "http://" + tika.getHost() + ":" + tika.getMappedPort(9998);
    } catch (Throwable t) {
      // Skip tests if Docker/Testcontainers are not available in the environment
      Assume.assumeNoException("Docker/Testcontainers not available; skipping TikaServer tests", t);
    }
  }

  @AfterClass
  public static void stopTikaServer() {
    if (tika != null) {
      try {
        tika.stop();
      } catch (Throwable ignore) {
      }
      tika = null;
    }
  }

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
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
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
  public void testExtractWithSaxHandlerXml() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
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
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = Files.readAllBytes(getFile("extraction/pdf-with-image.pdf"));
      // Tika 4.x removed the X-Tika-* header family entirely (see resolveConfigJson's javadoc);
      // there is no replacement for this combination. Per-request config now requires the
      // multipart /config endpoints, but Tika 4.x has no XML-output variant of /rmeta/config, so
      // per-request PDF options (e.g. explicit inline-image extraction) cannot be requested
      // together with tikaserver.recursive=true. The PDF's embedded image still gets OCR'd into
      // the main document's content by default, just not exposed as a separate embedded
      // resource entry the way the pre-4.x X-Tika-PDFextractInlineImages header used to.
      ExtractionRequest request =
          newRequest("pdf-with-image.pdf", "application/pdf", "xml", true, Map.of());
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ToXMLContentHandler xmlHandler = new ToXMLContentHandler();
        ExtractionMetadata md = backend.buildMetadataFromRequest(request);
        backend.extractWithSaxHandler(in, request, md, xmlHandler);
        String c = xmlHandler.toString();
        assertNotNull(c);
        assertTrue(c.contains("Puppet Apply"));
        // Tika 4.x renamed its metadata keys under a single lowercase tk: prefix (TIKA-4816)
        assertEquals("org.apache.tika.parser.DefaultParser", md.getFirst("tk:parsed-by-full-set"));
      }
    }
  }

  private ExtractionRequest newRequest(String file, String contentType, String content) {
    return newRequest(file, contentType, content, false, Map.of());
  }

  @Test
  public void testMaxCharsLimitEnforced() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    // Set a very small max chars limit and attempt to extract more than that
    long maxChars = 10L;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(baseUrl, 180, null, maxChars)) {
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
    Assume.assumeTrue("Tika server container not started", tika != null);
    long maxChars = 10L;
    try (TikaServerExtractionBackend backend =
        new TikaServerExtractionBackend(baseUrl, 180, null, maxChars)) {
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

  private static ExtractionRequest newRequestWithConfig(
      String resourceName, String contentType, String extractFormat, String configJson) {
    return ExtractionRequest.builder()
        .streamType(contentType)
        .resourceName(resourceName)
        .contentType(contentType)
        .streamName(resourceName)
        .extractFormat(extractFormat)
        .tikaServerConfigJson(configJson)
        .build();
  }

  @Test
  public void testConfigJsonDisablesOcr() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = Files.readAllBytes(getFile("extraction/pdf-with-image.pdf"));
      // With no config, the PDF's embedded image gets OCR'd and "Puppet Apply" (from the image)
      // appears in the extracted content. Disabling OCR via tikaserver.config should suppress it.
      ExtractionRequest request =
          newRequestWithConfig(
              "pdf-with-image.pdf",
              "application/pdf",
              "xml",
              "{\"pdf-parser\":{\"ocr\":{\"strategy\":\"NO_OCR\"}}}");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ExtractionResult res = backend.extract(in, request);
        assertNotNull(res.getContent());
        assertFalse(
            "Expected tikaserver.config's NO_OCR strategy to suppress the OCR'd image text",
            res.getContent().contains("Puppet Apply"));
      }
    }
  }

  @Test
  public void testConfigJsonMergesWithPassword() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = Files.readAllBytes(getFile("extraction/encrypted-password-is-solrRules.pdf"));
      ExtractionRequest request =
          ExtractionRequest.builder()
              .streamType("application/pdf")
              .resourceName("encrypted-password-is-solrRules.pdf")
              .contentType("application/pdf")
              .streamName("encrypted-password-is-solrRules.pdf")
              .extractFormat("xml")
              .resourcePassword("solrRules")
              .tikaServerConfigJson("{\"pdf-parser\":{\"ocr\":{\"strategy\":\"NO_OCR\"}}}")
              .build();
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        ExtractionResult res = backend.extract(in, request);
        assertNotNull(res);
        assertTrue(
            "Expected the password-unlocked content to still be present alongside the merged"
                + " tikaserver.config",
            res.getContent().contains("This is a test of PDF and Word extraction"));
      }
    }
  }

  @Test
  public void testInvalidConfigJsonRejected() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request =
          newRequestWithConfig("test.txt", "text/plain", "xml", "not valid json");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        SolrException e = expectThrows(SolrException.class, () -> backend.extract(in, request));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(e.getMessage().contains(ExtractingParams.TIKASERVER_CONFIG_JSON));
      }
    }
  }

  @Test
  public void testConfigJsonRejectedForRecursive() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request =
          ExtractionRequest.builder()
              .streamType("text/plain")
              .resourceName("test.txt")
              .contentType("text/plain")
              .streamName("test.txt")
              .extractFormat("xml")
              .tikaServerRecursive(true)
              .tikaServerConfigJson("{\"parse-context\":{}}")
              .build();
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        SolrException e = expectThrows(SolrException.class, () -> backend.extract(in, request));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertTrue(e.getMessage().contains(ExtractingParams.TIKASERVER_RECURSIVE));
      }
    }
  }
}
