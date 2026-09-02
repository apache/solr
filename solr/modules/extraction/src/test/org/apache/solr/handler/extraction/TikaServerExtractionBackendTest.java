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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.extraction.fromtika.ToXMLContentHandler;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
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
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

  private static Server embeddingsServer;
  private static GenericContainer<?> tikaChunks;
  private static String chunksBaseUrl;

  /**
   * A minimal, embedded-Jetty, OpenAI-compatible embeddings endpoint standing in for a real
   * embeddings API. Each embedding is a small deterministic function of the input text.
   */
  private static class EmbeddingsHandler extends Handler.Abstract {
    @Override
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
      String body = Content.Source.asString(request, StandardCharsets.UTF_8);
      @SuppressWarnings("unchecked")
      Map<String, Object> req = (Map<String, Object>) Utils.fromJSONString(body);
      List<?> inputs = (List<?>) req.get("input");
      List<Object> data = new ArrayList<>();
      for (int i = 0; i < inputs.size(); i++) {
        int total = 0;
        for (byte b : String.valueOf(inputs.get(i)).getBytes(StandardCharsets.UTF_8)) {
          total += (b & 0xFF);
        }
        List<Double> vector = new ArrayList<>();
        for (int d = 0; d < 4; d++) {
          vector.add(((total + d) % 10) / 10.0);
        }
        Map<String, Object> embedding = new LinkedHashMap<>();
        embedding.put("object", "embedding");
        embedding.put("index", i);
        embedding.put("embedding", vector);
        data.add(embedding);
      }
      Map<String, Object> resp = new LinkedHashMap<>();
      resp.put("object", "list");
      resp.put("data", data);
      resp.put("model", req.getOrDefault("model", "mock-embed"));
      byte[] respBytes = Utils.toJSONString(resp).getBytes(StandardCharsets.UTF_8);
      response.setStatus(200);
      response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json");
      response.write(true, ByteBuffer.wrap(respBytes), callback);
      return true;
    }
  }

  @SuppressWarnings("resource")
  @BeforeClass
  public static void startChunksTikaServer() {
    Assume.assumeFalse(
        "Skipping on s390x", "s390x".equalsIgnoreCase(System.getProperty("os.arch")));
    try {
      embeddingsServer = new Server();
      ServerConnector connector = new ServerConnector(embeddingsServer);
      connector.setPort(0);
      embeddingsServer.addConnector(connector);
      embeddingsServer.setHandler(new EmbeddingsHandler());
      embeddingsServer.start();
      int embeddingsPort = connector.getLocalPort();

      // Tika 4.x requires a top-level "server" element even when there's nothing to configure
      // in it.
      String config =
          "{\"server\":{},\"metadata-filters\":[{\"openai-embedding-filter\":"
              + "{\"baseUrl\":\"http://host.docker.internal:"
              + embeddingsPort
              + "\",\"model\":\"mock-embed\"}}]}";
      Path configFile = Files.createTempFile("tika-chunks-config", ".json");
      Files.writeString(configFile, config);
      // Files.createTempFile defaults to owner-only (0600) permissions; the TikaServer container
      // process runs as a different uid and needs read access to the bind-mounted file.
      Files.setPosixFilePermissions(configFile, PosixFilePermissions.fromString("rw-r--r--"));

      tikaChunks =
          new GenericContainer<>("apache/tika:4.0.0-full")
              .withExposedPorts(9998)
              .withExtraHost("host.docker.internal", "host-gateway")
              .withCopyFileToContainer(MountableFile.forHostPath(configFile), "/tika-config.json")
              .withCommand("-c", "/tika-config.json")
              .withLogConsumer(new Slf4jLogConsumer(log))
              .waitingFor(Wait.forListeningPort());
      tikaChunks.start();
      chunksBaseUrl = "http://" + tikaChunks.getHost() + ":" + tikaChunks.getMappedPort(9998);
    } catch (Throwable t) {
      // Skip tests if Docker/Testcontainers are not available in the environment
      Assume.assumeNoException("Docker/Testcontainers not available; skipping chunk tests", t);
    }
  }

  @AfterClass
  public static void stopChunksTikaServer() {
    if (tikaChunks != null) {
      try {
        tikaChunks.stop();
      } catch (Throwable ignore) {
      }
      tikaChunks = null;
    }
    if (embeddingsServer != null) {
      try {
        embeddingsServer.stop();
      } catch (Throwable ignore) {
      }
      embeddingsServer = null;
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

  private static ExtractionRequest newMarkdownRequest(String resourceName) {
    return ExtractionRequest.builder()
        .resourceName(resourceName)
        .contentType("text/markdown")
        .streamName(resourceName)
        .build();
  }

  @Test
  public void testExtractChunks() throws Exception {
    Assume.assumeTrue("Chunks TikaServer container not started", tikaChunks != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(chunksBaseUrl)) {
      String markdown =
          "# Report\n\nRevenue grew 15% year over year in the last quarter.\n\n"
              + "# Costs\n\nOperating costs remained flat compared to prior periods and did not"
              + " change much.\n";
      byte[] data = markdown.getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request = newMarkdownRequest("sample.md");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        List<TikaServerExtractionBackend.Chunk> chunks = backend.extractChunks(in, request);
        assertFalse("Expected at least one chunk", chunks.isEmpty());
        assertTrue(
            "Expected more than one chunk from a two-heading Markdown document", chunks.size() > 1);
        for (TikaServerExtractionBackend.Chunk chunk : chunks) {
          assertNotNull(chunk.text);
          assertFalse(chunk.text.isBlank());
          assertEquals(
              "Expected the mock embedding server's 4-dimensional vectors", 4, chunk.vector.length);
        }
      }
    }
  }

  @Test
  public void testExtractChunksThrowsWithoutEmbeddingFilterConfigured() throws Exception {
    // Reuses the plain `tika` container from startTikaServer(), which has no embedding filter.
    Assume.assumeTrue("Tika server container not started", tika != null);
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      byte[] data = "# Heading\n\nSome text.".getBytes(StandardCharsets.UTF_8);
      ExtractionRequest request = newMarkdownRequest("sample.md");
      try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
        SolrException e =
            expectThrows(SolrException.class, () -> backend.extractChunks(in, request));
        assertTrue(e.getMessage().contains("tk:chunks"));
      }
    }
  }
}
