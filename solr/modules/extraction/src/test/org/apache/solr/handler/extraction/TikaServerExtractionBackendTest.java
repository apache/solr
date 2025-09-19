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
import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

/**
 * Integration tests for TikaServerExtractionBackend using a real Tika Server via Testcontainers.
 */
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      TikaServerExtractionBackendTest.TestcontainersThreadsFilter.class
    })
public class TikaServerExtractionBackendTest extends SolrTestCaseJ4 {

  // Ignore known non-daemon threads spawned by Testcontainers and Java HttpClient in this test
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
  private static ExecutorService httpExec;
  private static HttpClient client;

  @BeforeClass
  public static void startTikaServer() {
    try {
      httpExec =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              2,
              r -> {
                Thread t = new Thread(r, "HttpClient-TestContainers");
                t.setDaemon(true);
                return t;
              });
      client = HttpClient.newBuilder().executor(httpExec).build();
      tika = new GenericContainer<>("apache/tika:3.2.3.0-full").withExposedPorts(9998);
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
    if (httpExec != null) {
      try {
        httpExec.shutdownNow();
      } catch (Throwable ignore) {
      }
      httpExec = null;
    }
    client = null;
  }

  private static ExtractionRequest newRequest(
      String resourceName, String contentType, String extractFormat) {
    return new ExtractionRequest(
        contentType, // streamType
        resourceName, // resourceName
        contentType, // contentType
        null, // charset
        resourceName, // streamName
        null, // sourceInfo
        null, // size
        null, // resourcePassword
        null, // passwordsMap
        extractFormat // extraction format xml or text
        );
  }

  @Test
  public void testExtractTextAndMetadata() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(client, baseUrl);
    byte[] data = "Hello TestContainers".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      ExtractionResult res = backend.extract(in, newRequest("test.txt", "text/plain", "text"));
      assertNotNull(res);
      assertNotNull(res.getContent());
      assertTrue(res.getContent().contains("Hello TestContainers"));
      assertNotNull(res.getMetadata());
      String[] cts = res.getMetadata().getValues("Content-Type");
      assertNotNull(cts);
      assertTrue(cts.length >= 1);
      // Tika may append charset; be flexible
      assertTrue(cts[0].startsWith("text/plain"));
    }
  }

  @Test
  public void testExtractOnlyXml() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(client, baseUrl);
    byte[] data = "Hello XML".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      ExtractionResult res =
          backend.extractOnly(in, newRequest("test.txt", "text/plain", "xml"), null);
      assertNotNull(res);
      String c = res.getContent();
      assertNotNull(c);
      // Tika Server may return XHTML without XML declaration; be flexible
      assertTrue(
          c.contains("<?xml")
              || c.toLowerCase(java.util.Locale.ROOT).contains("<html")
              || c.toLowerCase(java.util.Locale.ROOT).contains("<xhtml"));
      assertTrue(c.contains("Hello XML"));
    }
  }

  @Test
  public void testParseToSolrContentHandlerUnsupported() throws Exception {
    Assume.assumeTrue("Tika server container not started", tika != null);
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(client, baseUrl);
    byte[] data = "dummy".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      expectThrows(
          UnsupportedOperationException.class,
          () ->
              backend.parseToSolrContentHandler(
                  in,
                  newRequest("test.txt", "text/plain", "text"),
                  new SolrContentHandler(new ExtractionMetadata(), params(), null),
                  new ExtractionMetadata()));
    }
  }
}
