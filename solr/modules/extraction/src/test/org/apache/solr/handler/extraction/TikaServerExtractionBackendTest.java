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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/** Unit tests for TikaServerExtractionBackend using a mocked HttpClient (no networking). */
public class TikaServerExtractionBackendTest extends SolrTestCaseJ4 {

  static {
    // Allow the SecureRandom algorithm used in this environment to avoid class configuration
    // failure in tests.
    System.setProperty("test.solr.allowed.securerandom", "NativePRNG");
  }

  private static class FakeHttpClient extends HttpClient {
    @Override
    public Optional<CookieHandler> cookieHandler() { return Optional.empty(); }

    @Override
    public Optional<Duration> connectTimeout() { return Optional.of(Duration.ofSeconds(5)); }

    @Override
    public Redirect followRedirects() { return Redirect.NEVER; }

    @Override
    public Optional<ProxySelector> proxy() { return Optional.empty(); }

    @Override
    public SSLContext sslContext() { try { return SSLContext.getDefault(); } catch (Exception e) { throw new RuntimeException(e);} }

    @Override
    public SSLParameters sslParameters() { return new SSLParameters(); }

    @Override
    public Optional<Executor> executor() { return Optional.empty(); }

    @Override
    public Optional<java.net.Authenticator> authenticator() { return Optional.empty(); }

    @Override
    public Version version() { return Version.HTTP_1_1; }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException {
      return respond(request, responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
      return CompletableFuture.completedFuture(respond(request, responseBodyHandler));
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler, HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
      return CompletableFuture.completedFuture(respond(request, responseBodyHandler));
    }

    private <T> HttpResponse<T> respond(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
      try {
        URI uri = request.uri();
        String path = uri.getPath();
        byte[] body;
        String ct;
        int sc = 200;
        if ("/tika".equals(path)) {
          String accept = request.headers().firstValue("Accept").orElse("text/plain");
          if ("application/xml".equalsIgnoreCase(accept)) {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xhtml><body>XML OUT</body></xhtml>";
            body = xml.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ct = "application/xml";
          } else {
            body = "TEXT OUT".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ct = "text/plain";
          }
        } else if ("/meta".equals(path)) {
          String json =
              "{\"Content-Type\":[\"text/plain\"],\"resourcename\":[\"test.txt\"],\"X-Parsed-By\":[\"SomeParser\"]}";
          body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
          ct = "application/json";
        } else {
          body = "Not Found".getBytes(java.nio.charset.StandardCharsets.UTF_8);
          sc = 404;
          ct = "text/plain";
        }
        final int status = sc;
        final String contentType = ct;
        // Decide expected body type based on endpoint (mimics our backend usage)
        final Object bodyObj =
            "/meta".equals(path)
                ? new String(body, java.nio.charset.StandardCharsets.UTF_8)
                : body; // /tika returns bytes
        return new HttpResponse<>() {
          @Override public int statusCode() { return status; }
          @Override public HttpRequest request() { return request; }
          @Override public Optional<HttpResponse<T>> previousResponse() { return Optional.empty(); }
          @Override public HttpHeaders headers() { return HttpHeaders.of(java.util.Map.of("Content-Type", java.util.List.of(contentType)), (k,v)->true); }
          @Override public T body() { @SuppressWarnings("unchecked") T t = (T) bodyObj; return t; }
          @Override public Optional<javax.net.ssl.SSLSession> sslSession() { return Optional.empty(); }
          @Override public URI uri() { return uri; }
          @Override public Version version() { return Version.HTTP_1_1; }
        };
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static ExtractionRequest newRequest(String resourceName, String contentType) {
    return new ExtractionRequest(
        contentType, // streamType
        resourceName, // resourceName
        contentType, // contentType
        null, // charset
        resourceName, // streamName
        null, // sourceInfo
        null, // size
        null, // resourcePassword
        null // passwordsMap
        );
  }

  @Test
  public void testExtractTextAndMetadata() throws Exception {
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(new FakeHttpClient(), "http://example");
    byte[] data = "dummy".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      ExtractionResult res = backend.extract(in, newRequest("test.txt", "text/plain"));
      assertNotNull(res);
      assertEquals("TEXT OUT", res.getContent());
      assertNotNull(res.getMetadata());
      assertArrayEquals(new String[] {"text/plain"}, res.getMetadata().getValues("Content-Type"));
      assertArrayEquals(new String[] {"test.txt"}, res.getMetadata().getValues("resourcename"));
    }
  }

  @Test
  public void testExtractOnlyXml() throws Exception {
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(new FakeHttpClient(), "http://example");
    byte[] data = "dummy".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      ExtractionResult res =
          backend.extractOnly(
              in, newRequest("test.txt", "text/plain"), ExtractingDocumentLoader.XML_FORMAT, null);
      assertNotNull(res);
      assertTrue(res.getContent().contains("<?xml"));
      assertTrue(res.getContent().contains("XML OUT"));
    }
  }

  @Test
  public void testParseToSolrContentHandlerUnsupported() throws Exception {
    TikaServerExtractionBackend backend = new TikaServerExtractionBackend(new FakeHttpClient(), "http://example");
    byte[] data = "dummy".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      expectThrows(
          UnsupportedOperationException.class,
          () ->
              backend.parseToSolrContentHandler(
                  in,
                  newRequest("test.txt", "text/plain"),
                  new SolrContentHandler(new SimpleExtractionMetadata(), params(), null),
                  new SimpleExtractionMetadata()));
    }
  }
}
