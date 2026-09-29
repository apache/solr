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
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.junit.After;
import org.junit.Test;

/**
 * Verifies that {@link TikaServerExtractionBackend} rejects a TikaServer older than {@code 4.x}
 * with a clear diagnostic, rather than failing later with confusing 404s or missing metadata.
 *
 * <p>Uses a tiny in-process Jetty {@link Server} stub for the {@code /version} endpoint instead of
 * a real Tika Server, since that's all this check depends on.
 */
public class TikaServerVersionCheckTest extends SolrTestCaseJ4 {

  private Server server;

  @After
  public void stopServer() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  private String startServerWithVersion(String versionResponseBody) throws Exception {
    server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setHost("localhost");
    server.addConnector(connector);
    server.setHandler(
        new Handler.Abstract() {
          @Override
          public boolean handle(Request request, Response response, Callback callback) {
            String path = Request.getPathInContext(request);
            String body;
            if ("/version".equals(path)) {
              body = versionResponseBody;
            } else if ("/tika/xml".equals(path)) {
              body =
                  "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                      + "<head></head><body>hello world</body></html>";
            } else {
              response.setStatus(404);
              callback.succeeded();
              return true;
            }
            response.setStatus(200);
            Content.Sink.write(response, true, body, callback);
            return true;
          }
        });
    server.start();
    return "http://localhost:" + connector.getLocalPort();
  }

  private static ExtractionRequest newRequest() {
    return ExtractionRequest.builder()
        .streamType("text/plain")
        .resourceName("test.txt")
        .contentType("text/plain")
        .streamName("test.txt")
        .extractFormat("xml")
        .build();
  }

  @Test
  public void testRejectsPre4xTikaServer() throws Exception {
    String baseUrl = startServerWithVersion("Apache Tika 3.2.3");
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      ExtractionRequest request = newRequest();
      try (ByteArrayInputStream in =
          new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8))) {
        SolrException e = expectThrows(SolrException.class, () -> backend.extract(in, request));
        assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, e.code());
        assertTrue(
            "Expected message to name the offending version and the minimum required, but was: "
                + e.getMessage(),
            e.getMessage().contains("Apache Tika 3.") && e.getMessage().contains("requires"));
      }
    }
  }

  @Test
  public void testAcceptsSupportedTikaServerVersion() throws Exception {
    String baseUrl = startServerWithVersion("Apache Tika 4.0.0");
    try (TikaServerExtractionBackend backend = new TikaServerExtractionBackend(baseUrl)) {
      ExtractionRequest request = newRequest();
      try (ByteArrayInputStream in =
          new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8))) {
        ExtractionResult result = backend.extract(in, request);
        assertNotNull(result);
        assertTrue(result.getContent().contains("hello world"));
      }
    }
  }
}
