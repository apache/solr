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
package org.apache.solr.client.solrj.jetty;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpJdkSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.JavaBinRequestWriter;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.XMLRequestWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.XMLResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** Checks legacy request bodies and custom stream writers through both HTTP clients. */
@SuppressWarnings("deprecation")
public class TestLegacyContentStreamCompatibility extends SolrTestCase {

  private static final String REQUEST_BODY = "original-request-body";
  private static final String WRITER_BODY = "custom-writer-body";
  private static final String CONTENT_TYPE = "text/plain";
  private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";
  private static final AtomicReference<CapturedRequest> capturedRequest = new AtomicReference<>();

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    JettyConfig config =
        JettyConfig.builder()
            .withServlet(new ServletHolder(RawBodyServlet.class), "/legacy-content/*")
            .build();
    solrTestRule.startSolr(createTempDir(), new Properties(), config);
  }

  @After
  public void clearServlet() {
    capturedRequest.set(null);
  }

  @Test
  public void testJettyLegacyRequestWithJavaBinWriter() throws Exception {
    assertLegacyRequestBody(false, new JavaBinRequestWriter());
  }

  @Test
  public void testJettyLegacyRequestWithXmlWriter() throws Exception {
    assertLegacyRequestBody(false, new XMLRequestWriter());
  }

  @Test
  public void testJdkLegacyRequestWithJavaBinWriter() throws Exception {
    assertLegacyRequestBody(true, new JavaBinRequestWriter());
  }

  @Test
  public void testJdkLegacyRequestWithXmlWriter() throws Exception {
    assertLegacyRequestBody(true, new XMLRequestWriter());
  }

  @Test
  public void testJettyJavaBinWriterStreamOverride() throws Exception {
    assertWriterOverride(false, false);
  }

  @Test
  public void testJettyXmlWriterStreamOverride() throws Exception {
    assertWriterOverride(false, true);
  }

  @Test
  public void testJdkJavaBinWriterStreamOverride() throws Exception {
    assertWriterOverride(true, false);
  }

  @Test
  public void testJdkXmlWriterStreamOverride() throws Exception {
    assertWriterOverride(true, true);
  }

  @Test
  public void testJettyContentWriterControl() throws Exception {
    assertExplicitContentWriter(false, CONTENT_TYPE);
  }

  @Test
  public void testJdkContentWriterControl() throws Exception {
    assertExplicitContentWriter(true, CONTENT_TYPE);
  }

  @Test
  public void testJettyFormContentWriterControl() throws Exception {
    assertExplicitContentWriter(false, FORM_CONTENT_TYPE);
  }

  @Test
  public void testJdkFormContentWriterControl() throws Exception {
    assertExplicitContentWriter(true, FORM_CONTENT_TYPE);
  }

  private void assertLegacyRequestBody(boolean jdk, RequestWriter writer) throws Exception {
    GenericSolrRequest request =
        new GenericSolrRequest(
            SolrRequest.METHOD.POST, "/update", SolrRequest.SolrRequestType.UPDATE) {
          @Override
          public Collection<ContentStream> getContentStreams() {
            return streams(REQUEST_BODY);
          }
        };
    assertNull(request.getContentWriter(null));
    assertPostBody(jdk, writer, request, REQUEST_BODY);
  }

  private void assertWriterOverride(boolean jdk, boolean xml) throws Exception {
    AtomicInteger calls = new AtomicInteger();
    RequestWriter writer = streamOverrideWriter(xml, calls);
    assertPostBody(jdk, writer, new LegacyStreamsRequest(), WRITER_BODY);
    assertTrue("The custom writer's stream hook must be consulted", calls.get() > 0);
  }

  private RequestWriter streamOverrideWriter(boolean xml, AtomicInteger calls) {
    RequestWriter writer;
    if (xml) {
      writer =
          new XMLRequestWriter() {
            @Override
            public Collection<ContentStream> getContentStreams(SolrRequest<?> request) {
              calls.incrementAndGet();
              return streams(WRITER_BODY);
            }
          };
    } else {
      writer =
          new JavaBinRequestWriter() {
            @Override
            public Collection<ContentStream> getContentStreams(SolrRequest<?> request) {
              calls.incrementAndGet();
              return streams(WRITER_BODY);
            }
          };
    }
    return writer;
  }

  private void assertExplicitContentWriter(boolean jdk, String contentType) throws Exception {
    GenericSolrRequest request =
        new GenericSolrRequest(
            SolrRequest.METHOD.POST, "/update", SolrRequest.SolrRequestType.UPDATE);
    request.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(REQUEST_BODY, contentType));
    for (boolean xml : List.of(false, true)) {
      AtomicInteger calls = new AtomicInteger();
      CapturedRequest captured =
          assertPostBody(jdk, streamOverrideWriter(xml, calls), request, REQUEST_BODY);
      assertEquals(contentType, captured.contentType());
      assertEquals("An explicit writer must take precedence over the stream hook", 0, calls.get());
    }
  }

  private CapturedRequest assertPostBody(
      boolean jdk, RequestWriter writer, SolrRequest<?> request, String expectedBody)
      throws Exception {
    capturedRequest.set(null);
    String url = solrTestRule.getBaseUrl() + "/legacy-content";
    try (SolrClient client =
        jdk
            ? new HttpJdkSolrClient.Builder(url)
                .withDefaultCollection("collection1")
                .withRequestWriter(writer)
                .withResponseParser(new XMLResponseParser())
                .build()
            : new HttpJettySolrClient.Builder(url)
                .withDefaultCollection("collection1")
                .withRequestWriter(writer)
                .withResponseParser(new XMLResponseParser())
                .build()) {
      client.request(request);
      CapturedRequest captured = capturedRequest.get();
      assertNotNull("The servlet must capture the POST body", captured);
      assertEquals("POST", captured.method());
      // Record the legacy content type separately from the request-body assertion.
      System.out.println(
          "Captured POST: client="
              + client.getClass().getSimpleName()
              + ", writer="
              + writer.getClass().getName()
              + ", contentType="
              + captured.contentType()
              + ", body="
              + new String(captured.body(), StandardCharsets.UTF_8));
      assertArrayEquals(expectedBody.getBytes(StandardCharsets.UTF_8), captured.body());
      return captured;
    }
  }

  private record CapturedRequest(String method, String contentType, byte[] body) {}

  /** Captures bytes before any servlet parameter parsing can consume a form-encoded body. */
  public static class RawBodyServlet extends HttpServlet {

    @Override
    protected void doHead(HttpServletRequest request, HttpServletResponse response) {
      response.setContentType("application/xml; charset=UTF-8");
      response.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      byte[] body = request.getInputStream().readAllBytes();
      capturedRequest.set(new CapturedRequest(request.getMethod(), request.getContentType(), body));
      response.setContentType("application/xml; charset=UTF-8");
      response
          .getOutputStream()
          .write(
              "<response><lst name=\"responseHeader\"><int name=\"status\">0</int></lst></response>"
                  .getBytes(StandardCharsets.UTF_8));
    }
  }

  private static List<ContentStream> streams(String body) {
    return List.of(new ContentStreamBase.StringStream(body, CONTENT_TYPE));
  }

  private static class LegacyStreamsRequest extends SolrRequest<SimpleSolrResponse> {

    LegacyStreamsRequest() {
      super(METHOD.POST, "/update", SolrRequestType.UPDATE);
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
      return streams(REQUEST_BODY);
    }

    @Override
    public SolrParams getParams() {
      return new ModifiableSolrParams();
    }

    @Override
    protected SimpleSolrResponse createResponse(NamedList<Object> response) {
      return new SimpleSolrResponse();
    }
  }
}
