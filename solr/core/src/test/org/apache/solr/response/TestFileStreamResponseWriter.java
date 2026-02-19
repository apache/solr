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
package org.apache.solr.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.JavaBinResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.api.ReplicationAPIBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.junit.Test;

public class TestFileStreamResponseWriter extends SolrTestCase {

  @Test
  public void testWriteWithRawWriter() throws IOException {
    FileStreamResponseWriter writer = new FileStreamResponseWriter();
    SolrQueryRequest request = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    SolrQueryResponse response = new SolrQueryResponse();

    // Create a mock RawWriter
    String testContent = "test file content";
    TestRawWriter rawWriter = new TestRawWriter(testContent, "application/octet-stream");

    // Add the RawWriter to the response
    response.add(ReplicationAPIBase.FILE_STREAM, rawWriter);

    // Write to output stream
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(out, request, response, null);

    // Verify the content was written
    String written = out.toString(StandardCharsets.UTF_8);
    assertEquals("Content should be written directly", testContent, written);
  }

  @Test
  public void testWriteWithoutRawWriter() throws IOException {
    FileStreamResponseWriter writer = new FileStreamResponseWriter();
    SolrQueryRequest request = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    SolrQueryResponse response = new SolrQueryResponse();

    // Don't add any RawWriter to the response

    // Write to output stream
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(out, request, response, null);

    // Verify nothing was written (since no RawWriter present)
    assertEquals("Nothing should be written when no RawWriter present", 0, out.size());
  }

  @Test
  public void testGetContentTypeWithRawWriter() {
    FileStreamResponseWriter writer = new FileStreamResponseWriter();
    SolrQueryRequest request = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    SolrQueryResponse response = new SolrQueryResponse();

    // Create a mock RawWriter with custom content type
    String customContentType = "application/custom-type";
    TestRawWriter rawWriter = new TestRawWriter("content", customContentType);

    // Add the RawWriter to the response
    response.add(ReplicationAPIBase.FILE_STREAM, rawWriter);

    // Get content type
    String contentType = writer.getContentType(request, response);
    assertEquals("Should return RawWriter's content type", customContentType, contentType);
  }

  @Test
  public void testGetContentTypeWithoutRawWriter() {
    FileStreamResponseWriter writer = new FileStreamResponseWriter();
    SolrQueryRequest request = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    SolrQueryResponse response = new SolrQueryResponse();

    // Don't add any RawWriter to the response

    // Get content type
    String contentType = writer.getContentType(request, response);
    assertEquals(
        "Should return default javabin content type",
        JavaBinResponseParser.JAVABIN_CONTENT_TYPE,
        contentType);
  }

  @Test
  public void testGetContentTypeWithRawWriterReturningNull() {
    FileStreamResponseWriter writer = new FileStreamResponseWriter();
    SolrQueryRequest request = new SolrQueryRequestBase(null, new ModifiableSolrParams());
    SolrQueryResponse response = new SolrQueryResponse();

    // Create a mock RawWriter that returns null for content type
    TestRawWriter rawWriter = new TestRawWriter("content", null);

    // Add the RawWriter to the response
    response.add(ReplicationAPIBase.FILE_STREAM, rawWriter);

    // Get content type
    String contentType = writer.getContentType(request, response);
    assertEquals(
        "Should return default javabin content type when RawWriter returns null",
        JavaBinResponseParser.JAVABIN_CONTENT_TYPE,
        contentType);
  }

  // Test helper classes
  // Avoids standing up a full Solr core for this test by mocking.
  private static class TestRawWriter implements SolrCore.RawWriter {
    private final String content;
    private final String contentType;

    public TestRawWriter(String content, String contentType) {
      this.content = content;
      this.contentType = contentType;
    }

    @Override
    public String getContentType() {
      return contentType;
    }

    @Override
    public void write(OutputStream os) throws IOException {
      if (content != null) {
        os.write(content.getBytes(StandardCharsets.UTF_8));
      }
    }
  }
}
