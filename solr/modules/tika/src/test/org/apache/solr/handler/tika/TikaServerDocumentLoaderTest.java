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
package org.apache.solr.handler.tika;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TikaServerDocumentLoaderTest {

  @Mock private SolrQueryRequest mockSolrQueryRequest;
  @Mock private SolrQueryResponse mockSolrQueryResponse;
  @Mock private ContentStream mockContentStream;
  @Mock private UpdateRequestProcessor mockUpdateRequestProcessor;
  @Mock private HttpClient mockHttpClient; // Jetty HttpClient
  @Mock private Request mockJettyRequest; // Jetty Request
  @Mock private ContentResponse mockJettyResponse; // Jetty ContentResponse

  private TikaServerDocumentLoader loader;

  // Default configuration values for the loader instance used in most tests
  private String tikaServerUrl = "http://fakeTikaServer:9998";
  private int connectionTimeout = 5000;
  private int socketTimeout = 60000;
  private String idField = "X-TIKA:resourceName"; // Tika field used for Solr ID
  private boolean returnMetadata = true;
  private String metadataPrefix = "meta_";
  private String contentField = "text_content";

  @Before
  public void setUp() throws Exception {
    assumeWorkingMockito();

    MockitoAnnotations.openMocks(this);

    // Instantiate the loader with mocked dependencies and typical parameters
    loader =
        new TikaServerDocumentLoader(
            mockSolrQueryRequest,
            mockUpdateRequestProcessor,
            tikaServerUrl,
            connectionTimeout,
            socketTimeout,
            idField,
            returnMetadata,
            metadataPrefix,
            contentField,
            mockHttpClient // Injecting the mock Jetty HttpClient
            );
  }

  private void setupDefaultJettyMocks()
      throws ExecutionException, InterruptedException, TimeoutException {
    when(mockHttpClient.POST(anyString())).thenReturn(mockJettyRequest);
    when(mockJettyRequest.accept(anyString())).thenReturn(mockJettyRequest);
    when(mockJettyRequest.headers(any())).thenReturn(mockJettyRequest);
    when(mockJettyRequest.headers(any())).thenReturn(mockJettyRequest);
    when(mockJettyRequest.body(any())).thenReturn(mockJettyRequest);
    when(mockJettyRequest.timeout(anyLong(), any(TimeUnit.class))).thenReturn(mockJettyRequest);
    when(mockJettyRequest.send()).thenReturn(mockJettyResponse);
  }

  @Test
  public void testSuccessfulExtraction() throws Exception {
    setupDefaultJettyMocks();
    String sampleJson =
        "[{\"X-TIKA:content\": \"This is the extracted content.\", \"dc:title\": \"Test Document\", \"custom:myKey\": \"customValue\", \"X-TIKA:resourceName\": \"test.doc\"}]";

    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJson);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);
    when(mockContentStream.getContentType()).thenReturn("application/pdf");

    loader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertEquals("This is the extracted content.", sdoc.getFieldValue(contentField));
    assertEquals("Test Document", sdoc.getFieldValue(metadataPrefix + "dc:title"));
    assertEquals("customValue", sdoc.getFieldValue(metadataPrefix + "custom:myKey"));
    assertEquals("test.doc", sdoc.getFieldValue(CommonParams.ID));
    assertNotNull(sdoc.getFieldValue(metadataPrefix + "X-TIKA:resourceName"));
    assertEquals("test.doc", sdoc.getFieldValue(metadataPrefix + "X-TIKA:resourceName"));

    verify(dataStream, times(1)).close();
  }

  @Test
  public void testTikaServerError() throws Exception {
    setupDefaultJettyMocks();
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.INTERNAL_SERVER_ERROR_500);
    when(mockJettyResponse.getContentAsString()).thenReturn("Tika Server Error");

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);
    when(mockContentStream.getContentType()).thenReturn("application/pdf");

    SolrException e =
        assertThrows(
            SolrException.class,
            () -> {
              loader.load(
                  mockSolrQueryRequest,
                  mockSolrQueryResponse,
                  mockContentStream,
                  mockUpdateRequestProcessor);
            });
    assertTrue(e.getMessage().contains("Tika Server returned HTTP error 500"));

    verify(mockUpdateRequestProcessor, never()).processAdd(any(AddUpdateCommand.class));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testMalformedJsonResponse() throws Exception {
    setupDefaultJettyMocks();
    String malformedJson = "[{\"X-TIKA:content\": \"test content\",";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(malformedJson);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);
    when(mockContentStream.getContentType()).thenReturn("application/pdf");

    SolrException e =
        assertThrows(
            SolrException.class,
            () -> {
              loader.load(
                  mockSolrQueryRequest,
                  mockSolrQueryResponse,
                  mockContentStream,
                  mockUpdateRequestProcessor);
            });
    assertTrue(
        e.getMessage().contains("Error processing document")
            || e.getMessage().contains("Unexpected JSON response format"));

    verify(mockUpdateRequestProcessor, never()).processAdd(any(AddUpdateCommand.class));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testReturnMetadataFalse() throws Exception {
    TikaServerDocumentLoader customLoader =
        new TikaServerDocumentLoader(
            mockSolrQueryRequest,
            mockUpdateRequestProcessor,
            tikaServerUrl,
            connectionTimeout,
            socketTimeout,
            "X-TIKA:resourceName",
            false,
            "meta_",
            "content",
            mockHttpClient);

    setupDefaultJettyMocks();
    String sampleJson =
        "[{\"X-TIKA:content\": \"Test content\", \"dc:title\": \"Doc Title\", \"X-TIKA:resourceName\": \"doc1.pdf\"}]";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJson);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);

    customLoader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertEquals("Test content", sdoc.getFieldValue("content"));
    assertEquals("doc1.pdf", sdoc.getFieldValue(CommonParams.ID));
    assertNull("Metadata field 'meta_dc:title' should be null", sdoc.getField("meta_dc:title"));
    assertNull(
        "Metadata field 'meta_X-TIKA:resourceName' should be null",
        sdoc.getField("meta_X-TIKA:resourceName"));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testNoIdFieldConfigured() throws Exception {
    TikaServerDocumentLoader customLoader =
        new TikaServerDocumentLoader(
            mockSolrQueryRequest,
            mockUpdateRequestProcessor,
            tikaServerUrl,
            connectionTimeout,
            socketTimeout,
            null,
            true,
            "meta_",
            "content",
            mockHttpClient);

    setupDefaultJettyMocks();
    String sampleJson =
        "[{\"X-TIKA:content\": \"Test content\", \"dc:title\": \"Doc Title\", \"X-TIKA:resourceName\": \"doc1.pdf\"}]";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJson);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);

    customLoader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertEquals("Test content", sdoc.getFieldValue("content"));
    assertEquals("Doc Title", sdoc.getFieldValue("meta_dc:title"));
    assertEquals("doc1.pdf", sdoc.getFieldValue("meta_X-TIKA:resourceName"));
    assertNull("ID field should be null", sdoc.getFieldValue(CommonParams.ID));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testContentFieldMapping() throws Exception {
    TikaServerDocumentLoader customLoader =
        new TikaServerDocumentLoader(
            mockSolrQueryRequest,
            mockUpdateRequestProcessor,
            tikaServerUrl,
            connectionTimeout,
            socketTimeout,
            "X-TIKA:resourceName",
            true,
            "meta_",
            "my_custom_content_field",
            mockHttpClient);

    setupDefaultJettyMocks();
    String sampleJson =
        "[{\"X-TIKA:content\": \"Actual content here\", \"dc:title\": \"A Document\", \"X-TIKA:resourceName\": \"doc2.txt\"}]";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJson);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);

    customLoader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertEquals("Actual content here", sdoc.getFieldValue("my_custom_content_field"));
    assertNull(
        "Field 'content' (default) should be null as custom mapping is used",
        sdoc.getField("content"));
    assertEquals("doc2.txt", sdoc.getFieldValue(CommonParams.ID));
    assertEquals("A Document", sdoc.getFieldValue("meta_dc:title"));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testTikaReturnsNoContentField() throws Exception {
    // Uses the default 'loader' instance from setUp
    setupDefaultJettyMocks();
    String sampleJsonNoContent =
        "[{\"dc:title\": \"Document with no content field\", \"X-TIKA:resourceName\": \"no_content_field.doc\"}]";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJsonNoContent);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);

    loader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertNull(
        "Content field '" + this.contentField + "' should be null",
        sdoc.getFieldValue(this.contentField));
    assertEquals("no_content_field.doc", sdoc.getFieldValue(CommonParams.ID));
    assertEquals("Document with no content field", sdoc.getFieldValue(metadataPrefix + "dc:title"));
    verify(dataStream, times(1)).close();
  }

  @Test
  public void testTikaReturnsNullContentField() throws Exception {
    // Uses the default 'loader' instance from setUp
    setupDefaultJettyMocks();
    String sampleJsonNullContent =
        "[{\"X-TIKA:content\": null, \"dc:title\": \"Document with null content\", \"X-TIKA:resourceName\": \"null_content.doc\"}]";
    when(mockJettyResponse.getStatus()).thenReturn(HttpStatus.OK_200);
    when(mockJettyResponse.getContentAsString()).thenReturn(sampleJsonNullContent);

    InputStream dataStream =
        new ByteArrayInputStream("dummy data".getBytes(StandardCharsets.UTF_8));
    when(mockContentStream.getStream()).thenReturn(dataStream);

    loader.load(
        mockSolrQueryRequest, mockSolrQueryResponse, mockContentStream, mockUpdateRequestProcessor);

    ArgumentCaptor<AddUpdateCommand> addUpdateCommandCaptor =
        ArgumentCaptor.forClass(AddUpdateCommand.class);
    verify(mockUpdateRequestProcessor, times(1)).processAdd(addUpdateCommandCaptor.capture());
    SolrInputDocument sdoc = addUpdateCommandCaptor.getValue().getSolrInputDocument();

    assertNotNull("SolrInputDocument should not be null", sdoc);
    assertNull(
        "Content field '" + this.contentField + "' should be null",
        sdoc.getFieldValue(this.contentField));
    assertEquals("null_content.doc", sdoc.getFieldValue(CommonParams.ID));
    assertEquals("Document with null content", sdoc.getFieldValue(metadataPrefix + "dc:title"));
    verify(dataStream, times(1)).close();
  }
}
