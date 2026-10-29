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

package org.apache.solr.jersey;

import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.APPLICATION_XML;
import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.SolrTestCaseJ4.params;
import static org.apache.solr.client.solrj.response.JavaBinResponseParser.JAVABIN_CONTENT_TYPE_V2;
import static org.apache.solr.common.params.CommonParams.WT;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_PARAMS;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.handler.admin.ZookeeperRead;
import org.apache.solr.handler.admin.api.ReloadCollectionAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Checks response negotiation when a Jersey resource has no local SolrQueryRequest. */
public class MediaTypeOverridingFilterTest extends SolrTestCase {

  @Mock private ResourceInfo resourceInfo;
  @InjectMocks private MediaTypeOverridingFilter filter;

  private AutoCloseable mocks;
  private ContainerRequestContext request;
  private ContainerResponseContext response;
  private MultivaluedMap<String, String> requestHeaders;
  private MultivaluedMap<String, Object> responseHeaders;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUpFilter() {
    mocks = MockitoAnnotations.openMocks(this);
    request = mock(ContainerRequestContext.class);
    response = mock(ContainerResponseContext.class);
    requestHeaders = new MultivaluedHashMap<>();
    responseHeaders = new MultivaluedHashMap<>();
    responseHeaders.putSingle(CONTENT_TYPE, APPLICATION_XML);
    doReturn(ReloadCollectionAPI.class).when(resourceInfo).getResourceClass();
    when(request.getHeaders()).thenReturn(requestHeaders);
    when(request.getHeaderString(ACCEPT)).thenAnswer(ignored -> requestHeaders.getFirst(ACCEPT));
    when(response.getHeaders()).thenReturn(responseHeaders);
    when(response.getStatus()).thenReturn(200);
  }

  @After
  public void closeMocks() throws Exception {
    mocks.close();
  }

  @Test
  public void testWtFromQueryParamsWithoutSolrQueryRequest() throws Exception {
    requestHeaders.putSingle(ACCEPT, "text/plain");
    final Map<String, String> formats =
        Map.of("json", APPLICATION_JSON, "xml", APPLICATION_XML, "javabin", JAVABIN_CONTENT_TYPE_V2);
    for (var format : formats.entrySet()) {
      responseHeaders.putSingle(CONTENT_TYPE, "text/plain");
      when(request.getProperty(SOLR_PARAMS)).thenReturn(params(WT, format.getKey()));

      filter.filter(request, response);

      assertEquals(format.getKey(), format.getValue(), responseHeaders.getFirst(CONTENT_TYPE));
    }
  }

  @Test
  public void testSolrQueryRequestParamsTakePrecedence() throws Exception {
    responseHeaders.putSingle(CONTENT_TYPE, "text/plain");
    final SolrQueryRequest solrRequest = mock(SolrQueryRequest.class);
    when(solrRequest.getParams()).thenReturn(params(WT, "xml"));
    when(request.getProperty(SOLR_QUERY_REQUEST)).thenReturn(solrRequest);
    when(request.getProperty(SOLR_PARAMS)).thenReturn(params(WT, "javabin"));

    filter.filter(request, response);

    assertEquals(APPLICATION_XML, responseHeaders.getFirst(CONTENT_TYPE));
  }

  @Test
  public void testAcceptHeaderUsedWithoutWt() throws Exception {
    when(request.getProperty(SOLR_PARAMS)).thenReturn(params());
    requestHeaders.putSingle(ACCEPT, APPLICATION_XML);

    filter.filter(request, response);

    assertEquals(APPLICATION_XML, responseHeaders.getFirst(CONTENT_TYPE));
  }

  @Test
  public void testDefaultsToJsonWithoutWt() throws Exception {
    when(request.getProperty(SOLR_PARAMS)).thenReturn(params());
    for (String accept : new String[] {null, "*/*"}) {
      requestHeaders.clear();
      if (accept != null) {
        requestHeaders.putSingle(ACCEPT, accept);
      }
      responseHeaders.putSingle(CONTENT_TYPE, APPLICATION_XML);

      filter.filter(request, response);

      assertEquals(APPLICATION_JSON, responseHeaders.getFirst(CONTENT_TYPE));
    }
  }

  @Test
  public void testMissingParamsStillDefaultsToJson() throws Exception {
    filter.filter(request, response);

    assertEquals(APPLICATION_JSON, responseHeaders.getFirst(CONTENT_TYPE));
  }

  @Test
  public void testErrorsPreserveMediaType() throws Exception {
    when(request.getProperty(SOLR_PARAMS)).thenReturn(params(WT, "javabin"));
    for (int status : new int[] {400, 500}) {
      when(response.getStatus()).thenReturn(status);

      filter.filter(request, response);

      assertEquals(APPLICATION_XML, responseHeaders.getFirst(CONTENT_TYPE));
    }
  }

  @Test
  public void testExemptResourcesPreserveMediaType() throws Exception {
    when(request.getProperty(SOLR_PARAMS)).thenReturn(params(WT, "javabin"));
    for (Class<?> resourceClass : new Class<?>[] {null, ZookeeperRead.class}) {
      doReturn(resourceClass).when(resourceInfo).getResourceClass();

      filter.filter(request, response);

      assertEquals(APPLICATION_XML, responseHeaders.getFirst(CONTENT_TYPE));
    }
  }

  @Test
  public void testBlankAndUnknownWtDoNotOverrideNegotiatedType() throws Exception {
    requestHeaders.putSingle(ACCEPT, APPLICATION_XML);
    for (String wt : new String[] {"", "unknown"}) {
      when(request.getProperty(SOLR_PARAMS)).thenReturn(params(WT, wt));

      filter.filter(request, response);

      assertEquals(APPLICATION_XML, responseHeaders.getFirst(CONTENT_TYPE));
    }
  }
}
