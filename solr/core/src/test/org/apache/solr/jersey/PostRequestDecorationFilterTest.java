/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import static org.apache.solr.jersey.RequestContextKeys.NOT_FOUND_FLAG;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_REQUEST;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link PostRequestDecorationFilter} */
public class PostRequestDecorationFilterTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testFilterDoesNotThrowWhenNoSolrQueryRequestAttached() throws Exception {
    // solrQueryRequest can be null when the request failed before Jersey attached one to the
    // request context (e.g. via CatchAllExceptionMapper); filter() must not NPE in that case.
    final var mockRequestContext = mock(ContainerRequestContext.class);
    final var mockResponseContext = mock(ContainerResponseContext.class);

    when(mockRequestContext.getPropertyNames()).thenReturn(Set.of());
    when(mockRequestContext.getProperty(SOLR_QUERY_REQUEST)).thenReturn(null);

    final var response = new SolrJerseyResponse();
    when(mockResponseContext.hasEntity()).thenReturn(true);
    when(mockResponseContext.getEntity()).thenReturn(response);

    new PostRequestDecorationFilter().filter(mockRequestContext, mockResponseContext);

    assertEquals(0, response.responseHeader.qTime);
  }

  @Test
  public void testFilterSetsQTimeWhenSolrQueryRequestPresent() throws Exception {
    final var mockRequestContext = mock(ContainerRequestContext.class);
    final var mockResponseContext = mock(ContainerResponseContext.class);
    final var mockSolrQueryRequest = mock(SolrQueryRequest.class);
    final var timer = new org.apache.solr.util.RTimerTree();

    when(mockRequestContext.getPropertyNames()).thenReturn(Set.of());
    when(mockRequestContext.getProperty(SOLR_QUERY_REQUEST)).thenReturn(mockSolrQueryRequest);
    when(mockSolrQueryRequest.getRequestTimer()).thenReturn(timer);

    final var response = new SolrJerseyResponse();
    when(mockResponseContext.hasEntity()).thenReturn(true);
    when(mockResponseContext.getEntity()).thenReturn(response);

    new PostRequestDecorationFilter().filter(mockRequestContext, mockResponseContext);

    assertTrue(response.responseHeader.qTime >= 0);
  }

  @Test
  public void testFilterSkipsEntirelyForNotFoundRequests() throws Exception {
    final var mockRequestContext = mock(ContainerRequestContext.class);
    final var mockResponseContext = mock(ContainerResponseContext.class);
    when(mockRequestContext.getPropertyNames()).thenReturn(Set.of(NOT_FOUND_FLAG));

    new PostRequestDecorationFilter().filter(mockRequestContext, mockResponseContext);

    verify(mockResponseContext, never()).hasEntity();
  }
}
